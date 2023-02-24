import logging
import multiprocessing as mp
import os
import queue
import signal
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Sized, Tuple, Union

try:
    import numpy as np
    NUMPY_INSTALLED = True
except ImportError:
    np = None
    NUMPY_INSTALLED = False

from mpire.async_result import (AsyncInitResult, AsyncResult, AsyncResultType, UnorderedAsyncExitResultIterator,
                                UnorderedAsyncResultIterator)
from mpire.comms import EXIT_FUNC, INIT_FUNC, MAIN_PROCESS, POISON_PILL, WorkerComms
from mpire.context import DEFAULT_START_METHOD, RUNNING_WINDOWS
from mpire.dashboard.connection_utils import get_dashboard_connection_details
from mpire.exception import populate_exception
from mpire.insights import WorkerInsights
from mpire.params import check_map_parameters, CPUList, WorkerMapParams, WorkerPoolParams
from mpire.progress_bar import ProgressBarHandler, tqdm
from mpire.signal import DisableKeyboardInterruptSignal
from mpire.tqdm_utils import TqdmManager
from mpire.utils import apply_numpy_chunking, chunk_tasks, set_cpu_affinity
from mpire.worker import MP_CONTEXTS, worker_factory

logger = logging.getLogger(__name__)


# TODO: docs

class WorkerPool:
    """
    A multiprocessing worker pool which acts like a ``multiprocessing.Pool``, but is faster and has more options.
    """

    def __init__(self, n_jobs: Optional[int] = None, daemon: bool = True, cpu_ids: CPUList = None,
                 shared_objects: Any = None, pass_worker_id: bool = False, use_worker_state: bool = False,
                 start_method: str = DEFAULT_START_METHOD, keep_alive: bool = False, use_dill: bool = False,
                 enable_insights: bool = False, order_tasks: bool = False) -> None:
        """
        :param n_jobs: Number of workers to spawn. If ``None``, will use ``mpire.cpu_count()``
        :param daemon: Whether to start the child processes as daemon
        :param cpu_ids: List of CPU IDs to use for pinning child processes to specific CPUs. The list must be as long as
            the number of jobs used (if ``n_jobs`` equals ``None`` it must be equal to ``mpire.cpu_count()``), or the
            list must have exactly one element. In the former case, element `i` specifies the CPU ID(s) to use for child
            process `i`. In the latter case the single element specifies the CPU ID(s) for all child  processes to use.
            A single element can be either a single integer specifying a single CPU ID, or a list of integers specifying
            that a single child process can make use of multiple CPU IDs. If ``None``, CPU pinning will be disabled
        :param shared_objects: Objects to be passed on as shared objects to the workers once. It will be passed on to
            the target, ``worker_init``, and ``worker_exit`` functions. ``shared_objects`` is only passed on when it's
            not ``None``. Shared objects will be copy-on-write when using ``fork`` as start method. When enabled,
            functions receive the shared objects as second argument, depending on other settings. The order is:
            ``worker_id``, ``shared_objects``, ``worker_state``, and finally the arguments passed on from
            ``iterable_of_args``
        :param pass_worker_id: Whether to pass on a worker ID to the target, ``worker_init``, and ``worker_exit``
            functions. When enabled, functions receive the worker ID as first argument, depending on other settings. The
            order is: ``worker_id``, ``shared_objects``, ``worker_state``, and finally the arguments passed on from
            ``iterable_of_args``
        :param use_worker_state: Whether to let a worker have a worker state. The worker state will be passed on to the
            target, ``worker_init``, and ``worker_exit`` functions. When enabled, functions receive the worker state as
            third argument, depending on other settings. The order is: ``worker_id``,  ``shared_objects``,
            ``worker_state``, and finally the arguments passed on from ``iterable_of_args``
        :param start_method: Which process start method to use. Options for multiprocessing: ``'fork'`` (default, if
            available), ``'forkserver'`` and ``'spawn'`` (default, if ``'fork'`` isn't available). For multithreading
            use ``'threading'``. See https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods
            for more information and
            https://docs.python.org/3/library/multiprocessing.html#the-spawn-and-forkserver-start-methods for some
            caveats when using the ``'spawn'`` or ``'forkserver'`` methods
        :param keep_alive: When ``True`` it will keep workers alive after completing a map call, allowing to reuse
            workers
        :param use_dill: Whether to use dill as serialization backend. Some exotic types (e.g., lambdas, nested
            functions) don't work well when using ``spawn`` as start method. In such cased, use ``dill`` (can be a bit
            slower sometimes)
        :param enable_insights: Whether to enable worker insights. Might come at a small performance penalty (often
            neglible)
        :param order_tasks: Whether to provide tasks to the workers in order, such that worker 0 will get chunk 0,
            worker 1 will get chunk 1, etc.
        """
        # Set parameters
        self.pool_params = WorkerPoolParams(n_jobs, cpu_ids, daemon, shared_objects, pass_worker_id, use_worker_state,
                                            start_method, keep_alive, use_dill, enable_insights, order_tasks)
        self.map_params = None  # type: Optional[WorkerMapParams]

        # Worker factory
        self.Worker = worker_factory(start_method, use_dill)

        # Multiprocessing context
        if start_method == 'threading':
            self.ctx = MP_CONTEXTS['threading']
        else:
            self.ctx = MP_CONTEXTS['mp_dill' if use_dill else 'mp'][start_method]

        # Cache for storing intermediate results. Add result objects for the worker_init and worker_exit functions
        self._cache: Dict[int, AsyncResultType] = {}
        AsyncInitResult(self._cache)
        UnorderedAsyncExitResultIterator(self._cache)

        # Container of the child processes and corresponding communication objects
        self._workers = []
        self._worker_comms = WorkerComms(self.ctx, self.pool_params.n_jobs, self.pool_params.order_tasks)
        self._map_running = False

        # Threads needed for gathering results, restarts, and checking for unexpective deaths and timeouts
        self._results_handler_thread = None
        self._restart_handler_thread = None
        self._timeout_handler_thread = None
        self._unexpected_death_handler_thread = None
        self._handler_threads_stop_event = threading.Event()

        # Progress bar handler, in case it is used
        self._progress_bar_handler = None

        # Worker insights, used for profiling
        self._worker_insights = WorkerInsights(self.ctx, self.pool_params.n_jobs)

    def pass_on_worker_id(self, pass_on: bool = True) -> None:
        """
        Set whether to pass on the worker ID to the function to be executed or not (default= ``False``).

        :param pass_on: Whether to pass on a worker ID to the target, ``worker_init``, and ``worker_exit``
            functions. When enabled, functions receive the worker ID depending on other settings. The order is:
            ``worker_id``, ``shared_objects``, ``worker_state``, and finally the arguments passed on using
            ``iterable_of_args``
        """
        if pass_on != self.pool_params.pass_worker_id:
            self._worker_comms.reset()
        self.pool_params.pass_worker_id = pass_on

    def set_shared_objects(self, shared_objects: Any = None) -> None:
        """
        Set shared objects to pass to the workers.

        :param shared_objects: Objects to be passed on as shared objects to the workers once. It will be passed on to
            the target, ``worker_init``, and ``worker_exit`` functions. ``shared_objects`` is only passed on when it's
            not ``None``. Shared objects will be copy-on-write when using ``fork`` as start method. When enabled,
            functions receive the shared objects depending on other settings. The order is: ``worker_id``,
            ``shared_objects``, ``worker_state``, and finally the arguments passed on using ``iterable_of_args```
        """
        if shared_objects != self.pool_params.shared_objects:
            self._worker_comms.reset()
        self.pool_params.shared_objects = shared_objects

    def set_use_worker_state(self, use_worker_state: bool = True) -> None:
        """
        Set whether or not each worker should have its own state variable. Each worker has its own state, so it's not
        shared between the workers.

        :param use_worker_state: Whether to let a worker have a worker state. The worker state will be passed on to the
            target, ``worker_init``, and ``worker_exit`` functions. When enabled, functions receive the worker state
            depending on other settings. The order is: ``worker_id``,  ``shared_objects``, ``worker_state``, and finally
            the arguments passed on using ``iterable_of_args``
        """
        if use_worker_state != self.pool_params.use_worker_state:
            self._worker_comms.reset()
        self.pool_params.use_worker_state = use_worker_state

    def set_keep_alive(self, keep_alive: bool = True) -> None:
        """
        Set whether workers should be kept alive in between consecutive map calls.

        :param keep_alive: When True it will keep workers alive after completing a map call, allowing to reuse workers
        """
        self.pool_params.keep_alive = keep_alive

    def set_order_tasks(self, order_tasks: bool = True) -> None:
        """
        Set whether to provide tasks to the workers in order, such that worker 0 will get chunk 0, worker 1 will get
        chunk 1, etc.

        :param order_tasks: Whether to provide tasks to the workers in order, such that worker 0 will get chunk 0,
            worker 1 will get chunk 1, etc.
        """
        self.pool_params.order_tasks = order_tasks

    def _start_workers(self) -> None:
        """
        Spawns the workers and starts them so they're ready to start reading from the tasks queue.
        """
        # Init communication primitives
        self._worker_comms.init_comms()
        self._worker_insights.reset_insights(self.pool_params.enable_insights)

        # Start new workers
        self._workers = [None] * self.pool_params.n_jobs
        for worker_id in range(self.pool_params.n_jobs):
            self._start_worker(worker_id)

        # Start results listener, restart handler, timeout handler, unexpected death handler
        self._handler_threads_stop_event.clear()
        self._results_handler_thread = threading.Thread(target=self._results_handler, daemon=True)
        self._restart_handler_thread = threading.Thread(target=self._restart_handler, daemon=True)
        self._timeout_handler_thread = threading.Thread(target=self._timeout_handler, daemon=True)
        self._unexpected_death_handler_thread = threading.Thread(target=self._unexpected_death_handler, daemon=True)
        self._results_handler_thread.start()
        self._restart_handler_thread.start()
        self._timeout_handler_thread.start()
        self._unexpected_death_handler_thread.start()

    def _start_worker(self, worker_id: int) -> None:
        """
        Creates and starts a single worker

        :param worker_id: ID of the worker
        :return: Worker instance
        """
        # Disable the interrupt signal. We let the process die gracefully if it needs to
        with DisableKeyboardInterruptSignal():
            # Create worker
            self._workers[worker_id] = self.Worker(
                worker_id, self.pool_params, self.map_params, self._worker_comms, self._worker_insights,
                TqdmManager.get_connection_details(), get_dashboard_connection_details(), datetime.now()
            )
            self._workers[worker_id].daemon = self.pool_params.daemon
            self._workers[worker_id].name = f"Worker-{worker_id}"
            self._workers[worker_id].start()

        # Pin CPU if desired
        if self.pool_params.cpu_ids:
            set_cpu_affinity(self._workers[worker_id].pid, self.pool_params.cpu_ids[worker_id])

    def _results_handler(self) -> None:
        """
        Listen for results from the workers and add it to the cache. Note that when ``set`` is called on a result
        object, the result is automatically removed from the cache.
        """
        while True:
            results_batch = self._worker_comms.get_results(block=True)
            for job_id, success, result in results_batch:

                # Poison pill, stop the listener
                if isinstance(result, str) and result == POISON_PILL:
                    return

                if success:
                    self._cache[job_id]._set(True, result)
                else:
                    err, traceback_err = populate_exception(*result)
                    err.__cause__ = traceback_err
                    self._cache[job_id]._set(False, err)

    def _restart_handler(self) -> None:
        """
        Listen for worker restarts and restart them if needed.
        """
        while not self._worker_comms.exception_thrown() and not self._handler_threads_stop_event.is_set():

            for worker_id in self._worker_comms.get_worker_restarts():
                # The get_worker_restarts call is blocking, but can unblock when we need to stop (either due to an
                # exception or because all tasks have been processed). However, a worker could've asked for a restart in
                # the meantime, so we need to check if we need to stop again
                if self._worker_comms.exception_thrown() or self._handler_threads_stop_event.is_set():
                    return

                # Join worker. This can take a while as the worker could still be holding on to data it needs to send
                # over the results queue
                self._workers[worker_id].join()

                # Start new worker
                self._worker_comms.reset_worker_restart(worker_id)
                self._start_worker(worker_id)

    def _unexpected_death_handler(self) -> None:
        """
        Checks that workers that are supposed to be alive, are actually alive. If not, then a worker died unexpectedly.

        Note that a worker can be alive, but their alive status is still False. This doesn't really matter, because we
        know the worker is alive according to the OS. The only way we know that something bad happened is when a worker
        is supposed to be alive but according to the OS it's not.
        """
        while not self._worker_comms.exception_thrown() and not self._handler_threads_stop_event.is_set():

            # This thread can be started before the workers are created, so we need to check that they exist. If not
            # we just wait a bit and try again.
            for worker_id in range(len(self._workers)):
                try:
                    worker_died = (self._worker_comms.is_worker_alive(worker_id) and
                                   not self._workers[worker_id].is_alive())
                except ValueError:
                    worker_died = False

                if worker_died:
                    # Obtain task it was working on and set it to failed
                    job_id = self._worker_comms.get_worker_working_on_job(worker_id)
                    self._worker_comms.signal_exception_thrown(job_id)
                    err_msg = f"Worker-{worker_id} died unexpectedly"
                    self._cache[job_id]._set(False, RuntimeError(err_msg))
                    return

            # Check this every once in a while
            time.sleep(0.1)

    def _timeout_handler(self) -> None:
        """
        Check for worker_init/task/worker_exit timeouts
        """
        while not self._worker_comms.exception_thrown() and not self._handler_threads_stop_event.is_set():

            for timeout_var, has_timed_out_func, timeout_func_name in [
                (self.map_params.worker_init_timeout, self._worker_comms.has_worker_init_timed_out, 'worker_init'),
                (self.map_params.task_timeout, self._worker_comms.has_worker_task_timed_out, 'task'),
                (self.map_params.worker_exit_timeout, self._worker_comms.has_worker_exit_timed_out, 'worker_exit'),
            ]:
                if timeout_var is not None:
                    for worker_id in range(self.pool_params.n_jobs):
                        if has_timed_out_func(worker_id, timeout_var):

                            # Obtain task it was working on and set it to failed
                            job_id = self._worker_comms.get_worker_working_on_job(worker_id)
                            self._worker_comms.signal_exception_thrown(job_id)
                            err_msg = f"Worker-{worker_id} {timeout_func_name} timed out"
                            self._cache[job_id]._set(False, TimeoutError(err_msg))
                            return

            # Check this every once in a while
            time.sleep(0.1)

    def get_exit_results(self) -> List:
        """
        Obtain a list of exit results when an exit function is defined.

        :return: Exit results list
        """
        return self._cache[EXIT_FUNC].get_results()

    def __enter__(self) -> 'WorkerPool':
        """
        Enable the use of the ``with`` statement.
        """
        return self

    def __exit__(self, *_: Any) -> None:
        """
        Enable the use of the ``with`` statement. Gracefully terminates workers, if there are any
        """
        if self._workers:
            self.terminate()

    def map(self, func: Callable, iterable_of_args: Union[Sized, Iterable], iterable_len: Optional[int] = None,
            max_tasks_active: Optional[int] = None, chunk_size: Optional[int] = None, n_splits: Optional[int] = None,
            worker_lifespan: Optional[int] = None, progress_bar: bool = False,
            progress_bar_position: Optional[int] = None, concatenate_numpy_output: bool = True,
            worker_init: Optional[Callable] = None, worker_exit: Optional[Callable] = None,
            task_timeout: Optional[float] = None, worker_init_timeout: Optional[float] = None,
            worker_exit_timeout: Optional[float] = None, progress_bar_options: Optional[Dict[str, Any]] = None) -> Any:
        """
        Same as ``multiprocessing.map()``. Also allows a user to set the maximum number of tasks available in the queue.
        Note that this function can be slower than the unordered version.

        :param func: Function to call each time new task arguments become available. When passing on the worker ID the
            function should receive the worker ID as its first argument. If shared objects are provided the function
            should receive those as the next argument. If the worker state has been enabled it should receive a state
            variable as the next argument
        :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker, which
            passes it to the function ``func``
        :param iterable_len: Number of elements in the ``iterable_of_args``. When chunk_size is set to ``None`` it needs
            to know the number of tasks. This can either be provided by implementing the ``__len__`` function on the
            iterable object, or by specifying the number of tasks
        :param max_tasks_active: Maximum number of active tasks in the queue. If ``None`` it will be converted to
            ``n_jobs * chunk_size * 2``
        :param chunk_size: Number of simultaneous tasks to give to a worker. When ``None`` it will use ``n_splits``.
        :param n_splits: Number of splits to use when ``chunk_size`` is ``None``. When both ``chunk_size`` and
            ``n_splits`` are ``None``, it will use ``n_splits = n_jobs * 64``.
        :param worker_lifespan: Number of tasks a worker can handle before it is restarted. If ``None``, workers will
            stay alive the entire time. Use this when workers use up too much memory over the course of time
        :param progress_bar: When ``True`` it will display a progress bar
        :param progress_bar_position: Denotes the position (line nr) of the progress bar. This is useful when using
            multiple progress bars at the same time.

            DEPRECATED in v2.6.0, to be removed in v2.10.0! Set the progress bar position using ``progress_bar_options``
            instead.
        :param concatenate_numpy_output: When ``True`` it will concatenate numpy output to a single numpy array
        :param worker_init: Function to call each time a new worker starts. When passing on the worker ID the function
            should receive the worker ID as its first argument. If shared objects are provided the function should
            receive those as the next argument. If the worker state has been enabled it should receive a state variable
            as the next argument
        :param worker_exit: Function to call each time a worker exits. Return values will be fetched and made available
            through :obj:`mpire.WorkerPool.get_exit_results`. When passing on the worker ID the function should receive
            the worker ID as its first argument. If shared objects are provided the function should receive those as the
            next argument. If the worker state has been enabled it should receive a state variable as the next argument
        :param task_timeout: Timeout in seconds for a single task. When the timeout is exceeded, MPIRE will raise a
            ``TimeoutError``. Use ``None`` to disable (default). Note: the timeout doesn't apply to ``worker_init`` and
            ``worker_exit`` functions, use `worker_init_timeout` and `worker_exit_timeout` for that, respectively
        :param worker_init_timeout: Timeout in seconds for the ``worker_init`` function. When the timeout is exceeded,
            MPIRE will raise a ``TimeoutError``. Use ``None`` to disable (default).
        :param worker_exit_timeout: Timeout in seconds for the ``worker_exit`` function. When the timeout is exceeded,
            MPIRE will raise a ``TimeoutError``. Use ``None`` to disable (default).
        :param progress_bar_options: Dictionary containing keyword arguments to pass to the ``tqdm`` progress bar. See
            ``tqdm.tqdm()`` for details. The arguments ``total`` and ``leave`` will be overwritten by MPIRE.
        :return: List with ordered results
        """
        # Notify workers to keep order in mind
        self._worker_comms.signal_keep_order()

        # If we're dealing with numpy arrays, we have to chunk them here already
        if NUMPY_INSTALLED and isinstance(iterable_of_args, np.ndarray):
            iterable_of_args, iterable_len, chunk_size, n_splits = apply_numpy_chunking(iterable_of_args, iterable_len,
                                                                                        chunk_size, n_splits,
                                                                                        self.pool_params.n_jobs)

        # Process all args
        if iterable_len is None and hasattr(iterable_of_args, '__len__'):
            iterable_len = len(iterable_of_args)
        results = self.map_unordered(func, ((args_idx, args) for args_idx, args in enumerate(iterable_of_args)),
                                     iterable_len, max_tasks_active, chunk_size, n_splits, worker_lifespan,
                                     progress_bar, progress_bar_position, worker_init, worker_exit, task_timeout,
                                     worker_init_timeout, worker_exit_timeout, progress_bar_options)

        # Notify workers to forget about order
        self._worker_comms.clear_keep_order()

        # Rearrange and return
        sorted_results = [result[1] for result in sorted(results, key=lambda result: result[0])]

        # Convert back to numpy if necessary
        return (np.concatenate(sorted_results) if NUMPY_INSTALLED and sorted_results and concatenate_numpy_output and
                isinstance(sorted_results[0], np.ndarray) else sorted_results)

    def map_unordered(self, func: Callable, iterable_of_args: Union[Sized, Iterable],
                      iterable_len: Optional[int] = None, max_tasks_active: Optional[int] = None,
                      chunk_size: Optional[int] = None, n_splits: Optional[int] = None,
                      worker_lifespan: Optional[int] = None, progress_bar: bool = False,
                      progress_bar_position: Optional[int] = None, worker_init: Optional[Callable] = None,
                      worker_exit: Optional[Callable] = None, task_timeout: Optional[float] = None,
                      worker_init_timeout: Optional[float] = None, worker_exit_timeout: Optional[float] = None,
                      progress_bar_options: Optional[Dict[str, Any]] = None) -> Any:
        """
        Same as ``multiprocessing.map()``, but unordered. Also allows a user to set the maximum number of tasks
        available in the queue.

        :param func: Function to call each time new task arguments become available. When passing on the worker ID the
            function should receive the worker ID as its first argument. If shared objects are provided the function
            should receive those as the next argument. If the worker state has been enabled it should receive a state
            variable as the next argument
        :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker, which
            passes it to the function ``func``
        :param iterable_len: Number of elements in the ``iterable_of_args``. When chunk_size is set to ``None`` it needs
            to know the number of tasks. This can either be provided by implementing the ``__len__`` function on the
            iterable object, or by specifying the number of tasks
        :param max_tasks_active: Maximum number of active tasks in the queue. If ``None`` it will be converted to
            ``n_jobs * chunk_size * 2``
        :param chunk_size: Number of simultaneous tasks to give to a worker. When ``None`` it will use ``n_splits``.
        :param n_splits: Number of splits to use when ``chunk_size`` is ``None``. When both ``chunk_size`` and
            ``n_splits`` are ``None``, it will use ``n_splits = n_jobs * 64``.
        :param worker_lifespan: Number of tasks a worker can handle before it is restarted. If ``None``, workers will
            stay alive the entire time. Use this when workers use up too much memory over the course of time
        :param progress_bar: When ``True`` it will display a progress bar
        :param progress_bar_position: Denotes the position (line nr) of the progress bar. This is useful when using
            multiple progress bars at the same time.

            DEPRECATED in v2.6.0, to be removed in v2.10.0! Set the progress bar position using ``progress_bar_options``
            instead.
        :param worker_init: Function to call each time a new worker starts. When passing on the worker ID the function
            should receive the worker ID as its first argument. If shared objects are provided the function should
            receive those as the next argument. If the worker state has been enabled it should receive a state variable
            as the next argument
        :param worker_exit: Function to call each time a worker exits. Return values will be fetched and made available
            through :obj:`mpire.WorkerPool.get_exit_results`. When passing on the worker ID the function should receive
            the worker ID as its first argument. If shared objects are provided the function should receive those as the
            next argument. If the worker state has been enabled it should receive a state variable as the next argument
        :param task_timeout: Timeout in seconds for a single task. When the timeout is exceeded, MPIRE will raise a
            ``TimeoutError``. Use ``None`` to disable (default). Note: the timeout doesn't apply to ``worker_init`` and
            ``worker_exit`` functions, use `worker_init_timeout` and `worker_exit_timeout` for that, respectively
        :param worker_init_timeout: Timeout in seconds for the ``worker_init`` function. When the timeout is exceeded,
            MPIRE will raise a ``TimeoutError``. Use ``None`` to disable (default).
        :param worker_exit_timeout: Timeout in seconds for the ``worker_exit`` function. When the timeout is exceeded,
            MPIRE will raise a ``TimeoutError``. Use ``None`` to disable (default).
        :param progress_bar_options: Dictionary containing keyword arguments to pass to the ``tqdm`` progress bar. See
            ``tqdm.tqdm()`` for details. The arguments ``total`` and ``leave`` will be overwritten by MPIRE.
        :return: List with unordered results
        """
        # Simply call imap and cast it to a list. This make sure all elements are there before returning
        return list(self.imap_unordered(func, iterable_of_args, iterable_len, max_tasks_active, chunk_size,
                                        n_splits, worker_lifespan, progress_bar, progress_bar_position, worker_init,
                                        worker_exit, task_timeout, worker_init_timeout, worker_exit_timeout,
                                        progress_bar_options))

    def imap(self, func: Callable, iterable_of_args: Union[Sized, Iterable], iterable_len: Optional[int] = None,
             max_tasks_active: Optional[int] = None, chunk_size: Optional[int] = None, n_splits: Optional[int] = None,
             worker_lifespan: Optional[int] = None, progress_bar: bool = False,
             progress_bar_position: Optional[int] = None, worker_init: Optional[Callable] = None,
             worker_exit: Optional[Callable] = None, task_timeout: Optional[float] = None,
             worker_init_timeout: Optional[float] = None, worker_exit_timeout: Optional[float] = None,
             progress_bar_options: Optional[Dict[str, Any]] = None) -> Generator[Any, None, None]:
        """
        Same as ``multiprocessing.imap_unordered()``, but ordered. Also allows a user to set the maximum number of
        tasks available in the queue.

        :param func: Function to call each time new task arguments become available. When passing on the worker ID the
            function should receive the worker ID as its first argument. If shared objects are provided the function
            should receive those as the next argument. If the worker state has been enabled it should receive a state
            variable as the next argument
        :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker, which
            passes it to the function ``func``
        :param iterable_len: Number of elements in the ``iterable_of_args``. When chunk_size is set to ``None`` it needs
            to know the number of tasks. This can either be provided by implementing the ``__len__`` function on the
            iterable object, or by specifying the number of tasks
        :param max_tasks_active: Maximum number of active tasks in the queue. If ``None`` it will be converted to
            ``n_jobs * chunk_size * 2``
        :param chunk_size: Number of simultaneous tasks to give to a worker. When ``None`` it will use ``n_splits``.
        :param n_splits: Number of splits to use when ``chunk_size`` is ``None``. When both ``chunk_size`` and
            ``n_splits`` are ``None``, it will use ``n_splits = n_jobs * 64``.
        :param worker_lifespan: Number of tasks a worker can handle before it is restarted. If ``None``, workers will
            stay alive the entire time. Use this when workers use up too much memory over the course of time
        :param progress_bar: When ``True`` it will display a progress bar
        :param progress_bar_position: Denotes the position (line nr) of the progress bar. This is useful when using
            multiple progress bars at the same time.

            DEPRECATED in v2.6.0, to be removed in v2.10.0! Set the progress bar position using ``progress_bar_options``
            instead.
        :param worker_init: Function to call each time a new worker starts. When passing on the worker ID the function
            should receive the worker ID as its first argument. If shared objects are provided the function should
            receive those as the next argument. If the worker state has been enabled it should receive a state variable
            as the next argument
        :param worker_exit: Function to call each time a worker exits. Return values will be fetched and made available
            through :obj:`mpire.WorkerPool.get_exit_results`. When passing on the worker ID the function should receive
            the worker ID as its first argument. If shared objects are provided the function should receive those as the
            next argument. If the worker state has been enabled it should receive a state variable as the next argument
        :param task_timeout: Timeout in seconds for a single task. When the timeout is exceeded, MPIRE will raise a
            ``TimeoutError``. Use ``None`` to disable (default). Note: the timeout doesn't apply to ``worker_init`` and
            ``worker_exit`` functions, use `worker_init_timeout` and `worker_exit_timeout` for that, respectively
        :param worker_init_timeout: Timeout in seconds for the ``worker_init`` function. When the timeout is exceeded,
            MPIRE will raise a ``TimeoutError``. Use ``None`` to disable (default).
        :param worker_exit_timeout: Timeout in seconds for the ``worker_exit`` function. When the timeout is exceeded,
            MPIRE will raise a ``TimeoutError``. Use ``None`` to disable (default).
        :param progress_bar_options: Dictionary containing keyword arguments to pass to the ``tqdm`` progress bar. See
            ``tqdm.tqdm()`` for details. The arguments ``total`` and ``leave`` will be overwritten by MPIRE.
        :return: Generator yielding ordered results
        """
        # Notify workers to keep order in mind
        self._worker_comms.signal_keep_order()

        # If we're dealing with numpy arrays, we have to chunk them here already
        if NUMPY_INSTALLED and isinstance(iterable_of_args, np.ndarray):
            iterable_of_args, iterable_len, chunk_size, n_splits = apply_numpy_chunking(iterable_of_args, iterable_len,
                                                                                        chunk_size, n_splits,
                                                                                        self.pool_params.n_jobs)

        # Yield results in order
        next_result_idx = 0
        tmp_results = {}
        if iterable_len is None and hasattr(iterable_of_args, '__len__'):
            iterable_len = len(iterable_of_args)
        for result_idx, result in self.imap_unordered(func, ((args_idx, args) for args_idx, args
                                                             in enumerate(iterable_of_args)), iterable_len,
                                                      max_tasks_active, chunk_size, n_splits, worker_lifespan,
                                                      progress_bar, progress_bar_position, worker_init, worker_exit,
                                                      task_timeout, worker_init_timeout, worker_exit_timeout,
                                                      progress_bar_options):

            # Check if the next one(s) to return is/are temporarily stored. We use a while-true block with dict.pop() to
            # keep the temporary store as small as possible
            while True:
                if next_result_idx in tmp_results:
                    yield tmp_results.pop(next_result_idx)
                    next_result_idx += 1
                else:
                    break

            # Check if the current result is the next one to return. If so, return it
            if result_idx == next_result_idx:
                yield result
                next_result_idx += 1
            # Otherwise, temporarily store the current result
            else:
                tmp_results[result_idx] = result

        # Yield all remaining results
        for result_idx in sorted(tmp_results.keys()):
            yield tmp_results.pop(result_idx)

        # Notify workers to forget about order
        self._worker_comms.clear_keep_order()

    def imap_unordered(self, func: Callable, iterable_of_args: Union[Sized, Iterable],
                       iterable_len: Optional[int] = None, max_tasks_active: Optional[int] = None,
                       chunk_size: Optional[int] = None, n_splits: Optional[int] = None,
                       worker_lifespan: Optional[int] = None, progress_bar: bool = False,
                       progress_bar_position: Optional[int] = None, worker_init: Optional[Callable] = None,
                       worker_exit: Optional[Callable] = None, task_timeout: Optional[float] = None,
                       worker_init_timeout: Optional[float] = None, worker_exit_timeout: Optional[float] = None,
                       progress_bar_options: Optional[Dict[str, Any]] = None) -> Generator[Any, None, None]:
        """
        Same as ``multiprocessing.imap_unordered()``. Also allows a user to set the maximum number of tasks available in
        the queue.

        :param func: Function to call each time new task arguments become available. When passing on the worker ID the
            function should receive the worker ID as its first argument. If shared objects are provided the function
            should receive those as the next argument. If the worker state has been enabled it should receive a state
            variable as the next argument
        :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker, which
            passes it to the function ``func``
        :param iterable_len: Number of elements in the ``iterable_of_args``. When chunk_size is set to ``None`` it needs
            to know the number of tasks. This can either be provided by implementing the ``__len__`` function on the
            iterable object, or by specifying the number of tasks
        :param max_tasks_active: Maximum number of active tasks in the queue. If ``None`` it will be converted to
            ``n_jobs * chunk_size * 2``
        :param chunk_size: Number of simultaneous tasks to give to a worker. When ``None`` it will use ``n_splits``.
        :param n_splits: Number of splits to use when ``chunk_size`` is ``None``. When both ``chunk_size`` and
            ``n_splits`` are ``None``, it will use ``n_splits = n_jobs * 64``.
        :param worker_lifespan: Number of tasks a worker can handle before it is restarted. If ``None``, workers will
            stay alive the entire time. Use this when workers use up too much memory over the course of time
        :param progress_bar: When ``True`` it will display a progress bar
        :param progress_bar_position: Denotes the position (line nr) of the progress bar. This is useful when using
            multiple progress bars at the same time.

            DEPRECATED in v2.6.0, to be removed in v2.10.0! Set the progress bar position using ``progress_bar_options``
            instead.
        :param worker_init: Function to call each time a new worker starts. When passing on the worker ID the function
            should receive the worker ID as its first argument. If shared objects are provided the function should
            receive those as the next argument. If the worker state has been enabled it should receive a state variable
            as the next argument
        :param worker_exit: Function to call each time a worker exits. Return values will be fetched and made available
            through :obj:`mpire.WorkerPool.get_exit_results`. When passing on the worker ID the function should receive
            the worker ID as its first argument. If shared objects are provided the function should receive those as the
            next argument. If the worker state has been enabled it should receive a state variable as the next argument
        :param task_timeout: Timeout in seconds for a single task. When the timeout is exceeded, MPIRE will raise a
            ``TimeoutError``. Use ``None`` to disable (default). Note: the timeout doesn't apply to ``worker_init`` and
            ``worker_exit`` functions, use `worker_init_timeout` and `worker_exit_timeout` for that, respectively
        :param worker_init_timeout: Timeout in seconds for the ``worker_init`` function. When the timeout is exceeded,
            MPIRE will raise a ``TimeoutError``. Use ``None`` to disable (default).
        :param worker_exit_timeout: Timeout in seconds for the ``worker_exit`` function. When the timeout is exceeded,
            MPIRE will raise a ``TimeoutError``. Use ``None`` to disable (default).
        :param progress_bar_options: Dictionary containing keyword arguments to pass to the ``tqdm`` progress bar. See
            ``tqdm.tqdm()`` for details. The arguments ``total`` and ``leave`` will be overwritten by MPIRE.
        :return: Generator yielding unordered results
        """
        # If we're dealing with numpy arrays, we have to chunk them here already
        iterator_of_chunked_args = []
        numpy_chunking = False
        if NUMPY_INSTALLED and isinstance(iterable_of_args, np.ndarray):
            iterator_of_chunked_args, iterable_len, chunk_size, n_splits = apply_numpy_chunking(
                iterable_of_args, iterable_len, chunk_size, n_splits, self.pool_params.n_jobs
            )
            numpy_chunking = True

        # Check parameters and thereby obtain the number of tasks. The chunk_size and progress bar parameters could be
        # modified as well
        n_tasks, max_tasks_active, chunk_size, progress_bar, progress_bar_options = check_map_parameters(
            self.pool_params, iterable_of_args, iterable_len, max_tasks_active, chunk_size, n_splits, worker_lifespan,
            progress_bar, progress_bar_position, progress_bar_options, task_timeout, worker_init_timeout,
            worker_exit_timeout
        )
        new_map_params = WorkerMapParams(func, worker_init, worker_exit, worker_lifespan, progress_bar, task_timeout,
                                         worker_init_timeout, worker_exit_timeout)

        # Chunk the function arguments. Make single arguments when we're not dealing with numpy arrays
        if not numpy_chunking:
            iterator_of_chunked_args = chunk_tasks(iterable_of_args, n_tasks, chunk_size, n_splits)

        # Grab original lock in case we have a progress bar and we need to restore it
        original_tqdm_lock = tqdm.get_lock()
        tqdm_manager_owner = False

        imap_iterator = None
        try:
            self._map_running = True
            self._cache[INIT_FUNC].reset()
            self._cache[EXIT_FUNC].reset()

            # Start tqdm manager if a progress bar is desired. Will only start one when not already started. This has to
            # be done before starting the workers in case nested pools are used
            if progress_bar:
                tqdm_manager_owner = TqdmManager.start_manager()

            # Start workers if there aren't any. If they already exist check if we need to pass on new parameters
            if self._workers and not self._worker_comms.is_initialized():
                logger.warning("WorkerPool parameters changed while keep_alive=True. Restarting workers.")
                self.stop_and_join(keep_alive=False)
            if self._workers and (self.map_params != new_map_params):
                self.map_params = new_map_params
                self._worker_comms.add_new_map_params(new_map_params)
            if not self._workers:
                self.map_params = new_map_params
                self._start_workers()

            # Create async result objects. The imap_iterator container will be used to store the results from the
            # workers. We can yield from that
            imap_iterator = UnorderedAsyncResultIterator(self._cache, n_tasks)
            job_id = imap_iterator.job_id

            # Create progress bar handler, which receives progress updates from the workers and updates the progress bar
            # accordingly
            with ProgressBarHandler(self.pool_params, self.map_params, progress_bar, progress_bar_options,
                                    self._worker_comms, self._worker_insights) as self._progress_bar_handler:
                try:
                    # Process all args in the iterable
                    n_active = 0
                    n_tasks = 0
                    while True:

                        # Obtain next chunk of tasks
                        try:
                            chunk_of_tasks = next(iterator_of_chunked_args)
                            n_tasks += len(chunk_of_tasks)
                        except StopIteration:
                            break

                        # To keep the number of active tasks below max_tasks_active, we have to wait for results
                        while (not self._worker_comms.exception_thrown() and
                               n_active + len(chunk_of_tasks) > max_tasks_active):
                            try:
                                yield imap_iterator.next(block=True, timeout=0.01)
                                n_active -= 1
                            except queue.Empty:
                                pass

                        # If an exception has been thrown, stop now
                        if self._worker_comms.exception_thrown():
                            break

                        self._worker_comms.add_task(job_id, chunk_of_tasks)
                        n_active += len(chunk_of_tasks)

                    # Obtain the results not yet obtained
                    if not self._worker_comms.exception_thrown():
                        imap_iterator.set_length(n_tasks)
                        self._progress_bar_handler.set_new_total(n_tasks)
                    while not self._worker_comms.exception_thrown():
                        try:
                            yield imap_iterator.next(block=True, timeout=0.1)
                        except queue.Empty:
                            pass
                        except StopIteration:
                            break

                    # Terminate if exception has been thrown at this point
                    if self._worker_comms.exception_thrown():
                        self._handle_exception()

                    # All results are in: it's clean up time
                    self.stop_and_join(keep_alive=self.pool_params.keep_alive)

                    # Wait for the progress bar to finish, before we clean it up
                    if progress_bar:
                        self._worker_comms.wait_until_progress_bar_is_complete()

                except KeyboardInterrupt:
                    self._handle_exception()

        finally:
            if tqdm_manager_owner:
                tqdm.set_lock(original_tqdm_lock)
                TqdmManager.stop_manager()

            self._progress_bar_handler = None
            self._map_running = False

            if imap_iterator is not None:
                imap_iterator.remove_from_cache()

        # Log insights
        if self.pool_params.enable_insights:
            logger.debug(self._worker_insights.get_insights_string())

    def apply(self, func: Callable, args: Tuple = (), kwargs: Dict = None, callback: Optional[Callable] = None,
              error_callback: Optional[Callable] = None, worker_init: Optional[Callable] = None,
              worker_exit: Optional[Callable] = None, task_timeout: Optional[float] = None,
              worker_init_timeout: Optional[float] = None, worker_exit_timeout: Optional[float] = None) -> Any:
        """
        Apply a function to a single task. This is a blocking call.

        :param func: Function to apply to the task. When passing on the worker ID the function should receive the
            worker ID as its first argument. If shared objects are provided the function should receive those as the
            next argument. If the worker state has been enabled it should receive a state variable as the next argument
        :param args: Arguments to pass to a worker, which passes it to the function ``func`` as ``func(*args)``
        :param kwargs: Keyword arguments to pass to a worker, which passes it to the function ``func`` as
            ``func(**kwargs)``
        :param callback: Callback function to call when the task is finished. The callback function receives the output
            of the function ``func`` as its argument
        :param error_callback: Callback function to call when the task has failed. The callback function receives the
            exception as its argument
        :param worker_init: Function to call each time a new worker starts. When passing on the worker ID the function
            should receive the worker ID as its first argument. If shared objects are provided the function should
            receive those as the next argument. If the worker state has been enabled it should receive a state variable
            as the next argument
        :param worker_exit: Function to call each time a worker exits. Return values will be fetched and made available
            through :obj:`mpire.WorkerPool.get_exit_results`. When passing on the worker ID the function should receive
            the worker ID as its first argument. If shared objects are provided the function should receive those as the
            next argument. If the worker state has been enabled it should receive a state variable as the next argument
        :param task_timeout: Timeout in seconds for a single task. When the timeout is exceeded, MPIRE will raise a
            ``TimeoutError``. Use ``None`` to disable (default). Note: the timeout doesn't apply to ``worker_init`` and
            ``worker_exit`` functions, use `worker_init_timeout` and `worker_exit_timeout` for that, respectively
        :param worker_init_timeout: Timeout in seconds for the ``worker_init`` function. When the timeout is exceeded,
            MPIRE will raise a ``TimeoutError``. Use ``None`` to disable (default).
        :param worker_exit_timeout: Timeout in seconds for the ``worker_exit`` function. When the timeout is exceeded,
            MPIRE will raise a ``TimeoutError``. Use ``None`` to disable (default).
        :return: Result of the function ``func`` applied to the task
        """
        return self.apply_async(func, args, kwargs, callback, error_callback, worker_init, worker_exit,
                                task_timeout, worker_init_timeout, worker_exit_timeout).get()

    def apply_async(self, func: Callable, args: Tuple = (), kwargs: Dict = None, callback: Optional[Callable] = None,
                    error_callback: Optional[Callable] = None, worker_init: Optional[Callable] = None,
                    worker_exit: Optional[Callable] = None, task_timeout: Optional[float] = None,
                    worker_init_timeout: Optional[float] = None,
                    worker_exit_timeout: Optional[float] = None) -> AsyncResult:
        """
        Apply a function to a single task. This is a non-blocking call.

        :param func: Function to apply to the task. When passing on the worker ID the function should receive the
            worker ID as its first argument. If shared objects are provided the function should receive those as the
            next argument. If the worker state has been enabled it should receive a state variable as the next argument
        :param args: Arguments to pass to a worker, which passes it to the function ``func`` as ``func(*args)``
        :param kwargs: Keyword arguments to pass to a worker, which passes it to the function ``func`` as
            ``func(**kwargs)``
        :param callback: Callback function to call when the task is finished. The callback function receives the output
            of the function ``func`` as its argument
        :param error_callback: Callback function to call when the task has failed. The callback function receives the
            exception as its argument
        :param worker_init: Function to call each time a new worker starts. When passing on the worker ID the function
            should receive the worker ID as its first argument. If shared objects are provided the function should
            receive those as the next argument. If the worker state has been enabled it should receive a state variable
            as the next argument
        :param worker_exit: Function to call each time a worker exits. Return values will be fetched and made available
            through :obj:`mpire.WorkerPool.get_exit_results`. When passing on the worker ID the function should receive
            the worker ID as its first argument. If shared objects are provided the function should receive those as the
            next argument. If the worker state has been enabled it should receive a state variable as the next argument
        :param task_timeout: Timeout in seconds for a single task. When the timeout is exceeded, MPIRE will raise a
            ``TimeoutError``. Use ``None`` to disable (default). Note: the timeout doesn't apply to ``worker_init`` and
            ``worker_exit`` functions, use `worker_init_timeout` and `worker_exit_timeout` for that, respectively
        :param worker_init_timeout: Timeout in seconds for the ``worker_init`` function. When the timeout is exceeded,
            MPIRE will raise a ``TimeoutError``. Use ``None`` to disable (default).
        :param worker_exit_timeout: Timeout in seconds for the ``worker_exit`` function. When the timeout is exceeded,
            MPIRE will raise a ``TimeoutError``. Use ``None`` to disable (default).
        :return: Result of the function ``func`` applied to the task
        """
        # We can't use the pool if a map is running
        if self._map_running:
            raise RuntimeError("Cannot apply a function while a map function is running")

        # Check if the pool has been started
        if not self._workers:
            self.map_params = WorkerMapParams(func, worker_init, worker_exit, None, False, task_timeout,
                                              worker_init_timeout, worker_exit_timeout)
            self._start_workers()

        # Add task to the queue
        result = AsyncResult(self._cache, callback, error_callback)
        self._worker_comms.add_apply_task(result.job_id, func, args, kwargs)
        return result

    def _handle_exception(self) -> None:
        """
        Handles exceptions thrown by workers and KeyboardInterrupts
        """
        # Obtain exception
        if self._worker_comms.exception_thrown():
            exception = self._cache[self._worker_comms.exception_thrown_by()].get_exception()
            cause = exception.__cause__
        else:
            self._worker_comms.signal_exception_thrown(MAIN_PROCESS)
            exception = KeyboardInterrupt()
            cause = exception

        # Pass error to progress bar, if there is one. We are interested in the cause of the exception, as that contains
        # the traceback from the worker. If there is no cause, we use the exception itself (e.g., a TimeoutError thrown
        # in the timeout handler)
        if self._progress_bar_handler is not None:
            self._progress_bar_handler.set_exception(cause or exception)

        # Join exception queue and progress bar, and terminate workers
        if self._progress_bar_handler is not None and self._progress_bar_handler.thread is not None:
            self._progress_bar_handler.thread.join()
        self.terminate()

        # Clear keep order event so we can safely reuse the WorkerPool and use (i)map_unordered after an (i)map call
        self._worker_comms.clear_keep_order()

        # Raise
        raise exception

    def stop_and_join(self, keep_alive: bool = False) -> None:
        """
        When ``keep_alive=False``: inserts a poison pill, grabs the exit results, waits until the tasks/results queues
        are done, and wait until all workers are finished.
        When ``keep_alive=True``: inserts a non-lethal poison pill, and waits until the tasks/results queues are done.

        Note that the results queue should be drained first before joining the workers, otherwise we can get a deadlock.
        For more information, see the warnings at:
        https://docs.python.org/3.4/library/multiprocessing.html#pipes-and-queues.

        :param keep_alive: Whether to keep the workers alive
        """
        if self._workers:
            # All tasks have been processed and results are in. Insert (non-lethal) poison pill. When keep_alive is
            # False, also signal the handler threads to stop
            if keep_alive:
                self._worker_comms.insert_non_lethal_poison_pill()
                self._worker_comms.reset_last_completed_task_info()
            else:
                self._worker_comms.insert_poison_pill()

            # Wait until all (non-lethal) poison pills have been consumed. When a worker's lifetime has been reached
            # just before consuming the poison pill, we need to restart them
            t = threading.Thread(target=self._worker_comms.join_task_queues, args=(keep_alive,))
            t.daemon = True
            t.start()
            while not self._worker_comms.exception_thrown():
                t.join(timeout=0.01)
                if not t.is_alive():
                    break

            # When an exception occurred in the above process (i.e., the worker init function raises), we need to
            # handle the exception (i.e., terminate and raise)
            if self._worker_comms.exception_thrown():
                self._handle_exception()

            # Join workers
            if not keep_alive:
                for wid, worker_process in enumerate(self._workers):
                    try:
                        worker_process.join()
                    except ValueError:
                        raise
                    # Added since Python 3.7. This will clean up any resources that are left. For some reason though,
                    # when using daemon processes and nested pools, a process can still be alive after the join when
                    # close is called and a ValueError is raised. So we wait a bit and check if the process will die. If
                    # not, then the GC can clean up the resources later.
                    if hasattr(worker_process, 'close'):
                        try_count = 5
                        while worker_process.is_alive() and try_count > 0:
                            time.sleep(0.01)
                            try_count -= 1
                        try:
                            worker_process.close()
                        except ValueError:
                            pass
                self._workers = []

            # Join the results queue, but do not close it. All results should be in the cache at this point (including
            # exit results, because the workers joined successfully or keep_alive=True and the exit function isn't
            # called), but we still need this queue for closing the results listener thread
            self._worker_comms.join_results_queues(keep_alive=True)

            # If an exception occurred in the exit function, we need to handle the exception (i.e., terminate and raise)
            if self._worker_comms.exception_thrown():
                self._handle_exception()

            if not keep_alive:
                self._handler_threads_stop_event.set()

                # Join results listener thread
                if self._results_handler_thread is not None and self._results_handler_thread.is_alive():
                    self._worker_comms.insert_poison_pill_results_listener()
                    self._results_handler_thread.join()
                    self._results_handler_thread = None

                # Join restart handler thread
                if self._restart_handler_thread is not None and self._restart_handler_thread.is_alive():
                    self._worker_comms.signal_worker_restart_condition()
                    self._restart_handler_thread.join()
                    self._restart_handler_thread = None

                # Join timeout handler thread
                if self._timeout_handler_thread is not None and self._timeout_handler_thread.is_alive():
                    self._timeout_handler_thread.join()
                    self._timeout_handler_thread = None

                # Join unexpected death handler thread
                if (self._unexpected_death_handler_thread is not None and
                        self._unexpected_death_handler_thread.is_alive()):
                    self._unexpected_death_handler_thread.join()
                    self._unexpected_death_handler_thread = None

            # Join and close the results queue if needed
            if not keep_alive:
                self._worker_comms.join_results_queues(keep_alive=False)

    def terminate(self) -> None:
        """
        Tries to do a graceful shutdown of the workers, by interrupting them. In the case processes deadlock it will
        send a sigkill.
        """
        # Set exception thrown so workers know to stop fetching new tasks
        if not self._worker_comms.exception_thrown():
            self._worker_comms.signal_exception_thrown(MAIN_PROCESS)
        self._handler_threads_stop_event.set()

        # When we're working with threads we have to wait for them to join. We can't kill threads in Python
        if self.pool_params.start_method == 'threading':
            threads = self._workers
        else:
            # Create cleanup threads such that processes can get killed simultaneously, which can save quite some time
            threads = []
            dont_wait_event = threading.Event()
            dont_wait_event.set()
            for worker_id, worker_process in enumerate(self._workers):
                t = threading.Thread(target=self._terminate_worker, args=(worker_id, worker_process, dont_wait_event))
                t.start()
                threads.append(t)

        # Wait until cleanup threads are done
        for t in threads:
            t.join()

        # Join results listener thread
        if self._results_handler_thread is not None and self._results_handler_thread.is_alive():
            self._worker_comms.insert_poison_pill_results_listener()
            self._results_handler_thread.join()
            self._results_handler_thread = None

        # Join restart thread
        if self._restart_handler_thread is not None and self._restart_handler_thread.is_alive():
            self._worker_comms.signal_worker_restart_condition()
            self._restart_handler_thread.join()
            self._restart_handler_thread = None

        # Join timeout handler thread
        if self._timeout_handler_thread is not None and self._timeout_handler_thread.is_alive():
            self._timeout_handler_thread.join()
            self._timeout_handler_thread = None

        # Join unexpected death handler thread
        if (self._unexpected_death_handler_thread is not None and
                self._unexpected_death_handler_thread.is_alive()):
            self._unexpected_death_handler_thread.join()
            self._unexpected_death_handler_thread = None

        # Drain and join the queues
        self._worker_comms.drain_queues()

        # Reset workers and cache. Keep only the init and exit results objects
        self._workers = []
        init_result, exit_results = self._cache[INIT_FUNC], self._cache[EXIT_FUNC]
        self._cache = {INIT_FUNC: init_result, EXIT_FUNC: exit_results}

    def _terminate_worker(self, worker_id: int, worker_process: Optional[mp.context.Process],
                          dont_wait_event: threading.Event) -> None:
        """
        Terminates a single worker process

        :param worker_id: Worker ID
        :param worker_process: Worker instance or None when it wasn't initialized yet
        :param dont_wait_event: Event object to indicate whether other termination threads should continue. I.e., when
            we set it to False, threads should wait.
        """
        # When a worker didn't start in the first place, we don't have to do anything
        if worker_process is None or worker_process.pid is None:
            return

        # We wait until workers are done terminating. However, we don't have all the patience in the world. When the
        # patience runs out we terminate them.
        try_count = 10
        while try_count > 0:
            # Signal handling in Windows is cumbersome, to say the least. Therefore, it handles error handling
            # differently. See Worker::_exit_gracefully_windows for more information.
            if not RUNNING_WINDOWS:
                try:
                    os.kill(worker_process.pid, signal.SIGUSR1)
                except (ProcessLookupError, ValueError):
                    pass

            self._worker_comms.wait_for_dead_worker(worker_id, timeout=0.1)
            if not self._worker_comms.is_worker_alive(worker_id):
                break

            # For properly joining, it can help if we try to get some results here. Workers can still be busy putting
            # items in queues under the hood
            self._worker_comms.drain_results_queue_terminate_worker(dont_wait_event)
            try_count -= 1
            if not dont_wait_event.is_set():
                dont_wait_event.wait()

        # Join the worker process
        try_count = 10
        while try_count > 0:
            try:
                worker_process.join(timeout=0.1)
                if not worker_process.is_alive():
                    break
            except ValueError:
                # For Windows compatibility
                pass

            # For properly joining, it can help if we try to get some results here. Workers can still be busy putting
            # items in queues under the hood, even at this point.
            self._worker_comms.drain_results_queue_terminate_worker(dont_wait_event)
            try_count -= 1
            if not dont_wait_event.is_set():
                dont_wait_event.wait()

        # If, after all this, the worker is still alive, we terminate it with a brutal kill signal. This shouldn't
        # really happen. But, better safe than sorry
        try:
            if worker_process.is_alive():
                worker_process.terminate()
                worker_process.join()
        except ValueError:
            # For Windows compatibility
            pass

        # Added since Python 3.7
        if hasattr(worker_process, 'close'):
            try:
                worker_process.close()
            except ValueError:
                pass

    def print_insights(self) -> None:
        """
        Prints insights per worker
        """
        print(self._worker_insights.get_insights_string())

    def get_insights(self) -> Dict:
        """
        Creates insights from the raw insight data

        :return: Dictionary containing worker insights
        """
        return self._worker_insights.get_insights()
