import logging
import multiprocessing as mp
import os
import queue
import signal
import threading
import warnings
from datetime import datetime
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Sized, Union

try:
    import numpy as np
    NUMPY_INSTALLED = True
except ImportError:
    np = None
    NUMPY_INSTALLED = False

from mpire.comms import WorkerComms
from mpire.context import DEFAULT_START_METHOD, RUNNING_WINDOWS
from mpire.dashboard.connection_utils import get_dashboard_connection_details
from mpire.insights import WorkerInsights
from mpire.params import check_map_parameters, CPUList, WorkerMapParams, WorkerPoolParams
from mpire.progress_bar import ProgressBarHandler
from mpire.signal import DisableKeyboardInterruptSignal
from mpire.tqdm_utils import TqdmManager
from mpire.utils import apply_numpy_chunking, chunk_tasks, set_cpu_affinity
from mpire.worker import MP_CONTEXTS, worker_factory

logger = logging.getLogger(__name__)


class WorkerPool:
    """
    A multiprocessing worker pool which acts like a ``multiprocessing.Pool``, but is faster and has more options.
    """

    def __init__(self, n_jobs: Optional[int] = None, daemon: bool = True, cpu_ids: CPUList = None,
                 shared_objects: Any = None, pass_worker_id: bool = False, use_worker_state: bool = False,
                 start_method: str = DEFAULT_START_METHOD, keep_alive: bool = False, use_dill: bool = False,
                 enable_insights: bool = False) -> None:
        """
        :param n_jobs: Number of workers to spawn. If ``None``, will use ``cpu_count()``
        :param daemon: Whether to start the child processes as daemon
        :param cpu_ids: List of CPU IDs to use for pinning child processes to specific CPUs. The list must be as long as
            the number of jobs used (if ``n_jobs`` equals ``None`` it must be equal to ``mpire.cpu_count()``), or the
            list must have exactly one element. In the former case, element x specifies the CPU ID(s) to use for child
            process x. In the latter case the single element specifies the CPU ID(s) for all child  processes to use. A
            single element can be either a single integer specifying a single CPU ID, or a list of integers specifying
            that a single child process can make use of multiple CPU IDs. If ``None``, CPU pinning will be disabled.
            Note that CPU pinning may only work on Linux based systems
        :param shared_objects: ``None`` or any other type of object (multiple objects can be wrapped in a single tuple).
            Shared objects is only passed on to the user function when it's not ``None``
        :param pass_worker_id: Whether to pass on a worker ID to the user function or not
        :param use_worker_state: Whether to let a worker have a worker state or not
        :param start_method: What process start method to use. Options for multiprocessing: ``'fork'`` (default, if
            available), ``'forkserver'`` and ``'spawn'`` (default, if ``'fork'`` isn't available). For multithreading
            use ``'threading'``. See https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods
            for more information and
            https://docs.python.org/3/library/multiprocessing.html#the-spawn-and-forkserver-start-methods for some
            caveats when using the ``'spawn'`` or ``'forkserver'`` methods
        :param keep_alive: When True it will keep workers alive after completing a map call, allowing to reuse workers
            when map is called with the same function and worker lifespan multiple times in a row
        :param use_dill: Whether to use dill as serialization backend. Some exotic types (e.g., lambdas, nested
            functions) don't work well when using ``spawn`` as start method. In such cased, use ``dill`` (can be a bit
            slower sometimes)
        :param enable_insights: Whether to enable worker insights. Might come at a small performance penalty (often
            neglible)
        """
        # Set parameters
        self.pool_params = WorkerPoolParams(n_jobs, cpu_ids, daemon, shared_objects, pass_worker_id, use_worker_state,
                                            start_method, keep_alive, use_dill, enable_insights)
        self.map_params = None  # type: Optional[WorkerMapParams]

        # Worker factory
        self.Worker = worker_factory(start_method, use_dill)

        # Multiprocessing context
        if start_method == 'threading':
            self.ctx = MP_CONTEXTS['threading']
        else:
            self.ctx = MP_CONTEXTS['mp_dill' if use_dill else 'mp'][start_method]

        # Container of the child processes and corresponding communication objects
        self._workers = []
        self._worker_comms = WorkerComms(self.ctx, self.pool_params.n_jobs, use_dill, start_method == 'threading')
        self._exit_results = None

        # Worker insights, used for profiling
        self._worker_insights = WorkerInsights(self.ctx, self.pool_params.n_jobs)

    def pass_on_worker_id(self, pass_on: bool = True) -> None:
        """
        Set whether to pass on the worker ID to the function to be executed or not (default= ``False``).

        The worker ID will be the first argument passed on to the function

        :param pass_on: Whether to pass on a worker ID to the user function or not
        """
        self.pool_params.pass_worker_id = pass_on

    def set_shared_objects(self, shared_objects: Any = None) -> None:
        """
        Set shared objects to pass to the workers.

        Shared objects will be copy-on-write. Process-aware shared objects (e.g., ``multiprocessing.Array``) can be used
        to write to the same object from multiple processes. When providing shared objects the provided function in the
        ``map`` function should receive the shared objects as its first argument if the worker ID is not passed on. If
        the worker ID is passed on the shared objects will be the second argument.

        :param shared_objects: ``None`` or any other type of object (multiple objects can be wrapped in a single tuple).
            Shared objects is only passed on to the user function when it's not ``None``
        """
        self.pool_params.shared_objects = shared_objects

    def set_use_worker_state(self, use_worker_state: bool = True) -> None:
        """
        Set whether or not each worker should have its own state variable. Each worker has its own state, so it's not
        shared between the workers.

        :param use_worker_state: Whether to let a worker have a worker state or not
        """
        self.pool_params.use_worker_state = use_worker_state

    def set_keep_alive(self, keep_alive: bool = True) -> None:
        """
        Set whether workers should be kept alive in between consecutive map calls.

        :param keep_alive: When True it will keep workers alive after completing a map call, allowing to reuse workers
            when map is called with the same function and worker lifespan multiple times in a row
        """
        self.pool_params.keep_alive = keep_alive

    def _start_workers(self, progress_bar: bool) -> None:
        """
        Spawns the workers and starts them so they're ready to start reading from the tasks queue.

        :param progress_bar: Whether there's a progress bar
        """
        # Init communication primitives
        logger.debug("Initializing comms")
        self._worker_comms.init_comms(self.map_params.worker_exit is not None, progress_bar)
        self._worker_insights.reset_insights(self.pool_params.enable_insights)
        self._exit_results = []

        # Start new workers
        logger.debug("Spinning up workers")
        for worker_id in range(self.pool_params.n_jobs):
            self._workers.append(self._start_worker(worker_id))
        logger.debug("Workers created")

    def _restart_workers(self) -> None:
        """
        Restarts workers that need to be restarted.
        """
        for worker_id in self._worker_comms.get_worker_restarts():
            # Obtain results from exit results queue (should be done before joining the worker)
            if self.map_params.worker_exit:
                self._exit_results.append(self._worker_comms.get_exit_results(worker_id))

            # Join worker
            self._worker_comms.reset_worker_restart(worker_id)
            self._workers[worker_id].join()

            # Start new worker
            self._workers[worker_id] = self._start_worker(worker_id)

    def _start_worker(self, worker_id: int) -> mp.Process:
        """
        Creates and starts a single worker

        :param worker_id: ID of the worker
        :return: Worker instance
        """
        # Disable the interrupt signal. We let the process die gracefully if it needs to
        with DisableKeyboardInterruptSignal():
            # Create worker
            w = self.Worker(worker_id, self.pool_params, self.map_params, self._worker_comms, self._worker_insights,
                            TqdmManager.get_connection_details(), get_dashboard_connection_details(), datetime.now())
            w.daemon = self.pool_params.daemon
            w.start()

            # Pin CPU if desired
            if self.pool_params.cpu_ids:
                set_cpu_affinity(w.pid, self.pool_params.cpu_ids[worker_id])

            return w

    def get_exit_results(self) -> List:
        """
        Obtain a list of exit results when an exit function is defined.

        :return: Exit results list
        """
        return self._exit_results

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
            worker_lifespan: Optional[int] = None, progress_bar: bool = False, progress_bar_position: int = 0,
            concatenate_numpy_output: bool = True, enable_insights: Optional[bool] = None,
            worker_init: Optional[Callable] = None, worker_exit: Optional[Callable] = None) -> Any:
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
            ``n_jobs * 2``
        :param chunk_size: Number of simultaneous tasks to give to a worker. When ``None`` it will use ``n_splits``.
        :param n_splits: Number of splits to use when ``chunk_size`` is ``None``. When both ``chunk_size`` and
            ``n_splits`` are ``None``, it will use ``n_splits = n_jobs * 64``.
        :param worker_lifespan: Number of tasks a worker can handle before it is restarted. If ``None``, workers will
            stay alive the entire time. Use this when workers use up too much memory over the course of time
        :param progress_bar: When ``True`` it will display a progress bar
        :param progress_bar_position: Denotes the position (line nr) of the progress bar. This is useful wel using
            multiple progress bars at the same time
        :param concatenate_numpy_output: When ``True`` it will concatenate numpy output to a single numpy array
        :param enable_insights: Whether to enable worker insights. Might come at a small performance penalty (often
            neglible).

            DEPRECATED in v2.3.0, to be removed in v2.6.0! Set ``enable_insights`` from the ``WorkerPool`` constructor
            instead.
        :param worker_init: Function to call each time a new worker starts. When passing on the worker ID the function
            should receive the worker ID as its first argument. If shared objects are provided the function should
            receive those as the next argument. If the worker state has been enabled it should receive a state variable
            as the next argument
        :param worker_exit: Function to call each time a worker exits. Return values will be fetched and made available
            through :obj:`mpire.WorkerPool.get_exit_results`. When passing on the worker ID the function should receive
            the worker ID as its first argument. If shared objects are provided the function should receive those as the
            next argument. If the worker state has been enabled it should receive a state variable as the next argument
        :return: List with ordered results
        """
        # Notify workers to keep order in mind
        self._worker_comms.set_keep_order()

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
                                     progress_bar, progress_bar_position, enable_insights, worker_init, worker_exit)

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
                      progress_bar_position: int = 0, enable_insights: Optional[bool] = None,
                      worker_init: Optional[Callable] = None, worker_exit: Optional[Callable] = None) -> Any:
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
            ``n_jobs * 2``
        :param chunk_size: Number of simultaneous tasks to give to a worker. When ``None`` it will use ``n_splits``.
        :param n_splits: Number of splits to use when ``chunk_size`` is ``None``. When both ``chunk_size`` and
            ``n_splits`` are ``None``, it will use ``n_splits = n_jobs * 64``.
        :param worker_lifespan: Number of tasks a worker can handle before it is restarted. If ``None``, workers will
            stay alive the entire time. Use this when workers use up too much memory over the course of time
        :param progress_bar: When ``True`` it will display a progress bar
        :param progress_bar_position: Denotes the position (line nr) of the progress bar. This is useful wel using
            multiple progress bars at the same time
        :param enable_insights: Whether to enable worker insights. Might come at a small performance penalty (often
            neglible).

            DEPRECATED in v2.3.0, to be removed in v2.6.0! Set ``enable_insights`` from the ``WorkerPool`` constructor
            instead.
        :param worker_init: Function to call each time a new worker starts. When passing on the worker ID the function
            should receive the worker ID as its first argument. If shared objects are provided the function should
            receive those as the next argument. If the worker state has been enabled it should receive a state variable
            as the next argument
        :param worker_exit: Function to call each time a worker exits. Return values will be fetched and made available
            through :obj:`mpire.WorkerPool.get_exit_results`. When passing on the worker ID the function should receive
            the worker ID as its first argument. If shared objects are provided the function should receive those as the
            next argument. If the worker state has been enabled it should receive a state variable as the next argument
        :return: List with unordered results
        """
        # Simply call imap and cast it to a list. This make sure all elements are there before returning
        return list(self.imap_unordered(func, iterable_of_args, iterable_len, max_tasks_active, chunk_size,
                                        n_splits, worker_lifespan, progress_bar, progress_bar_position,
                                        enable_insights, worker_init, worker_exit))

    def imap(self, func: Callable, iterable_of_args: Union[Sized, Iterable], iterable_len: Optional[int] = None,
             max_tasks_active: Optional[int] = None, chunk_size: Optional[int] = None, n_splits: Optional[int] = None,
             worker_lifespan: Optional[int] = None, progress_bar: bool = False,
             progress_bar_position: int = 0, enable_insights: Optional[bool] = None,
             worker_init: Optional[Callable] = None,
             worker_exit: Optional[Callable] = None) -> Generator[Any, None, None]:
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
            ``n_jobs * 2``
        :param chunk_size: Number of simultaneous tasks to give to a worker. When ``None`` it will use ``n_splits``.
        :param n_splits: Number of splits to use when ``chunk_size`` is ``None``. When both ``chunk_size`` and
            ``n_splits`` are ``None``, it will use ``n_splits = n_jobs * 64``.
        :param worker_lifespan: Number of tasks a worker can handle before it is restarted. If ``None``, workers will
            stay alive the entire time. Use this when workers use up too much memory over the course of time
        :param progress_bar: When ``True`` it will display a progress bar
        :param progress_bar_position: Denotes the position (line nr) of the progress bar. This is useful wel using
            multiple progress bars at the same time
        :param enable_insights: Whether to enable worker insights. Might come at a small performance penalty (often
            neglible).

            DEPRECATED in v2.3.0, to be removed in v2.6.0! Set ``enable_insights`` from the ``WorkerPool`` constructor
            instead.
        :param worker_init: Function to call each time a new worker starts. When passing on the worker ID the function
            should receive the worker ID as its first argument. If shared objects are provided the function should
            receive those as the next argument. If the worker state has been enabled it should receive a state variable
            as the next argument
        :param worker_exit: Function to call each time a worker exits. Return values will be fetched and made available
            through :obj:`mpire.WorkerPool.get_exit_results`. When passing on the worker ID the function should receive
            the worker ID as its first argument. If shared objects are provided the function should receive those as the
            next argument. If the worker state has been enabled it should receive a state variable as the next argument
        :return: Generator yielding ordered results
        """
        # Notify workers to keep order in mind
        self._worker_comms.set_keep_order()

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
                                                      progress_bar, progress_bar_position, enable_insights, worker_init,
                                                      worker_exit):

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
                       progress_bar_position: int = 0, enable_insights: Optional[bool] = None,
                       worker_init: Optional[Callable] = None,
                       worker_exit: Optional[Callable] = None) -> Generator[Any, None, None]:
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
            ``n_jobs * 2``
        :param chunk_size: Number of simultaneous tasks to give to a worker. When ``None`` it will use ``n_splits``.
        :param n_splits: Number of splits to use when ``chunk_size`` is ``None``. When both ``chunk_size`` and
            ``n_splits`` are ``None``, it will use ``n_splits = n_jobs * 64``.
        :param worker_lifespan: Number of tasks a worker can handle before it is restarted. If ``None``, workers will
            stay alive the entire time. Use this when workers use up too much memory over the course of time
        :param progress_bar: When ``True`` it will display a progress bar
        :param progress_bar_position: Denotes the position (line nr) of the progress bar. This is useful wel using
            multiple progress bars at the same time
        :param enable_insights: Whether to enable worker insights. Might come at a small performance penalty (often
            neglible).

            DEPRECATED in v2.3.0, to be removed in v2.6.0! Set ``enable_insights`` from the ``WorkerPool`` constructor
            instead.
        :param worker_init: Function to call each time a new worker starts. When passing on the worker ID the function
            should receive the worker ID as its first argument. If shared objects are provided the function should
            receive those as the next argument. If the worker state has been enabled it should receive a state variable
            as the next argument
        :param worker_exit: Function to call each time a worker exits. Return values will be fetched and made available
            through :obj:`mpire.WorkerPool.get_exit_results`. When passing on the worker ID the function should receive
            the worker ID as its first argument. If shared objects are provided the function should receive those as the
            next argument. If the worker state has been enabled it should receive a state variable as the next argument
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
        n_tasks, max_tasks_active, chunk_size, progress_bar = check_map_parameters(
            self.pool_params, iterable_of_args, iterable_len, max_tasks_active, chunk_size, n_splits, worker_lifespan,
            progress_bar, progress_bar_position
        )
        new_map_params = WorkerMapParams(func, worker_init, worker_exit, worker_lifespan)

        # enable_insights is deprecated as a map parameter
        if (self._workers and enable_insights is not None and self.pool_params.enable_insights is not None and
                enable_insights != self.pool_params.enable_insights):
            warnings.warn("Changing enable_insights is not allowed when keep_alive is set to True.",
                          RuntimeWarning, stacklevel=2)
        elif enable_insights is not None:
            warnings.warn("Deprecated in v2.3.0, to be removed in v2.6.0! Set enable_insights from the WorkerPool "
                          "constructor instead.", DeprecationWarning, stacklevel=2)
            self.pool_params.enable_insights = enable_insights

        # Start tqdm manager if a progress bar is desired. Will only start one when not already started. This has to be
        # done before starting the workers in case nested pools are used
        tqdm_manager_owner = TqdmManager.start_manager() if progress_bar else False

        # Chunk the function arguments. Make single arguments when we're not dealing with numpy arrays
        if not numpy_chunking:
            iterator_of_chunked_args = chunk_tasks(iterable_of_args, n_tasks, chunk_size,
                                                   n_splits or self.pool_params.n_jobs * 64)

        # Start workers if there aren't any. If they already exist check if we need to pass on new parameters
        if self._workers and self.map_params != new_map_params:
            self.map_params = new_map_params
            self._worker_comms.add_new_map_params(new_map_params)
        if not self._workers:
            self.map_params = new_map_params
            self._start_workers(progress_bar)

        # Create progress bar handler, which receives progress updates from the workers and updates the progress bar
        # accordingly
        try:
            with ProgressBarHandler(self.ctx, self.pool_params, self.map_params, progress_bar, n_tasks,
                                    progress_bar_position, self._worker_comms,
                                    self._worker_insights) as progress_bar_handler:
                try:
                    # Process all args in the iterable
                    n_active = 0
                    while not self._worker_comms.exception_thrown():
                        # Add task, only if allowed and if there are any
                        if n_active < max_tasks_active:
                            try:
                                self._worker_comms.add_task(next(iterator_of_chunked_args))
                                n_active += 1
                            except StopIteration:
                                break

                        # Check if new results are available, but don't wait for it
                        try:
                            yield from self._worker_comms.get_results(block=False)
                            n_active -= 1
                        except queue.Empty:
                            pass

                        # Restart workers if necessary
                        self._restart_workers()

                    # Obtain the results not yet obtained
                    while not self._worker_comms.exception_thrown() and n_active != 0:
                        try:
                            yield from self._worker_comms.get_results(block=True, timeout=0.01)
                            n_active -= 1
                        except queue.Empty:
                            pass

                        # Restart workers if necessary
                        self._restart_workers()

                    # Terminate if exception has been thrown at this point
                    if self._worker_comms.exception_thrown():
                        self._handle_exception(progress_bar_handler)

                    # All results are in: it's clean up time
                    self.stop_and_join(progress_bar_handler, keep_alive=self.pool_params.keep_alive)

                except KeyboardInterrupt:
                    self._handle_exception(progress_bar_handler)

            # Join exception queue, if it hasn't already
            logger.debug("Joining exception queue")
            self._worker_comms.join_exception_queue(self.pool_params.keep_alive)
            logger.debug("Done joining exception queue, now joining progress bar queue")
            self._worker_comms.join_progress_bar_task_completed_queue(self.pool_params.keep_alive)
            logger.debug("Done joining progress bar queue")

        finally:
            if tqdm_manager_owner:
                TqdmManager.stop_manager()

        # Log insights
        if enable_insights:
            logger.debug(self._worker_insights.get_insights_string())

    def _handle_exception(self, progress_bar_handler: Optional[ProgressBarHandler] = None) -> None:
        """
        Handles exceptions thrown by workers and KeyboardInterrupts

        :param progress_bar_handler: Progress bar handler
        """
        logger.debug("Exception occurred")

        # If we have a progress bar and this function is called because there was a KeyboardInterrupt (i.e., no
        # exception thrown), we pass the KeyboardInterrupt to the progress bar handler (using the exception queue)
        keyboard_interrupt = False
        with self._worker_comms.exception_lock:
            if not self._worker_comms.exception_thrown():
                keyboard_interrupt = True
                self._worker_comms.set_exception_thrown()
                if self._worker_comms.has_progress_bar() and progress_bar_handler is not None:
                    self._worker_comms.add_exception(KeyboardInterrupt, "KeyboardInterrupt")

        # When we're not dealing with a KeyboardInterrupt, obtain the exception thrown by a worker
        if keyboard_interrupt:
            err, traceback_str = KeyboardInterrupt, ""
        else:
            err, traceback_str = self._worker_comms.get_exception()
            self._worker_comms.task_done_exception()
        logger.debug(f"Exception obtained: {err}")

        # Join exception queue and progress bar, and terminate workers
        logger.debug("Joining exception queue")
        self._worker_comms.join_exception_queue()
        logger.debug("Exception queue joined")
        if progress_bar_handler and progress_bar_handler.process is not None:
            logger.debug("Joining progress bar handler")
            progress_bar_handler.process.join()
            logger.debug("Progress bar handler joined")
        self.terminate()

        # Clear keep order event so we can safely reuse the WorkerPool and use (i)map_unordered after an (i)map call
        self._worker_comms.clear_keep_order()

        # Raise
        logger.debug("Re-raising obtained exception")
        raise err(traceback_str)

    def stop_and_join(self, progress_bar_handler: Optional[ProgressBarHandler] = None,
                      keep_alive: bool = False) -> None:
        """
        When ``keep_alive=False``: inserts a poison pill, grabs the exit results, waits until the tasks/results queues
        are done, and wait until all workers are finished.
        When ``keep_alive=True``: inserts a non-lethal poison pill, and waits until the tasks/results queues are done.

        Note that the results queue should be drained first before joining the workers, otherwise we can get a deadlock.
        For more information, see the warnings at:
        https://docs.python.org/3.4/library/multiprocessing.html#pipes-and-queues.

        :param progress_bar_handler: Progress bar handler
        :param keep_alive: Whether to keep the workers alive
        """
        if self._workers:
            logger.debug(f"Cleaning up workers (keep_alive={keep_alive})")

            # All tasks have been processed and results are in. Insert (non-lethal) poison pill
            if keep_alive:
                self._worker_comms.insert_non_lethal_poison_pill()
                self._worker_comms.reset_last_completed_task_info()
            else:
                self._worker_comms.insert_poison_pill()

            # Wait until all (non-lethal) poison pills have been consumed. When a worker's lifetime has been reached
            # just before consuming the poison pill, we need to restart them
            logger.debug("Joining task queues")
            t = threading.Thread(target=self._worker_comms.join_task_queues, args=(keep_alive,))
            t.daemon = True
            t.start()
            while not self._worker_comms.exception_thrown():
                t.join(timeout=0.01)
                if not t.is_alive():
                    break
                self._restart_workers()
            logger.debug("Done joining task queues")

            # When an exception occurred in the above process (i.e., the worker init function raises), we need to handle
            # the exception (i.e., terminate and raise)
            if self._worker_comms.exception_thrown():
                self._handle_exception(progress_bar_handler)

            # Obtain results from the exit results queues (should be done before joining the workers)
            if not keep_alive and self.map_params.worker_exit:
                logger.debug("Obtaining exit results")
                self._exit_results.extend(self._worker_comms.get_exit_results_all_workers())
                self._worker_comms.set_all_exit_results_obtained()
                logger.debug("Done obtaining exit results")

            # If an exception occurred in the exit function, we need to handle the exception (i.e., terminate and raise)
            if self._worker_comms.exception_thrown():
                self._handle_exception(progress_bar_handler)

            # Join (exit) results queue (should be done immediately, as all (exit) results have been processed)
            logger.debug("Joining results queues")
            self._worker_comms.join_results_queues(keep_alive)
            logger.debug("Done joining results queues")

            # Join workers
            if not keep_alive:
                logger.debug("Joining workers")
                for worker_process in self._workers:
                    worker_process.join()
                    # Added since Python 3.7
                    if hasattr(worker_process, 'close'):
                        worker_process.close()
                self._workers = []
                logger.debug("Done joining workers")

            logger.debug("Cleaning up workers done")

    def terminate(self) -> None:
        """
        Tries to do a graceful shutdown of the workers, by interrupting them. In the case processes deadlock it will
        send a sigkill.
        """
        logger.debug("Terminating workers")

        # Set exception thrown so workers know to stop fetching new tasks
        self._worker_comms.set_exception_thrown()

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
        logger.debug("Terminating workers done")

        # Drain and join the queues
        logger.debug("Draining queues")
        self._worker_comms.drain_queues()
        logger.debug("Draining queues done")

        # Reset workers
        self._workers = []

    def _terminate_worker(self, worker_id: int, worker_process: mp.context.Process,
                          dont_wait_event: threading.Event) -> None:
        """
        Terminates a single worker process

        :param worker_id: Worker ID
        :param worker_process: Worker instance
        :param dont_wait_event: Event object to indicate whether other termination threads should continue. I.e., when
            we set it to False, threads should wait.
        """
        # We wait until workers are done terminating. However, we don't have all the patience in the world. When the
        # patience runs out we terminate them.
        try_count = 10
        while try_count > 0:
            # Signal handling in Windows is cumbersome, to say the least. Therefore, it handles error handling
            # differently. See Worker::_exit_gracefully_windows for more information.
            if not RUNNING_WINDOWS:
                try:
                    os.kill(worker_process.pid, signal.SIGUSR1)
                except ProcessLookupError:
                    pass

            self._worker_comms.wait_for_dead_worker(worker_id, timeout=0.1)
            if not self._worker_comms.is_worker_alive(worker_id):
                break

            # For properly joining, it can help if we try to get some results here. Workers can still be busy putting
            # items in queues under the hood
            self._worker_comms.drain_queues_terminate_worker(worker_id, dont_wait_event)
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
            self._worker_comms.drain_queues_terminate_worker(worker_id, dont_wait_event)
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
            worker_process.close()

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
