import logging
import multiprocess as mp
import os
import queue
import signal
import threading
from datetime import datetime
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Sized, Union

import numpy as np

from mpire.comms import WorkerComms
from mpire.exception import ExceptionHandler
from mpire.insights import WorkerInsights
from mpire.params import CPUList, WorkerPoolParams
from mpire.progress_bar import ProgressBarHandler
from mpire.signal import DelayedKeyboardInterrupt, DisableKeyboardInterruptSignal
from mpire.utils import apply_numpy_chunking, chunk_tasks
from mpire.worker import MP_CONTEXTS, worker_factory

logger = logging.getLogger(__name__)


class WorkerPool:
    """
    A multiprocessing worker pool which acts like a ``multiprocessing.Pool``, but is faster and has more options.
    """

    def __init__(self, n_jobs: Optional[int] = None, daemon: bool = True, cpu_ids: CPUList = None,
                 shared_objects: Any = None, pass_worker_id: bool = False, use_worker_state: bool = False,
                 start_method: str = 'fork', keep_alive: bool = False) -> None:
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
        :param start_method: What process start method to use. Options for multiprocessing: ``'fork'`` (default),
            ``'forkserver'`` and ``'spawn'``. For multithreading use ``'threading'``. See
            https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods for more information and
            https://docs.python.org/3/library/multiprocessing.html#the-spawn-and-forkserver-start-methods for some
            caveats when using the ``'spawn'`` or ``'forkserver'`` methods
        :param keep_alive: When True it will keep workers alive after completing a map call, allowing to reuse workers
            when map is called with the same function and worker lifespan multiple times in a row
        """
        # Set parameters
        self.params = WorkerPoolParams(n_jobs, daemon, cpu_ids, shared_objects, pass_worker_id, use_worker_state,
                                       start_method, keep_alive)

        # Multiprocessing context
        self.ctx = MP_CONTEXTS[start_method]

        # Worker factory
        self.Worker = worker_factory(start_method)

        # Container of the child processes and corresponding communication objects
        self._workers = []
        self._worker_comms = WorkerComms(self.ctx, self.params.n_jobs)
        self._exit_results = None

        # Worker insights, used for profiling
        self._worker_insights = WorkerInsights(self.ctx, self.params.n_jobs)

    def pass_on_worker_id(self, pass_on: bool = True) -> None:
        """
        Set whether to pass on the worker ID to the function to be executed or not (default= ``False``).

        The worker ID will be the first argument passed on to the function

        :param pass_on: Whether to pass on a worker ID to the user function or not
        """
        self.params.pass_worker_id = pass_on

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
        self.params.shared_objects = shared_objects

    def set_use_worker_state(self, use_worker_state: bool = True) -> None:
        """
        Set whether or not each worker should have its own state variable. Each worker has its own state, so it's not
        shared between the workers.

        :param use_worker_state: Whether to let a worker have a worker state or not
        """
        self.params.use_worker_state = use_worker_state

    def set_keep_alive(self, keep_alive: bool = True) -> None:
        """
        Set whether workers should be kept alive in between consecutive map calls.

        :param keep_alive: When True it will keep workers alive after completing a map call, allowing to reuse workers
            when map is called with the same function and worker lifespan multiple times in a row
        """
        self.params.keep_alive = keep_alive

    def _start_workers(self, func: Callable, worker_init: Optional[Callable], worker_exit: Optional[Callable],
                       worker_lifespan: Optional[int], progress_bar: bool) -> None:
        """
        Spawns the workers and starts them so they're ready to start reading from the tasks queue.

        :param func: Function to call each time new task arguments become available
        :param worker_init: Function to call each time a new worker starts
        :param worker_exit: Function to call each time a worker exits. Return values will be fetched and made available
            through `WorkerPool.get_exit_results`.
        :param worker_lifespan: Number of chunks a worker can handle before it is restarted. If ``None``, workers will
            stay alive the entire time. Use this when workers use up too much memory over the course of time
        :param progress_bar: Whether there's a progress bar
        """
        # Save params for later reference (for example, when restarting workers)
        self.params.set_map_params(func, worker_init, worker_exit, worker_lifespan)

        # Start new workers
        self._worker_comms.init_comms(self.params.worker_exit is not None, progress_bar)
        self._exit_results = []
        for worker_id in range(self.params.n_jobs):
            self._workers.append(self._start_worker(worker_id))

    def _restart_workers(self) -> None:
        """
        Restarts workers that need to be restarted.
        """
        for worker_id in self._worker_comms.get_worker_restarts():
            # Obtain results from exit results queue (should be done before joining the worker)
            if self.params.worker_exit:
                with DelayedKeyboardInterrupt():
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
            w = self.Worker(worker_id, self.params, self._worker_comms, self._worker_insights, datetime.now())
            w.daemon = self.params.daemon
            w.start()

            # Pin CPU if desired
            if self.params.cpu_ids:
                os.sched_setaffinity(w.pid, self.params.cpu_ids[worker_id])

            return w

    def get_exit_results(self) -> List:
        """
        Obtain a list of exit results when an exit function is defined.

        :return: Exit results list
        """
        return self._exit_results

    def stop_and_join(self) -> None:
        """
        Inserts a poison pill, grabs the exit results, and waits until all workers are finished.

        Note that the results queue should be drained first before joining the workers, otherwise we can get a deadlock.
        For more information, see the warnings at:
        https://docs.python.org/3.4/library/multiprocessing.html#pipes-and-queues.
        """
        if self._workers:
            # Insert poison pill
            logger.debug("Cleaning up workers")
            self._worker_comms.insert_poison_pill()

            # It can occur that some processes requested a restart while all tasks are already complete. This means that
            # processes won't get restarted anymore, although a poison pill is just added (at the time they we're still
            # barely alive). This results in a tasks queue never being empty which can lead to BrokenPipeErrors or
            # deadlocks. We can also not simply drain the tasks queue because workers could still be busy. To remedy
            # this problem we start a thread which waits for the tasks queue (including poison pills) to be empty. If
            # this thread isn't done that means that some tasks/poison pills are still there, meaning that there are
            # child processes which could/should be restarted, but aren't.
            t = threading.Thread(target=self._worker_comms.join_tasks_queue)
            t.start()
            while True:
                t.join(timeout=0.1)
                if not t.is_alive():
                    break
                self._restart_workers()

            # Obtain results from the exit results queues (should be done before joining the workers)
            if self.params.worker_exit:
                self._exit_results.extend(self._worker_comms.get_exit_results_all_workers())

            # Join queues and workers
            self._worker_comms.join_results_queues()
            for w in self._workers:
                w.join()
            self._workers = []
            logger.debug("Cleaning up workers done")

    def terminate(self) -> None:
        """
        Tries to do a graceful shutdown of the workers, by interrupting them. In the case processes deadlock it will
        send a sigkill.
        """
        logger.debug("Terminating workers")

        # Set exception thrown so workers know to stop fetching new tasks
        self._worker_comms.set_exception()

        # When we're working with threads we have to wait for them to join. We can't kill threads in Python
        if self.params.start_method == 'threading':
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

        # Reset variables
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
            worker_process.join(timeout=0.1)
            if not worker_process.is_alive():
                break

            # For properly joining, it can help if we try to get some results here. Workers can still be busy putting
            # items in queues under the hood, even at this point.
            self._worker_comms.drain_queues_terminate_worker(worker_id, dont_wait_event)
            try_count -= 1
            if not dont_wait_event.is_set():
                dont_wait_event.wait()

        # If, after all this, the worker is still alive, we terminate it with a brutal kill signal. This shouldn't
        # really happen. But, better safe than sorry
        if worker_process.is_alive():
            worker_process.terminate()
            worker_process.join()

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

    def map(self, func: Callable, iterable_of_args: Union[Sized, Iterable, np.ndarray],
            iterable_len: Optional[int] = None, max_tasks_active: Optional[int] = None,
            chunk_size: Optional[int] = None, n_splits: Optional[int] = None, worker_lifespan: Optional[int] = None,
            progress_bar: bool = False, progress_bar_position: int = 0,
            concatenate_numpy_output: bool = True, enable_insights: bool = False,
            worker_init: Optional[Callable] = None,
            worker_exit: Optional[Callable] = None) -> Union[List[Any], np.ndarray]:
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
            neglible)
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
        if isinstance(iterable_of_args, np.ndarray):
            iterable_of_args, iterable_len, chunk_size, n_splits = apply_numpy_chunking(iterable_of_args, iterable_len,
                                                                                        chunk_size, n_splits,
                                                                                        self.params.n_jobs)

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
        return (np.concatenate(sorted_results)
                if sorted_results and concatenate_numpy_output and isinstance(sorted_results[0], np.ndarray) else
                sorted_results)

    def map_unordered(self, func: Callable, iterable_of_args: Union[Sized, Iterable, np.ndarray],
                      iterable_len: Optional[int] = None, max_tasks_active: Optional[int] = None,
                      chunk_size: Optional[int] = None, n_splits: Optional[int] = None,
                      worker_lifespan: Optional[int] = None, progress_bar: bool = False,
                      progress_bar_position: int = 0, enable_insights: bool = False,
                      worker_init: Optional[Callable] = None, worker_exit: Optional[Callable] = None) -> List[Any]:
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
            neglible)
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

    def imap(self, func: Callable, iterable_of_args: Union[Sized, Iterable, np.ndarray],
             iterable_len: Optional[int] = None, max_tasks_active: Optional[int] = None,
             chunk_size: Optional[int] = None, n_splits: Optional[int] = None,
             worker_lifespan: Optional[int] = None, progress_bar: bool = False,
             progress_bar_position: int = 0, enable_insights: bool = False, worker_init: Optional[Callable] = None,
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
            neglible)
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
        if isinstance(iterable_of_args, np.ndarray):
            iterable_of_args, iterable_len, chunk_size, n_splits = apply_numpy_chunking(iterable_of_args, iterable_len,
                                                                                        chunk_size, n_splits,
                                                                                        self.params.n_jobs)

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

    def imap_unordered(self, func: Callable, iterable_of_args: Union[Sized, Iterable, np.ndarray],
                       iterable_len: Optional[int] = None, max_tasks_active: Optional[int] = None,
                       chunk_size: Optional[int] = None, n_splits: Optional[int] = None,
                       worker_lifespan: Optional[int] = None, progress_bar: bool = False,
                       progress_bar_position: int = 0, enable_insights: bool = False,
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
            neglible)
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
        if isinstance(iterable_of_args, np.ndarray):
            iterator_of_chunked_args, iterable_len, chunk_size, n_splits = apply_numpy_chunking(
                iterable_of_args, iterable_len, chunk_size, n_splits, self.params.n_jobs
            )

        # Check parameters and thereby obtain the number of tasks. The chunk_size and progress bar parameters could be
        # modified as well
        n_tasks, max_tasks_active, chunk_size, progress_bar = self.params.check_map_parameters(
            iterable_of_args, iterable_len, max_tasks_active, chunk_size, n_splits, worker_lifespan, progress_bar,
            progress_bar_position
        )

        # Chunk the function arguments. Make single arguments when we're dealing with numpy arrays
        if not isinstance(iterable_of_args, np.ndarray):
            iterator_of_chunked_args = chunk_tasks(iterable_of_args, n_tasks, chunk_size,
                                                   n_splits or self.params.n_jobs * 64)

        # Reset profiling stats
        self._worker_insights.reset_insights(enable_insights)

        # Start workers if there aren't any. If they already exist they must be restarted when either the function to
        # execute or the worker lifespan changes
        if self._workers and self.params.workers_need_restart(func, worker_init, worker_exit, worker_lifespan):
            self.stop_and_join()
        if not self._workers:
            logger.debug("Spinning up workers")
            self._start_workers(func, worker_init, worker_exit, worker_lifespan, bool(progress_bar))

        # Create exception, exit results, and progress bar handlers. The exception handler receives any exceptions
        # thrown by the workers, terminates everything and re-raise an exception in the main process. The exit results
        # handler fetches results from the exit function, if provided. The progress bar handler receives progress
        # updates from the workers and updates the progress bar accordingly
        with ExceptionHandler(self.terminate, self._worker_comms, bool(progress_bar)) as exception_handler, \
             ProgressBarHandler(func, self.params.n_jobs, progress_bar, n_tasks, progress_bar_position,
                                self._worker_comms, self._worker_insights):

            # Process all args in the iterable
            n_active = 0
            while not self._worker_comms.exception_caught():
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
            while not self._worker_comms.exception_caught() and n_active != 0:
                try:
                    yield from self._worker_comms.get_results(block=True, timeout=0.1)
                    n_active -= 1
                except queue.Empty:
                    pass

                # Restart workers if necessary
                self._restart_workers()

            # Clean up time. When keep_alive is set to True we won't join the workers. During the stop_and_join call an
            # error can occur as well, so we have to check once again whether an exception occurred and raise if it did
            exception_handler.raise_on_exception()
            if not self.params.keep_alive:
                self.stop_and_join()
                exception_handler.raise_on_exception()

        # Log insights
        if enable_insights:
            logger.debug(self._worker_insights.get_insights_string())

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
