import itertools
import os
import queue
import signal
import threading
import time
import warnings
from typing import Any, Callable, Generator, Iterable, List, Optional, Tuple, Union

import numpy as np
from tqdm.auto import tqdm

from mpire.exception import ExceptionHandler
from mpire.progress_bar import ProgressBarHandler
from mpire.signal import DelayedKeyboardInterrupt, DisableKeyboardInterruptSignal
from mpire.utils import apply_numpy_chunking, chunk_tasks
from mpire.worker import MP_CONTEXTS, worker_factory, mp


# Typedefs
CPUList = Optional[List[Union[int, List[int]]]]


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
        :param start_method: What process start method to use. Options for multiprocessing: 'fork' (default),
            'forkserver' and 'spawn'. For multithreading use 'threading'. See
            https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods for more information and
            https://docs.python.org/3/library/multiprocessing.html#the-spawn-and-forkserver-start-methods for some
            caveats when using the 'spawn' or 'forkserver' methods
        :param keep_alive: When True it will keep workers alive after completing a map call, allowing to reuse workers
            when map is called with the same function and worker lifespan multiple times in a row
        """
        # Set parameters
        self.n_jobs = n_jobs or mp.cpu_count()
        self.daemon = daemon
        self.cpu_ids = self._check_cpu_ids(cpu_ids)

        # Multiprocessing context
        self.ctx = MP_CONTEXTS[start_method]

        # Worker factory
        self.Worker = worker_factory(start_method)

        # Queue to pass on tasks to child processes
        self._tasks_queue = None

        # Queue where the child processes can pass on results
        self._results_queue = None

        # Array where the child processes can request a restart
        self._worker_done_array = None

        # Queue related to the progress bar. Child processes can pass on a random value whenever they are finished with
        # a job
        self._task_completed_queue = None

        # Queue where the child processes can pass on an encountered exception
        self._exception_queue = None

        # Lock object such that child processes can only throw one at a time. The Event object ensures only one
        # exception can be thrown
        self._exception_lock = self.ctx.Lock()
        self._exception_thrown = self.ctx.Event()

        # Whether or not to inform the child processes to keep order in mind (for the map functions)
        self._keep_order = mp.Event()

        # Container of the child processes
        self._workers = []

        # User provided shared objects to pass on to the user provided function
        self.shared_objects = shared_objects

        # User provided function to call
        self.func_pointer = None

        # Number of (chunks of) jobs a child process can process before requesting a restart
        self.worker_lifespan = None

        # Whether to pass on the worker ID to the user provided function
        self.pass_worker_id = pass_worker_id

        # Whether to let a worker have a worker state
        self.use_worker_state = use_worker_state

        # Whether to keep workers alive after completing a map call
        self.keep_alive = keep_alive

        # Whether or not an exception was caught by one of the child processes
        self.exception_caught = mp.Event()

    def _check_cpu_ids(self, cpu_ids: CPUList) -> List[List[int]]:
        """
        Checks the cpu_ids parameter for correctness

        :param cpu_ids: List of CPU IDs to use for pinning child processes to specific CPUs. The list must be as long as
            the number of jobs used (if ``n_jobs`` equals ``None`` it must be equal to ``mpire.cpu_count()``), or the
            list must have exactly one element. In the former case, element x specifies the CPU ID(s) to use for child
            process x. In the latter case the single element specifies the CPU ID(s) for all child  processes to use. A
            single element can be either a single integer specifying a single CPU ID, or a list of integers specifying
            that a single child process can make use of multiple CPU IDs. If ``None``, CPU pinning will be disabled.
            Note that CPU pinning may only work on Linux based systems
        :return: cpu_ids
        """
        # Check CPU IDs
        converted_cpu_ids = []
        if cpu_ids:
            # Check number of arguments
            if len(cpu_ids) != 1 and len(cpu_ids) != self.n_jobs:
                raise ValueError("Number of CPU IDs (%d) does not match number of jobs (%d)" %
                                 (len(cpu_ids), self.n_jobs))

            # Convert CPU IDs to proper format and find the max and min CPU ID
            max_cpu_id = 0
            min_cpu_id = 0
            for cpu_id in cpu_ids:
                if isinstance(cpu_id, list):
                    converted_cpu_ids.append(cpu_id)
                    max_cpu_id = max(max_cpu_id, max(cpu for cpu in cpu_id))
                    min_cpu_id = min(min_cpu_id, min(cpu for cpu in cpu_id))
                elif isinstance(cpu_id, int):
                    converted_cpu_ids.append([cpu_id])
                    max_cpu_id = max(max_cpu_id, cpu_id)
                    min_cpu_id = min(min_cpu_id, cpu_id)
                else:
                    raise TypeError("CPU ID(s) must be either a list or a single integer")

            # Check max CPU ID
            if max_cpu_id >= mp.cpu_count():
                raise ValueError("CPU ID %d exceeds the maximum CPU ID available on your system: %d" %
                                 (max_cpu_id, mp.cpu_count() - 1))

            # Check min CPU ID
            if min_cpu_id < 0:
                raise ValueError("CPU IDs cannot be negative")

        # If only one item is given, use this item for all child processes
        if len(converted_cpu_ids) == 1:
            converted_cpu_ids = list(itertools.repeat(converted_cpu_ids[0], self.n_jobs))

        return converted_cpu_ids

    def pass_on_worker_id(self, pass_on: bool = True) -> None:
        """
        Set whether to pass on the worker ID to the function to be executed or not (default= ``False``).

        The worker ID will be the first argument passed on to the function

        :param pass_on: Whether to pass on a worker ID to the user function or not
        """
        self.pass_worker_id = pass_on

    def set_shared_objects(self, shared_objects: Any = None) -> None:
        """
        Set shared objects to pass to the workers.

        Shared objects will be copy-on-write. Process-aware shared objects (e.g., ``multiprocessing.Array``) can be used
        to write to the same object from multiple processes. When providing shared objects the provided function pointer
        in the ``map`` function should receive the shared objects as its first argument if the worker ID is not passed
        on. If the worker ID is passed on the shared objects will be the second argument.

        :param shared_objects: ``None`` or any other type of object (multiple objects can be wrapped in a single tuple).
            Shared objects is only passed on to the user function when it's not ``None``
        """
        self.shared_objects = shared_objects

    def set_use_worker_state(self, use_worker_state: bool = True) -> None:
        """
        Set whether or not each worker should have its own state variable. Each worker has its own state, so it's not
        shared between the workers.

        :param use_worker_state: Whether to let a worker have a worker state or not
        """
        self.use_worker_state = use_worker_state

    def set_keep_alive(self, keep_alive: bool = True) -> None:
        """
        Set whether workers should be kept alive in between consecutive map calls.

        :param keep_alive: When True it will keep workers alive after completing a map call, allowing to reuse workers
            when map is called with the same function and worker lifespan multiple times in a row
        """
        self.keep_alive = keep_alive

    def start_workers(self, func_pointer: Callable, worker_lifespan: Optional[int]) -> None:
        """
        Spawns the workers and starts them so they're ready to start reading from the tasks queue.

        :param func_pointer: Function pointer to call each time new task arguments become available
        :param worker_lifespan: Number of chunks a worker can handle before it is restarted. If ``None``, workers will
            stay alive the entire time. Use this when workers use up too much memory over the course of time
        """
        # Save params for later reference (for example, when restarting workers)
        self.func_pointer = func_pointer
        self.worker_lifespan = worker_lifespan

        # Start new workers
        self._tasks_queue = self.ctx.JoinableQueue()
        self._results_queue = self.ctx.JoinableQueue()
        self._worker_done_array = self.ctx.Array('b', self.n_jobs, lock=False)
        self._exception_queue = self.ctx.JoinableQueue()
        self._exception_thrown.clear()
        self.exception_caught.clear()
        for worker_id in range(self.n_jobs):
            self._workers.append(self._start_worker(worker_id))

    def _restart_workers(self) -> None:
        """
        Restarts workers that need to be restarted.
        """
        for worker_id, restart_worker in enumerate(self._worker_done_array):

            # Check if it needs restarting
            if restart_worker:
                self._worker_done_array[worker_id] = False
                self._workers[worker_id].join()

                # Start worker
                self._workers[worker_id] = self._start_worker(worker_id)

    def _start_worker(self, worker_id: int) -> mp.Process:
        """
        Creates and starts a single worker

        :param worker_id: ID of the worker
        :return: Worker instance
        """
        with DisableKeyboardInterruptSignal():
            # Create worker
            w = self.Worker(worker_id, self._tasks_queue, self._results_queue, self._worker_done_array,
                            self._task_completed_queue, self._exception_queue, self._exception_lock,
                            self._exception_thrown, self.func_pointer, self._keep_order.is_set(), self.shared_objects,
                            self.worker_lifespan, self.pass_worker_id, self.use_worker_state)
            w.daemon = self.daemon
            w.start()

            # Pin CPU if desired
            if self.cpu_ids:
                os.sched_setaffinity(w.pid, self.cpu_ids[worker_id])

            return w

    def add_task(self, args: Any) -> None:
        """
        Add a task to the queue so a worker can process it.

        :param args: A tuple of arguments to pass to a worker, which acts upon it
        """
        with DelayedKeyboardInterrupt():
            self._tasks_queue.put(args, block=True)

    def get_result(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        """
        Obtain the next result from the results queue.

        :param block: Whether to block (wait for results) or not
        :param timeout: How long to wait for results in case ``block==True``
        :return: The next result from the queue, which is the result of calling the function pointer
        """
        with DelayedKeyboardInterrupt():
            results = self._results_queue.get(block=block, timeout=timeout)
            self._results_queue.task_done()
        return results

    def insert_poison_pill(self) -> None:
        """
        Tell the workers their job is done by killing them brutally.
        """
        for _ in self._workers:
            self.add_task('\0')

    stop_workers = insert_poison_pill

    def join(self) -> None:
        """
        Blocks until all workers are finished working.

        Note that the results queue should be drained first before joining the workers, otherwise we can get a deadlock.
        For more information, see the warnings at:
        https://docs.python.org/3.4/library/multiprocessing.html#pipes-and-queues.
        """
        for w in self._workers:
            w.join()

    def stop_and_join(self) -> None:
        """
        Inserts a poison pill and waits until all workers are finished.

        Note that the results queue should be drained first before joining the workers, otherwise we can get a deadlock.
        For more information, see the warnings at:
        https://docs.python.org/3.4/library/multiprocessing.html#pipes-and-queues.
        """
        if self._workers:
            # Insert poison pill
            self.insert_poison_pill()

            # It can occur that some processes requested a restart while all tasks are already complete. This means that
            # processes won't get restarted anymore, although a poison pill is just added (at the time they we're still
            # barely alive). This results in a tasks queue never being empty which can lead to BrokenPipeErrors or
            # deadlocks. We can also not simply drain the tasks queue because workers could still be busy. To remedy
            # this problem we start a thread which waits for the tasks queue (including poison pills) to be empty. If
            # this thread isn't done that means that some tasks/poison pills are still there, meaning that there are
            # child processes which could/should be restarted, but aren't.
            t = threading.Thread(target=self._handle_tasks_done)
            t.start()
            while True:
                t.join(timeout=0.1)
                if not t.is_alive():
                    break
                self._restart_workers()

            # Join workers
            self.join()

            # Join queues
            self._tasks_queue.join()
            self._results_queue.join()

            # Reset variables
            self._workers = []

    def _handle_tasks_done(self) -> None:
        """
        Blocks until the tasks queue is joinable
        """
        self._tasks_queue.join()

    def terminate(self) -> None:
        """
        Does not wait until all workers are finished, but terminates them.
        """
        # Set exception thrown so workers know to stop fetching new tasks
        self._exception_thrown.set()

        # Create cleanup threads such that processes can get killed simultaneously, which can save quite some time
        threads = []
        for w in self._workers:
            t = threading.Thread(target=self._terminate_worker, args=(w,))
            t.start()
            threads.append(t)

        # Wait until cleanup threads are done
        for t in threads:
            t.join()

        # Drain and join the queues
        self._drain_and_join_queue(self._tasks_queue)
        self._drain_and_join_queue(self._results_queue)

        # Reset variables
        self._workers = []

    @staticmethod
    def _terminate_worker(process: mp.Process) -> None:
        """
        Terminates a single worker

        :param process: Worker instance
        """
        # We wait until workers are done terminating. However, we don't have all the patience in the world and sometimes
        # workers can get stuck when starting up when an exception is handled. So when the patience runs out we
        # terminate them.
        try_count = 3
        while process.is_alive() and try_count > 0:
            # Send interrupt signal so the processes can die gracefully. Sometimes this will throw an exception, e.g.
            # when processes are in the middle of restarting
            try:
                os.kill(process.pid, signal.SIGUSR1)
            except ProcessLookupError:
                pass
            try_count -= 1
            time.sleep(0.1)

        # If a graceful kill is not possible, terminate the process.
        if process.is_alive():
            process.terminate()

        # Join worker
        process.join()

    @staticmethod
    def _drain_and_join_queue(q: mp.JoinableQueue, join: bool = True) -> None:
        """
        Drains a queue completely, such that it is joinable

        :param q: Queue to join
        :param join: Whether to join the queue or not
        """
        # Do nothing when it's not set
        if q is None:
            return

        # Extract all items left in the queue
        try:
            while True:
                q.get(block=False)
                q.task_done()
        except (queue.Empty, EOFError):
            pass

        # Call task done up to the point where we get a ValueError. We need to do this when child processes already
        # started processing on some tasks and got terminated half-way.
        try:
            while True:
                q.task_done()
        except ValueError:
            pass

        # Join
        if join:
            q.join()

    def __enter__(self) -> 'WorkerPool':
        """
        Enable the use of the ``with`` statement.
        """
        return self

    def __exit__(self, *_: Any) -> None:
        """
        Enable the use of the ``with`` statement. Gracefully terminates workers when the task queue is not None.
        """
        if self._tasks_queue is not None:
            self.terminate()

    def map(self, func_pointer: Callable, iterable_of_args: Union[Iterable, np.ndarray],
            iterable_len: Optional[int] = None, max_tasks_active: Optional[int] = None,
            chunk_size: Optional[int] = None, n_splits: Optional[int] = None, worker_lifespan: Optional[int] = None,
            progress_bar: bool = False, progress_bar_position: int = 0,
            concatenate_numpy_output: bool = True) -> Union[List[Any], np.ndarray]:
        """
        Same as ``multiprocessing.map()``. Also allows a user to set the maximum number of tasks available in the queue.
        Note that this function can be slower than the unordered version.

        :param func_pointer: Function pointer to call each time new task arguments become available. When passing on the
            worker ID the function should receive the worker ID as its first argument. If shared objects are provided
            the function should receive those as the next argument. If the worker state has been enabled it should
            receive a state variable as the next argument
        :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker, which
            passes it to the function pointer
        :param iterable_len: Number of elements in the ``iterable_of_args``. When chunk_size is set to ``None`` it needs
            to know the number of tasks. This can either be provided by implementing the ``__len__`` function on the
            iterable object, or by specifying the number of tasks
        :param max_tasks_active: Maximum number of active tasks in the queue. Use ``None`` to not limit the queue
        :param chunk_size: Number of simultaneous tasks to give to a worker. If ``None`` it will generate ``n_jobs * 4``
            number of chunks
        :param n_splits: Number of splits to use when ``chunk_size`` is ``None``
        :param worker_lifespan: Number of chunks a worker can handle before it is restarted. If ``None``, workers will
            stay alive the entire time. Use this when workers use up too much memory over the course of time
        :param progress_bar: When ``True`` it will display a progress bar
        :param progress_bar_position: Denotes the position (line nr) of the progress bar. This is useful wel using
            multiple progress bars at the same time
        :param concatenate_numpy_output: When ``True`` it will concatenate numpy output to a single numpy array
        :return: List with ordered results
        """
        # Notify workers to keep order in mind
        self._keep_order.set()

        # If we're dealing with numpy arrays, we have to chunk them here already
        if isinstance(iterable_of_args, np.ndarray):
            iterable_of_args, iterable_len, chunk_size, n_splits = apply_numpy_chunking(iterable_of_args, iterable_len,
                                                                                        chunk_size, n_splits,
                                                                                        self.n_jobs)

        # Process all args
        if iterable_len is None and hasattr(iterable_of_args, '__len__'):
            iterable_len = len(iterable_of_args)
        results = self.map_unordered(func_pointer, ((args_idx, args) for args_idx, args in enumerate(iterable_of_args)),
                                     iterable_len, max_tasks_active, chunk_size, n_splits, worker_lifespan,
                                     progress_bar, progress_bar_position)

        # Notify workers to forget about order
        self._keep_order.clear()

        # Rearrange and return
        sorted_results = [result[1] for result in sorted(results, key=lambda result: result[0])]

        # Convert back to numpy if necessary
        return (np.concatenate(sorted_results)
                if sorted_results and concatenate_numpy_output and isinstance(sorted_results[0], np.ndarray) else
                sorted_results)

    def map_unordered(self, func_pointer: Callable, iterable_of_args: Union[Iterable, np.ndarray],
                      iterable_len: Optional[int] = None, max_tasks_active: Optional[int] = None,
                      chunk_size: Optional[int] = None, n_splits: Optional[int] = None,
                      worker_lifespan: Optional[int] = None, progress_bar: bool = False,
                      progress_bar_position: int = 0) -> List[Any]:
        """
        Same as ``multiprocessing.map()``, but unordered. Also allows a user to set the maximum number of tasks
        available in the queue.

        :param func_pointer: Function pointer to call each time new task arguments become available. When passing on the
            worker ID the function should receive the worker ID as its first argument. If shared objects are provided
            the function should receive those as the next argument. If the worker state has been enabled it should
            receive a state variable as the next argument
        :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker, which
            passes it to the function pointer
        :param iterable_len: Number of elements in the ``iterable_of_args``. When chunk_size is set to ``None`` it needs
            to know the number of tasks. This can either be provided by implementing the ``__len__`` function on the
            iterable object, or by specifying the number of tasks
        :param max_tasks_active: Maximum number of active tasks in the queue. Use ``None`` to not limit the queue
        :param chunk_size: Number of simultaneous tasks to give to a worker. If ``None`` it will generate ``n_jobs * 4``
            number of chunks
        :param n_splits: Number of splits to use when ``chunk_size`` is ``None``
        :param worker_lifespan: Number of chunks a worker can handle before it is restarted. If ``None``, workers will
            stay alive the entire time. Use this when workers use up too much memory over the course of time
        :param progress_bar: When ``True`` it will display a progress bar
        :param progress_bar_position: Denotes the position (line nr) of the progress bar. This is useful wel using
            multiple progress bars at the same time
        :return: List with unordered results
        """
        # Simply call imap and cast it to a list. This make sure all elements are there before returning
        return list(self.imap_unordered(func_pointer, iterable_of_args, iterable_len, max_tasks_active, chunk_size,
                                        n_splits, worker_lifespan, progress_bar, progress_bar_position))

    def imap(self, func_pointer: Callable, iterable_of_args: Union[Iterable, np.ndarray],
             iterable_len: Optional[int] = None, max_tasks_active: Optional[int] = None,
             chunk_size: Optional[int] = None, n_splits: Optional[int] = None,
             worker_lifespan: Optional[int] = None, progress_bar: bool = False,
             progress_bar_position: int = 0) -> Generator[Any, None, None]:
        """
        Same as ``multiprocessing.imap_unordered()``, but ordered. Also allows a user to set the maximum number of
        tasks available in the queue.

        :param func_pointer: Function pointer to call each time new task arguments become available. When passing on the
            worker ID the function should receive the worker ID as its first argument. If shared objects are provided
            the function should receive those as the next argument. If the worker state has been enabled it should
            receive a state variable as the next argument
        :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker, which
            passes it to the function pointer
        :param iterable_len: Number of elements in the ``iterable_of_args``. When chunk_size is set to ``None`` it needs
            to know the number of tasks. This can either be provided by implementing the ``__len__`` function on the
            iterable object, or by specifying the number of tasks
        :param max_tasks_active: Maximum number of active tasks in the queue. Use ``None`` to not limit the queue
        :param chunk_size: Number of simultaneous tasks to give to a worker. If ``None`` it will generate ``n_jobs * 4``
            number of chunks
        :param n_splits: Number of splits to use when ``chunk_size`` is ``None``
        :param worker_lifespan: Number of chunks a worker can handle before it is restarted. If ``None``, workers will
            stay alive the entire time. Use this when workers use up too much memory over the course of time
        :param progress_bar: When ``True`` it will display a progress bar
        :param progress_bar_position: Denotes the position (line nr) of the progress bar. This is useful wel using
            multiple progress bars at the same time
        :return: Generator yielding ordered results
        """
        # Notify workers to keep order in mind
        self._keep_order.set()

        # If we're dealing with numpy arrays, we have to chunk them here already
        if isinstance(iterable_of_args, np.ndarray):
            iterable_of_args, iterable_len, chunk_size, n_splits = apply_numpy_chunking(iterable_of_args, iterable_len,
                                                                                        chunk_size, n_splits,
                                                                                        self.n_jobs)

        # Yield results in order
        next_result_idx = 0
        tmp_results = {}
        if iterable_len is None and hasattr(iterable_of_args, '__len__'):
            iterable_len = len(iterable_of_args)
        for result_idx, result in self.imap_unordered(func_pointer, ((args_idx, args) for args_idx, args
                                                      in enumerate(iterable_of_args)), iterable_len, max_tasks_active,
                                                      chunk_size, n_splits, worker_lifespan, progress_bar,
                                                      progress_bar_position):

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
        self._keep_order.clear()

    def imap_unordered(self, func_pointer: Callable, iterable_of_args: Union[Iterable, np.ndarray],
                       iterable_len: Optional[int] = None, max_tasks_active: Optional[int] = None,
                       chunk_size: Optional[int] = None, n_splits: Optional[int] = None,
                       worker_lifespan: Optional[int] = None, progress_bar: bool = False,
                       progress_bar_position: int = 0) -> Generator[Any, None, None]:
        """
        Same as ``multiprocessing.imap_unordered()``. Also allows a user to set the maximum number of tasks available in
        the queue.

        :param func_pointer: Function pointer to call each time new task arguments become available. When passing on the
            worker ID the function should receive the worker ID as its first argument. If shared objects are provided
            the function should receive those as the next argument. If the worker state has been enabled it should
            receive a state variable as the next argument
        :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker, which
            passes it to the function pointer
        :param iterable_len: Number of elements in the ``iterable_of_args``. When chunk_size is set to ``None`` it needs
            to know the number of tasks. This can either be provided by implementing the ``__len__`` function on the
            iterable object, or by specifying the number of tasks
        :param max_tasks_active: Maximum number of active tasks in the queue. Use ``None`` to not limit the queue
        :param chunk_size: Number of simultaneous tasks to give to a worker. If ``None`` it will generate ``n_jobs * 4``
            number of chunks
        :param n_splits: Number of splits to use when ``chunk_size`` is ``None``
        :param worker_lifespan: Number of chunks a worker can handle before it is restarted. If ``None``, workers will
            stay alive the entire time. Use this when workers use up too much memory over the course of time
        :param progress_bar: When ``True`` it will display a progress bar
        :param progress_bar_position: Denotes the position (line nr) of the progress bar. This is useful wel using
            multiple progress bars at the same time
        :return: Generator yielding unordered results
        """
        # If we're dealing with numpy arrays, we have to chunk them here already
        if isinstance(iterable_of_args, np.ndarray):
            iterator_of_chunked_args, iterable_len, chunk_size, n_splits = apply_numpy_chunking(
                iterable_of_args, iterable_len, chunk_size, n_splits, self.n_jobs
            )

        # Check parameters and thereby obtain the number of tasks. The chunk_size and progress bar parameters could be
        # modified as well
        n_tasks, chunk_size, progress_bar = self._check_map_parameters(iterable_of_args, iterable_len, max_tasks_active,
                                                                       chunk_size, n_splits, worker_lifespan,
                                                                       progress_bar, progress_bar_position)

        # Chunk the function arguments. Make single arguments when we're dealing with numpy arrays
        if not isinstance(iterable_of_args, np.ndarray):
            iterator_of_chunked_args = chunk_tasks(iterable_of_args, n_tasks, chunk_size, n_splits or self.n_jobs * 4)

        # Start workers if there aren't any. If they already exist they must be restarted when either the function to
        # execute or the worker lifespan changes
        if self._workers and (func_pointer != self.func_pointer or worker_lifespan != self.worker_lifespan):
            self.stop_and_join()
        if not self._workers:
            self.start_workers(func_pointer, worker_lifespan)

        # Create exception and progress bar handlers. The exception handler will receive any exceptions thrown by the
        # workers, terminates everything and re-raise an exceptoin in the main process. The progress bar handler will
        # receive updates from the workers and updates the progress bar accordingly
        with ExceptionHandler(self.terminate, self._exception_queue, self.exception_caught, self._keep_order,
                              progress_bar is not None) as exception_handler, \
             ProgressBarHandler(func_pointer, progress_bar, n_tasks, progress_bar_position, self._task_completed_queue,
                                self._exception_queue, self.exception_caught):

            # Process all args in the iterable. If maximum number of active tasks is None, we avoid all the if and
            # try-except clauses to speed up the process.
            n_active = 0
            if max_tasks_active == 'n_jobs*2':
                max_tasks_active = self.n_jobs * 2

            if max_tasks_active is None:
                for chunked_args in iterator_of_chunked_args:
                    # Stop given tasks when an exception was caught
                    if self.exception_caught.is_set():
                        break

                    # Add task
                    self.add_task(chunked_args)
                    n_active += 1

                    # Restart workers if necessary
                    self._restart_workers()

            elif isinstance(max_tasks_active, int):
                while not self.exception_caught.is_set():
                    # Add task, only if allowed and if there are any
                    if n_active < max_tasks_active:
                        try:
                            self.add_task(next(iterator_of_chunked_args))
                            n_active += 1
                        except StopIteration:
                            break

                    # Check if new results are available, but don't wait for it
                    try:
                        yield from self.get_result(block=False)
                        n_active -= 1
                    except queue.Empty:
                        pass

                    # Restart workers if necessary
                    self._restart_workers()

            # Obtain the results not yet obtained
            while not self.exception_caught.is_set() and n_active != 0:
                try:
                    yield from self.get_result(block=True, timeout=0.1)
                    n_active -= 1
                except queue.Empty:
                    pass

                # Restart workers if necessary
                self._restart_workers()

            # Clean up time. When keep_alive is set to True we won't join the workers
            exception_handler.raise_on_exception()
            if not self.keep_alive:
                self.stop_and_join()

    def _check_map_parameters(self, iterable_of_args: Union[Iterable, np.ndarray], iterable_len: Optional[int],
                              max_tasks_active: Optional[int], chunk_size: Optional[int], n_splits: Optional[int],
                              worker_lifespan: Optional[int], progress_bar: bool, progress_bar_position: int) \
            -> Tuple[Optional[int], Optional[int], Union[bool, tqdm]]:
        """
        Check the parameters provided to any (i)map function. Also extracts the number of tasks and can modify the
        ``chunk_size`` and ``progress_bar`` parameters.

        :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker, which
            passes it to the function pointer
        :param iterable_len: Number of elements in the ``iterable_of_args``. When chunk_size is set to ``None`` it needs
            to know the number of tasks. This can either be provided by implementing the ``__len__`` function on the
            iterable object, or by specifying the number of tasks
        :param max_tasks_active: Maximum number of active tasks in the queue. Use ``None`` to not limit the queue
        :param chunk_size: Number of simultaneous tasks to give to a worker. If ``None`` it will generate ``n_jobs * 4``
            number of chunks
        :param n_splits: Number of splits to use when ``chunk_size`` is ``None``
        :param worker_lifespan: Number of chunks a worker can handle before it is restarted. If ``None``, workers will
            stay alive the entire time. Use this when workers use up too much memory over the course of time
        :param progress_bar: When ``True`` it will display a progress bar
        :param progress_bar_position: Denotes the position (line nr) of the progress bar. This is useful wel using
            multiple progress bars at the same time
        :return: Number of tasks, chunk size, progress bar
        """
        # Get number of tasks
        n_tasks = None
        if iterable_len is not None:
            n_tasks = iterable_len
        elif hasattr(iterable_of_args, '__len__'):
            n_tasks = len(iterable_of_args)
        elif chunk_size is None or progress_bar:
            warnings.simplefilter('default')
            warnings.warn('Failed to obtain length of iterable when chunk size or number of splits is None and/or a '
                          'progress bar is requested. Chunk size is set to 1 and no progress bar will be shown. '
                          'Remedy: either provide an iterable with a len() function or specify iterable_len in the '
                          'function call', RuntimeWarning, stacklevel=2)
            chunk_size = 1
            progress_bar = False

        # Check chunk_size parameter
        if chunk_size is not None:
            if not isinstance(chunk_size, (int, float)):
                raise TypeError('chunk_size should be either None or an integer value')
            elif chunk_size <= 0:
                raise ValueError('chunk_size should be a positive integer > 0')

        # Check n_splits parameter (only when chunk_size is None)
        else:
            if not (isinstance(n_splits, int) or n_splits is None):
                raise TypeError('n_splits should be either None or an integer value')
            if isinstance(n_splits, int) and n_splits <= 0:
                raise ValueError('n_splits should be a positive integer > 0')

        # Check max_tasks_active parameter
        if isinstance(max_tasks_active, int):
            if max_tasks_active <= 0:
                raise ValueError('max_tasks_active should be a positive integer, None or "n_jobs*2')
        elif max_tasks_active is not None and max_tasks_active != 'n_jobs*2':
            raise TypeError('max_tasks_active should be a positive integer, None or "n_jobs*2"')

        # If worker lifespan is not None or not a positive integer, raise
        if isinstance(worker_lifespan, int):
            if worker_lifespan <= 0:
                raise ValueError('worker_lifespan should be either None or a positive integer (> 0)')
        elif worker_lifespan is not None:
            raise TypeError('worker_lifespan should be either None or a positive integer (> 0)')

        # Progress bar position should be a positive integer
        if not isinstance(progress_bar_position, int):
            raise TypeError('progress_bar_position should be a positive integer (>= 0)')
        if progress_bar_position < 0:
            raise ValueError('progress_bar_position should be a positive integer (>= 0)')

        # Create queue for the progress bar
        self._task_completed_queue = self.ctx.JoinableQueue() if progress_bar else None

        return n_tasks, chunk_size, progress_bar
