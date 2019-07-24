import collections
import itertools
import os
import pickle
import queue
import signal
import subprocess
import time
import traceback
import warnings
from functools import partial
from threading import Thread
from typing import Any, Callable, Iterable, List, Optional, Tuple, Union

import numpy as np
from tqdm.auto import tqdm

from mpire.exception import CannotPickleExceptionError, ExceptionHandler, StopWorker
from mpire.progress_bar import ProgressBarHandler
from mpire.signal import DisableSignal, DelayedKeyboardInterrupt
from mpire.utils import chunk_tasks, apply_numpy_chunking

# If multiprocess is installed we want to use that as it has more capabilities than regular multiprocessing (e.g.,
# pickling lambdas en functions located in __main__)
try:
    import multiprocess as mp
except ImportError:
    import multiprocessing as mp


# Typedefs
CPUList = Optional[List[Union[int, List[int]]]]


MP_CONTEXTS = {'fork': mp.get_context('fork'),
               'forkserver': mp.get_context('forkserver'),
               'spawn': mp.get_context('spawn')}


class AbstractWorker:
    """
    A multiprocessing helper class which continuously asks the queue for new jobs, until a poison pill is inserted
    """

    def __init__(self, start_method: str, worker_id: int, tasks_queue: mp.JoinableQueue,
                 results_queue: mp.JoinableQueue, worker_done_array: mp.Array, task_completed_queue: mp.JoinableQueue,
                 exception_queue: mp.JoinableQueue, exception_lock: mp.Lock, exception_thrown: mp.Event,
                 func_pointer: Callable, keep_order: bool, shared_objects: Any = None,
                 worker_lifespan: Optional[int] = None, pass_worker_id: bool = False, use_worker_state: bool = False):
        """
        :param start_method: What Process start method to use
        :param worker_id: Worker ID
        :param tasks_queue: Queue object for retrieving new task arguments
        :param results_queue: Queue object for storing the results
        :param worker_done_array: Array object for notifying the ``WorkerPool`` to restart a worker
        :param task_completed_queue: Queue object for notifying the main process we're done with a single task. ``None``
            when notifying is not necessary.
        :param exception_queue: Queue object for sending Exception objects to whenever an Exception was raised inside a
            user function
        :param exception_lock: Lock object such that child processes can only throw one at a time.
        :param func_pointer: Function pointer to call each time new task arguments become available
        :param keep_order: Boolean flag which signals if the task arguments contain an order index which should be
            preserved and not fed to the function pointer (e.g., used in ``map``)
        :param shared_objects: ``None`` or an iterable of process-aware shared objects (e.g., ``multiprocessing.Array``)
            to pass to the function as the first argument.
        :param worker_lifespan: Number of chunks a worker can handle before it is restarted. If ``None``, workers will
            stay alive the entire time. Use this when workers use up too much memory over the course of time.
        :param pass_worker_id: Whether or not to pass the worker ID to the function
        :param use_worker_state: Whether to let a worker have a worker state or not
        """
        super().__init__()

        # Parameters
        self.start_method = start_method
        self.worker_id = worker_id
        self.tasks_queue = tasks_queue
        self.results_queue = results_queue
        self.worker_done_array = worker_done_array
        self.task_completed_queue = task_completed_queue
        self.exception_queue = exception_queue
        self.exception_lock = exception_lock
        self.exception_thrown = exception_thrown
        self.func_pointer = func_pointer
        self.keep_order = keep_order
        self.shared_objects = shared_objects
        self.worker_lifespan = worker_lifespan
        self.pass_worker_id = pass_worker_id
        self.use_worker_state = use_worker_state

        # Worker state
        self.worker_state = {}

        # Exception handling variables
        ctx = MP_CONTEXTS[self.start_method]
        self.is_running = False
        self.is_running_lock = ctx.Lock()

        # Register handler for graceful shutdown
        signal.signal(signal.SIGUSR1, self._exit_gracefully)

    def _exit_gracefully(self, *_) -> None:
        """
        This function is called when the main process sends a kill signal to this process. This can only mean another
        child process encountered an exception which means we should exit.

        We do this by setting the stop event and, if the process is in the middle of running the user defined function,
        by raising a StopWorker exception (which is then caught by the ``run()`` function) so we can quit gracefully.
        """
        # A rather complex locking mechanism is used here so we can make sure we only raise an exception when we should.
        # We want to make sure we only raise an exception when the user function is called. If it is running the
        # exception is caught and `run` will return. If it is running and the user function throws another exception
        # first, it depends on who obtains the lock first. If `run` obtains it it will set `running` to False, meaning
        # we won't raise and `run` will return. If this function obtains it first it will throw, which again is caught
        # by the `run` function, which will return.
        with self.is_running_lock:
            if self.is_running:
                self.is_running = False
                raise StopWorker

    def run(self) -> None:
        """
        Continuously asks the tasks queue for new task arguments. When not receiving a poisonous pill or when the max
        life span is not yet reached it will execute the new task and put the results in the results queue.
        """
        # Obtain additional args to pass to the function
        additional_args = []
        if self.pass_worker_id:
            additional_args.append(self.worker_id)
        if self.shared_objects is not None:
            additional_args.append(self.shared_objects)
        if self.use_worker_state:
            additional_args.append(self.worker_state)

        # Determine what function to call. If we have to keep in mind the order (for map) we use the helper function
        # with idx support which deals with the provided idx variable.
        func = partial(self._helper_func_with_idx if self.keep_order else self._helper_func,
                       partial(self.func_pointer, *additional_args))

        n_chunks_executed = 0
        while self.worker_lifespan is None or n_chunks_executed < self.worker_lifespan:

            # Obtain new chunk of jobs
            next_chunked_args = self._retrieve_task()

            # If we obtained a poison pill, we stop. When we receive None this means we stop because of an exception
            if next_chunked_args is '\0' or next_chunked_args is None:
                if next_chunked_args is '\0':
                    self.tasks_queue.task_done()
                return

            # Execute jobs in this chunk
            results = []
            for args in next_chunked_args:
                try:

                    try:
                        # Try to run this function and save results
                        with self.is_running_lock:
                            self.is_running = True
                        results.append(func(args))
                        with self.is_running_lock:
                            self.is_running = False

                    except StopWorker:
                        raise

                    except Exception as err:
                        # Let the signal handler know it shouldn't raise any exception anymore, we got this.
                        with self.is_running_lock:
                            self.is_running = False

                        # Pass exception to parent process and stop
                        self._raise(args, err)
                        raise StopWorker

                except StopWorker:
                    # The main process tells us to stop working, shutting down
                    self.tasks_queue.task_done()
                    return

                # Notify that we've completed a task (only when using a progress bar)
                if self.task_completed_queue is not None:
                    self.task_completed_queue.put(1)

            # Send results back to main process
            self.results_queue.put(results)
            self.tasks_queue.task_done()
            n_chunks_executed += 1

        # Notify WorkerPool to start a new worker if max lifespan is reached
        if self.worker_lifespan is not None and n_chunks_executed == self.worker_lifespan:
            self.worker_done_array[self.worker_id] = True

    def _retrieve_task(self) -> Any:
        """
        Obtain new chunk of jobs and occasionally poll the stop event

        :return: chunked args
        """
        while not self.exception_thrown.is_set():
            try:
                return self.tasks_queue.get(block=True, timeout=0.1)
            except queue.Empty:
                pass

        return None

    def _raise(self, args: Any, err: Exception) -> None:
        """
        Create exception and pass it to the parent process. Let other processes know an exception is set
        """
        # Only one process can throw at a time
        with self.exception_lock:

            # Only raise an exception when this process is the first one to raise. We do this because when the first
            # exception is caught by the main process the workers are joined which can cause a deadlock on draining the
            # exception queue. By only allowing one process to throw we know for sure that the exception queue will be
            # empty when the first one arrives.
            if not self.exception_thrown.is_set():

                # Determine function arguments
                func_args = args[1] if self.keep_order else args
                if isinstance(func_args, dict):
                    argument_str = "\n".join("Arg %s: %s" % (str(key), str(value)) for key, value in func_args.items())
                elif isinstance(func_args, collections.Iterable) and not isinstance(func_args, (str, bytes)):
                    argument_str = "\n".join("Arg %d: %s" % (arg_nr, str(arg)) for arg_nr, arg in enumerate(func_args))
                else:
                    argument_str = "Arg 0: %s" % func_args

                # Create traceback string
                traceback_str = "\n\nException occurred in Worker-%d with the following arguments:\n%s\n%s" % (
                    self.worker_id, argument_str, traceback.format_exc()
                )

                # Sometimes an exception cannot be pickled (i.e., we get the _pickle.PickleError: Can't pickle
                # <class ...>: it's not the same object as ...). We check that here by trying the pickle.dumps manually.
                # The call to `queue.put` creates a thread in which it pickles and when that raises an exception we
                # cannot catch it.
                try:
                    pickle.dumps(type(err))
                except pickle.PicklingError:
                    err = CannotPickleExceptionError()

                # Add exception to queue
                self.exception_queue.put((type(err), traceback_str))
                self.exception_thrown.set()

    def _helper_func_with_idx(self, func, args):
        """ Helper function which calls the function pointer but preserves the order index """
        return args[0], self._call_func(func, args[1])

    def _helper_func(self, func, args):
        """ Helper function which calls the function pointer """
        return self._call_func(func, args)

    @staticmethod
    def _call_func(func, args):
        """ Helper function which calls the function pointer and passes the arguments in the correct way """
        if isinstance(args, dict):
            return func(**args)
        elif isinstance(args, collections.Iterable) and not isinstance(args, (str, bytes, np.ndarray)):
            return func(*args)
        else:
            return func(args)


class ForkWorker(AbstractWorker, MP_CONTEXTS['fork'].Process):
    pass


class ForkServerWorker(AbstractWorker, MP_CONTEXTS['forkserver'].Process):
    pass


class SpawnWorker(AbstractWorker, MP_CONTEXTS['spawn'].Process):
    pass


def worker_factory(start_method):
    """
    Returns the appropriate worker class given the start method

    :param start_method: What Process start method to use, see the WorkerPool constructor
    :return: Worker class where the start_method is already filled in
    """
    if start_method == 'fork':
        return partial(ForkWorker, start_method)
    elif start_method == 'forkserver':
        return partial(ForkServerWorker, start_method)
    elif start_method == 'spawn':
        return partial(SpawnWorker, start_method)
    else:
        raise ValueError("Unknown start method: '{}'".format(start_method))


class WorkerPool:
    """
    A multiprocessing worker pool which acts like a ``multiprocessing.Pool``, but is faster and has more options.
    """

    def __init__(self, n_jobs: Optional[int] = None, daemon: bool = True, cpu_ids: CPUList = None,
                 shared_objects: Any = None, pass_worker_id: bool = False, use_worker_state: bool = False,
                 start_method: str = 'fork') -> None:
        """
        :param n_jobs: Number of workers to spawn. If ``None``, will use ``cpu_count()``.
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
        :param start_method: What Process start method to use, see
            https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods for more information and
            https://docs.python.org/3/library/multiprocessing.html#the-spawn-and-forkserver-start-methods for some
            caveats when using the 'spawn' or 'forkserver' methods
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

        # Boolean flag to inform the child processes to keep order in mind (for the map functions)
        self._keep_order = False

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

        # Whether or not an exception was caught by one of the child processes
        self.exception_caught = mp.Event()

    def _check_cpu_ids(self, cpu_ids: CPUList) -> List[str]:
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
                    converted_cpu_ids.append(','.join(map(str, cpu_id)))
                    max_cpu_id = max(max_cpu_id, max(cpu for cpu in cpu_id))
                    min_cpu_id = min(min_cpu_id, min(cpu for cpu in cpu_id))
                elif isinstance(cpu_id, int):
                    converted_cpu_ids.append(str(cpu_id))
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

    def start_workers(self, func_pointer: Callable, worker_lifespan: Optional[int]) -> None:
        """
        Spawns the workers and starts them so they're ready to start reading from the tasks queue.

        :param func_pointer: Function pointer to call each time new task arguments become available
        :param worker_lifespan: Number of chunks a worker can handle before it is restarted. If ``None``, workers will
            stay alive the entire time. Use this when workers use up too much memory over the course of time.
        """
        # Save params for later reference (for example, when restarting workers)
        self.func_pointer = func_pointer
        self.worker_lifespan = worker_lifespan

        # Start new workers
        self._tasks_queue = self.ctx.JoinableQueue()
        self._results_queue = self.ctx.JoinableQueue()
        self._worker_done_array = self.ctx.Array('b', self.n_jobs, lock=False)
        self._exception_queue = self.ctx.JoinableQueue()
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
        with DisableSignal():
            # Create worker
            w = self.Worker(worker_id, self._tasks_queue, self._results_queue, self._worker_done_array,
                            self._task_completed_queue, self._exception_queue, self._exception_lock,
                            self._exception_thrown, self.func_pointer, self._keep_order, self.shared_objects,
                            self.worker_lifespan, self.pass_worker_id, self.use_worker_state)
            w.daemon = self.daemon
            w.start()

            # Pin CPU if desired
            if self.cpu_ids:
                subprocess.call('taskset -p -c %s %d' % (self.cpu_ids[worker_id], w.pid), stdout=subprocess.DEVNULL,
                                shell=True)

            return w

    def add_task(self, args: Any) -> None:
        """
        Add a task to the queue so a worker can process it.

        :param args: A tuple of arguments to pass to a worker, which acts upon it
        """
        with DelayedKeyboardInterrupt():
            self._tasks_queue.put(args, block=True)

    def get_result(self) -> Any:
        """
        Obtain the next result from the results queue.

        :return: The next result from the queue, which is the result of calling the function pointer.
        """
        with DelayedKeyboardInterrupt():
            results = self._results_queue.get(block=True)
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
            t = Thread(target=self._handle_tasks_done)
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
            t = Thread(target=self._terminate_worker, args=(w,))
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
            progress_bar: Union[bool, tqdm] = False, concatenate_numpy_output: bool = True) -> Any:
        """
        Same as ``multiprocessing.map()``. Also allows a user to set the maximum number of tasks available in the queue.
        Note that this function can be slower than the unordered version.

        :param func_pointer: Function pointer to call each time new task arguments become available. When passing on the
            worker ID the function should receive the worker ID as its first argument. If shared objects are provided
            the function should receive those as the next argument. If the worker state has been enabled it should
            receive a state variable as the next argument.
        :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker, which
            passes it to the function pointer
        :param iterable_len: Int or ``None``. When chunk_size is set to ``None`` it needs to know the number of tasks.
            This  can either be provided by implementing the ``__len__`` function on the iterable object, or by
            specifying the number of tasks.
        :param max_tasks_active: Int or ``None``. Maximum number of active tasks in the queue. Use ``None`` to not limit
            the queue
        :param chunk_size: Int or ``None``. Number of simultaneous tasks to give to a worker. If ``None``, will generate
            ``n_jobs * 4`` number of chunks.
        :param n_splits: Int or ``None``. Number of splits to use when ``chunk_size`` is ``None``.
        :param worker_lifespan: Int or ``None``. Number of chunks a worker can handle before it is restarted. If
            ``None``, workers will stay alive the entire time. Use this when workers use up too much memory over the
            course of time.
        :param progress_bar: Boolean or ``tqdm`` instance. When ``True`` will display a default progress bar. Defaults
            can be overridden by supplying a custom ``tqdm`` progress bar instance. Use ``False`` to disable.
        :param concatenate_numpy_output: Boolean. When ``True`` it will concatenate numpy output to a single numpy array
        :return: List with ordered results
        """
        # Notify workers to keep order in mind
        self._keep_order = True

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
                                     progress_bar)

        # Notify workers to forget about order
        self._keep_order = False

        # Rearrange and return
        sorted_results = [result[1] for result in sorted(results, key=lambda result: result[0])]

        # Convert back to numpy if necessary
        return (np.concatenate(sorted_results)
                if sorted_results and concatenate_numpy_output and isinstance(sorted_results[0], np.ndarray) else
                sorted_results)

    def map_unordered(self, func_pointer: Callable, iterable_of_args: Union[Iterable, np.ndarray],
                      iterable_len: Optional[int] = None, max_tasks_active: Optional[int] = None,
                      chunk_size: Optional[int] = None, n_splits: Optional[int] = None,
                      worker_lifespan: Optional[int] = None, progress_bar: Union[bool, tqdm] = False) -> Any:
        """
        Same as ``multiprocessing.map()``, but unordered. Also allows a user to set the maximum number of tasks
        available in the queue.

        :param func_pointer: Function pointer to call each time new task arguments become available. When passing on the
            worker ID the function should receive the worker ID as its first argument. If shared objects are provided
            the function should receive those as the next argument. If the worker state has been enabled it should
            receive a state variable as the next argument.
        :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker, which
            passes it to the function pointer
        :param iterable_len: Int or ``None``. When chunk_size is set to ``None`` it needs to know the number of tasks.
            This  can either be provided by implementing the ``__len__`` function on the iterable object, or by
            specifying the number of tasks.
        :param max_tasks_active: Int or ``None``. Maximum number of active tasks in the queue. Use ``None`` to not limit
            the queue
        :param chunk_size: Int or ``None``. Number of simultaneous tasks to give to a worker. If ``None``, will generate
            ``n_jobs * 4`` number of chunks.
        :param n_splits: Int or ``None``. Number of splits to use when ``chunk_size`` is ``None``.
        :param worker_lifespan: Int or ``None``. Number of chunks a worker can handle before it is restarted. If
            ``None``, workers will stay alive the entire time. Use this when workers use up too much memory over the
            course of time.
        :param progress_bar: Boolean or ``tqdm`` instance. When ``True`` will display a default progress bar. Defaults
            can be overridden by supplying a custom ``tqdm`` progress bar instance. Use ``False`` to disable.
        :return: List with unordered results
        """
        # Simply call imap and cast it to a list. This make sure all elements are there before returning
        return list(self.imap_unordered(func_pointer, iterable_of_args, iterable_len, max_tasks_active, chunk_size,
                                        n_splits, worker_lifespan, progress_bar))

    def imap(self, func_pointer: Callable, iterable_of_args: Union[Iterable, np.ndarray],
             iterable_len: Optional[int] = None, max_tasks_active: Optional[int] = None,
             chunk_size: Optional[int] = None, n_splits: Optional[int] = None,
             worker_lifespan: Optional[int] = None, progress_bar: Union[bool, tqdm] = False) -> Any:
        """
        Same as ``multiprocessing.imap_unordered()``, but ordered. Also allows a user to set the maximum number of
        tasks available in the queue.

        :param func_pointer: Function pointer to call each time new task arguments become available. When passing on the
            worker ID the function should receive the worker ID as its first argument. If shared objects are provided
            the function should receive those as the next argument. If the worker state has been enabled it should
            receive a state variable as the next argument.
        :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker, which
            passes it to the function pointer
        :param iterable_len: Int or ``None``. When chunk_size is set to ``None`` it needs to know the number of tasks.
            This  can either be provided by implementing the ``__len__`` function on the iterable object, or by
            specifying the number of tasks.
        :param max_tasks_active: Int or ``None``. Maximum number of active tasks in the queue. Use ``None`` to not limit
            the queue
        :param chunk_size: Int or ``None``. Number of simultaneous tasks to give to a worker. If ``None``, will generate
            ``n_jobs * 4`` number of chunks.
        :param n_splits: Int or ``None``. Number of splits to use when ``chunk_size`` is ``None``.
        :param worker_lifespan: Int or ``None``. Number of chunks a worker can handle before it is restarted. If
            ``None``, workers will stay alive the entire time. Use this when workers use up too much memory over the
            course of time.
        :param progress_bar: Boolean or ``tqdm`` instance. When ``True`` will display a default progress bar. Defaults
            can be overridden by supplying a custom ``tqdm`` progress bar instance. Use ``False`` to disable.
        :return: Generator yielding ordered results
        """
        # Notify workers to keep order in mind
        self._keep_order = True

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
                                                      chunk_size, n_splits, worker_lifespan, progress_bar):

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
        self._keep_order = False

    def imap_unordered(self, func_pointer: Callable, iterable_of_args: Union[Iterable, np.ndarray],
                       iterable_len: Optional[int] = None, max_tasks_active: Optional[int] = None,
                       chunk_size: Optional[int] = None, n_splits: Optional[int] = None,
                       worker_lifespan: Optional[int] = None, progress_bar: Union[bool, tqdm] = False) -> Any:
        """
        Same as ``multiprocessing.imap_unordered()``. Also allows a user to set the maximum number of tasks available in
        the queue.

        :param func_pointer: Function pointer to call each time new task arguments become available. When passing on the
            worker ID the function should receive the worker ID as its first argument. If shared objects are provided
            the function should receive those as the next argument. If the worker state has been enabled it should
            receive a state variable as the next argument.
        :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker, which
            passes it to the function pointer
        :param iterable_len: Int or ``None``. When chunk_size is set to ``None`` it needs to know the number of tasks.
            This  can either be provided by implementing the ``__len__`` function on the iterable object, or by
            specifying the number of tasks.
        :param max_tasks_active: Int or ``None``. Maximum number of active tasks in the queue. Use ``None`` to not limit
            the queue
        :param chunk_size: Int or ``None``. Number of simultaneous tasks to give to a worker. If ``None``, will generate
            ``n_jobs * 4`` number of chunks.
        :param n_splits: Int or ``None``. Number of splits to use when ``chunk_size`` is ``None``.
        :param worker_lifespan: Int or ``None``. Number of chunks a worker can handle before it is restarted. If
            ``None``, workers will stay alive the entire time. Use this when workers use up too much memory over the
            course of time.
        :param progress_bar: Boolean or ``tqdm`` instance. When ``True`` will display a default progress bar. Defaults
            can be overridden by supplying a custom ``tqdm`` progress bar instance. Use ``False`` to disable.
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
                                                                       progress_bar)

        # Chunk the function arguments. Make single arguments when we're dealing with numpy arrays
        if not isinstance(iterable_of_args, np.ndarray):
            iterator_of_chunked_args = chunk_tasks(iterable_of_args, n_tasks, chunk_size, n_splits or self.n_jobs * 4)

        # Start workers
        self.start_workers(func_pointer, worker_lifespan)

        # Create exception and progress bar handlers. The exception handler will receive any exceptions thrown by the
        # workers, terminates everything and re-raise an exceptoin in the main process. The progress bar handler will
        # receive updates from the workers and updates the progress bar accordingly
        with ExceptionHandler(self.terminate, self._exception_queue, self.exception_caught,
                              progress_bar is not None) as exception_handler, \
             ProgressBarHandler(func_pointer, progress_bar, self._task_completed_queue, self._exception_queue,
                                self.exception_caught):

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
                        with DelayedKeyboardInterrupt():
                            results = self._results_queue.get(block=False)
                            self._results_queue.task_done()
                        yield from results
                        n_active -= 1
                    except queue.Empty:
                        pass

                    # Restart workers if necessary
                    self._restart_workers()

            # Obtain the results not yet obtained
            while not self.exception_caught.is_set() and n_active != 0:
                try:
                    with DelayedKeyboardInterrupt():
                        results = self._results_queue.get(block=True, timeout=0.1)
                        self._results_queue.task_done()
                    yield from results
                    n_active -= 1
                except queue.Empty:
                    pass

                # Restart workers if necessary
                self._restart_workers()

            # Clean up time
            exception_handler.raise_on_exception()
            self.stop_and_join()

    def _check_map_parameters(self, iterable_of_args: Union[Iterable, np.ndarray], iterable_len: Optional[int],
                              max_tasks_active: Optional[int], chunk_size: Optional[int], n_splits: Optional[int],
                              worker_lifespan: Optional[int], progress_bar: Union[bool, tqdm]) \
            -> Tuple[Optional[int], Optional[int], Union[bool, tqdm]]:
        """
        Check the parameters provided to any (i)map function. Also extracts the number of tasks and can modify the
        ``chunk_size`` and ``progress_bar`` parameters.

        :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker, which
            passes it to the function pointer
        :param iterable_len: Int or ``None``. When chunk_size is set to ``None`` it needs to know the number of tasks.
            This  can either be provided by implementing the ``__len__`` function on the iterable object, or by
            specifying the number of tasks.
        :param max_tasks_active: Int or ``None``. Maximum number of active tasks in the queue. Use ``None`` to not limit
            the queue
        :param chunk_size: Int or ``None``. Number of simultaneous tasks to give to a worker. If ``None``, will generate
            ``n_jobs * 4`` number of chunks.
        :param n_splits: Int or ``None``. Number of splits to use when ``chunk_size`` is ``None``.
        :param worker_lifespan: Int or ``None``. Number of chunks a worker can handle before it is restarted. If
            ``None``, workers will stay alive the entire time. Use this when workers use up too much memory over the
            course of time.
        :param progress_bar: Boolean or ``tqdm`` instance. When ``True`` will display a default progress bar. Defaults
            can be overridden by supplying a custom ``tqdm`` progress bar instance. Use ``False`` to disable.
        :return: Number of tasks, chunk size, progress bar
        """
        # Get number of tasks
        n_tasks = None
        if iterable_len is not None:
            n_tasks = iterable_len
        elif hasattr(iterable_of_args, '__len__'):
            n_tasks = len(iterable_of_args)
        elif isinstance(progress_bar, tqdm) and progress_bar.total is not None:
            n_tasks = progress_bar.total
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
        elif max_tasks_active is not None and max_tasks_active is not 'n_jobs*2':
            raise TypeError('max_tasks_active should be a positive integer, None or "n_jobs*2"')

        # If worker lifespan is not None or not a positive integer, raise
        if isinstance(worker_lifespan, int):
            if worker_lifespan <= 0:
                raise ValueError('worker_lifespan should be either None or a positive integer (> 0)')
        elif worker_lifespan is not None:
            raise TypeError('worker_lifespan should be either None or a positive integer (> 0)')

        # Parameter total should be given
        if isinstance(progress_bar, tqdm) and progress_bar.total is None:
            raise ValueError("Custom tqdm progress bar instance needs parameter 'total'")

        # Create progress bar
        if progress_bar is True or isinstance(progress_bar, tqdm):
            progress_bar = progress_bar if isinstance(progress_bar, tqdm) else tqdm(total=n_tasks)
            self._task_completed_queue = self.ctx.JoinableQueue()
        else:
            progress_bar = None
            self._task_completed_queue = None

        return n_tasks, chunk_size, progress_bar


MotherForker = WorkerPool
