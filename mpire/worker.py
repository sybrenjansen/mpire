import collections
import queue
import signal
import traceback
from datetime import datetime
from functools import partial
from multiprocessing.managers import ListProxy
from typing import Any, Callable, Optional, Tuple, List

import numpy as np

from mpire.context import MP_CONTEXTS
from mpire.exception import CannotPickleExceptionError, StopWorker
from mpire.utils import TimeIt

# If multiprocess is installed we want to use that as it has more capabilities than regular multiprocessing (e.g.,
# pickling lambdas en functions located in __main__)
try:
    import multiprocess as mp
    import dill as pickle
except ImportError:
    import multiprocessing as mp
    import pickle


class AbstractWorker:
    """
    A multiprocessing helper class which continuously asks the queue for new jobs, until a poison pill is inserted
    """

    def __init__(self, start_method: str, worker_id: int, tasks_queue: mp.JoinableQueue,
                 results_queue: mp.JoinableQueue, exit_results_queue: Optional[mp.JoinableQueue],
                 worker_done_array: mp.Array, task_completed_queue: mp.JoinableQueue, exception_queue: mp.JoinableQueue,
                 exception_lock: mp.Lock, exception_thrown: mp.Event, func: Callable, worker_init: Optional[Callable],
                 worker_exit: Optional[Callable], keep_order: bool, start_time: datetime,
                 worker_start_up_time: mp.Array, worker_init_time: mp.Array, worker_n_completed_tasks: mp.Array,
                 worker_waiting_time: mp.Array, worker_working_time: mp.Array, worker_exit_time: mp.Array,
                 max_task_duration: mp.Array, max_task_args: ListProxy, shared_objects: Any = None,
                 worker_lifespan: Optional[int] = None, pass_worker_id: bool = False,
                 use_worker_state: bool = False) -> None:
        """
        :param start_method: What Process start method to use
        :param worker_id: Worker ID
        :param tasks_queue: Queue object for retrieving new task arguments
        :param results_queue: Queue object for storing the results
        :param worker_done_array: Array object for notifying the ``WorkerPool`` to restart a worker
        :param task_completed_queue: Queue object for notifying the main process we're done with a single task. ``None``
            when notifying is not necessary
        :param exception_queue: Queue object for sending Exception objects to whenever an Exception was raised inside a
            user function
        :param exception_lock: Lock object such that child processes can only throw one at a time
        :param func: Function to call each time new task arguments become available
        :param keep_order: Boolean flag which signals if the task arguments contain an order index which should be
            preserved and not fed to the function `func` (e.g., used in ``map``)
        :param start_time: `datetime` object indicating at what time the Worker instance was created and started
        :param worker_start_up_time: Array object which holds the total number of seconds the workers take to start up
        :param worker_init_time: Array object which holds the total number of seconds the workers take to run the init
            function
        :param worker_n_completed_tasks: Array object which holds the total number of completed tasks per worker
        :param worker_waiting_time: Array object which holds the total number of seconds the workers have been idle
        :param worker_working_time: Array object which holds the total number of seconds the workers are executing the
            task function
        :param worker_exit_time: Array object which holds the total number of seconds the workers take to run the exit
            function
        :param max_task_duration: Array object which holds the top 5 max task durations in seconds per worker
        :param max_task_args: Array object which holds the top 5 task arguments (string) for the longest task per worker
        :param shared_objects: ``None`` or an iterable of process-aware shared objects (e.g., ``multiprocessing.Array``)
            to pass to the function as the first argument
        :param worker_lifespan: Number of chunks a worker can handle before it is restarted. If ``None``, workers will
            stay alive the entire time. Use this when workers use up too much memory over the course of time
        :param pass_worker_id: Whether or not to pass the worker ID to the function
        :param use_worker_state: Whether to let a worker have a worker state or not
        """
        super().__init__()

        # Parameters
        self.start_method = start_method
        self.worker_id = worker_id
        self.tasks_queue = tasks_queue
        self.results_queue = results_queue
        self.exit_results_queue = exit_results_queue
        self.worker_done_array = worker_done_array
        self.task_completed_queue = task_completed_queue
        self.exception_queue = exception_queue
        self.exception_lock = exception_lock
        self.exception_thrown = exception_thrown
        self.func = func
        self.worker_init = worker_init
        self.worker_exit = worker_exit
        self.keep_order = keep_order
        self.start_time = start_time
        self.worker_start_up_time = worker_start_up_time
        self.worker_init_time = worker_init_time
        self.worker_n_completed_tasks = worker_n_completed_tasks
        self.worker_waiting_time = worker_waiting_time
        self.worker_working_time = worker_working_time
        self.worker_exit_time = worker_exit_time
        self.max_task_duration = max_task_duration
        self.max_task_args = max_task_args
        self.shared_objects = shared_objects
        self.worker_lifespan = worker_lifespan
        self.pass_worker_id = pass_worker_id
        self.use_worker_state = use_worker_state

        # Worker state
        self.worker_state = {}

        # Additional worker insights container that holds (task duration, task args) tuples, sorted for heapq. We use a
        # local container as to not put too big of a burden on interprocess communication
        self.max_task_duration_list = (list(zip(self.max_task_duration[self.worker_id * 5:(self.worker_id + 1) * 5],
                                                self.max_task_args[self.worker_id * 5:(self.worker_id + 1) * 5]))
                                       if self.max_task_duration is not None else None)
        self.max_task_duration_last_updated = datetime(1970, 1, 1)

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

        If this process is in the middle of running the user defined function we raise a StopWorker exception (which is
        then caught by the ``_run_safely()`` function) so we can quit gracefully.
        """
        # A rather complex locking and exception mechanism is used here so we can make sure we only raise an exception
        # when we should. We want to make sure we only raise an exception when the user function is called. If it is
        # running the exception is caught and `run` will return. If it is running and the user function throws another
        # exception first, it depends on who obtains the lock first. If `run` obtains it, it will set `running` to
        # False, meaning we won't raise and `run` will return. If this function obtains it first it will throw, which
        # again is caught by the `run` function, which will return.
        with self.is_running_lock:
            if self.is_running:
                self.is_running = False
                raise StopWorker

    def run(self) -> None:
        """
        Continuously asks the tasks queue for new task arguments. When not receiving a poisonous pill or when the max
        life span is not yet reached it will execute the new task and put the results in the results queue.
        """
        # Store how long it took to start up
        if self.worker_start_up_time is not None:
            self.worker_start_up_time[self.worker_id] = (datetime.now() - self.start_time).total_seconds()

        # Obtain additional args to pass to the function
        additional_args = []
        if self.pass_worker_id:
            additional_args.append(self.worker_id)
        if self.shared_objects is not None:
            additional_args.append(self.shared_objects)
        if self.use_worker_state:
            additional_args.append(self.worker_state)

        # Run initialization function. If it returns True it means an exception occurred and we should exit
        if self.worker_init and self._run_init_func(additional_args):
            return

        # Determine what function to call. If we have to keep in mind the order (for map) we use the helper function
        # with idx support which deals with the provided idx variable.
        func = partial(self._helper_func_with_idx if self.keep_order else self._helper_func,
                       partial(self.func, *additional_args))

        n_chunks_executed = 0
        while self.worker_lifespan is None or n_chunks_executed < self.worker_lifespan:

            # Obtain new chunk of jobs
            with TimeIt(self.worker_waiting_time, self.worker_id):
                next_chunked_args = self._retrieve_task()

            # If we obtained a poison pill, we stop. When the _retrieve_task function returns None this means we stop
            # because of an exception in the main process
            if next_chunked_args == '\0' or next_chunked_args is None:
                self._update_task_insights()
                if next_chunked_args == '\0':
                    self.tasks_queue.task_done()

                    # Run exit function when a poison pill was received
                    if self.worker_exit:
                        self._run_exit_func(additional_args)
                return

            # Execute jobs in this chunk
            results = []
            for args in next_chunked_args:

                # Try to run this function and save results
                results_part, should_return = self._run_func(func, args)
                if should_return:
                    self.tasks_queue.task_done()
                    return
                results.append(results_part)

                # Notify that we've completed a task (only when using a progress bar)
                if self.task_completed_queue is not None:
                    self.task_completed_queue.put(1)

            # Send results back to main process
            self.results_queue.put(results)
            self.tasks_queue.task_done()
            n_chunks_executed += 1

            # Update task insights every once in a while (every 2 seconds)
            if (self.max_task_duration is not None and
                    (datetime.now() - self.max_task_duration_last_updated).total_seconds() > 2):
                self._update_task_insights()

        # Run exit function and store results
        if self.worker_exit and self._run_exit_func(additional_args):
            return

        # Notify WorkerPool to start a new worker if max lifespan is reached
        self._update_task_insights()
        if self.worker_lifespan is not None and n_chunks_executed == self.worker_lifespan:
            self.worker_done_array[self.worker_id] = True

    def _retrieve_task(self) -> Any:
        """
        Obtain new chunk of jobs and occasionally poll the stop event

        :return: Chunked args
        """
        while not self.exception_thrown.is_set():
            try:
                return self.tasks_queue.get(block=True, timeout=0.1)
            except queue.Empty:
                pass

        return None

    def _run_init_func(self, additional_args: List) -> bool:
        """
        Runs the init function when provided.

        :param additional_args: Additional args to pass to the function (worker ID, shared objects, worker state)
        :return: True when the worker needs to shut down, False otherwise
        """
        def _init_func():
            with TimeIt(self.worker_init_time, self.worker_id):
                self.worker_init(*additional_args)

        return self._run_safely(_init_func, no_args=True)[1]

    def _run_func(self, func: Callable, args: List) -> Tuple[Any, bool]:
        """
        Runs the main function when provided.

        :param func: Function to call
        :param args: Args to pass to the function
        :return: Tuple containing results from the function and a boolean value indicating whether the worker needs to
            shut down
        """
        def _func():
            with TimeIt(self.worker_working_time, self.worker_id, self.max_task_duration_list,
                        lambda: self._format_args(args, separator=' | ')):
                results = func(args)
            if self.worker_n_completed_tasks is not None:
                self.worker_n_completed_tasks[self.worker_id] += 1
            return results

        return self._run_safely(_func, args)

    def _run_exit_func(self, additional_args: List) -> bool:
        """
        Runs the exit function when provided and stores its results.

        :param additional_args: Additional args to pass to the function (worker ID, shared objects, worker state)
        :return: True when the worker needs to shut down, False otherwise
        """
        def _exit_func():
            with TimeIt(self.worker_exit_time, self.worker_id):
                return self.worker_exit(*additional_args)

        results, should_return = self._run_safely(_exit_func, no_args=True)

        if should_return:
            return True
        else:
            self.exit_results_queue.put(results)
            return False

    def _run_safely(self, func: Callable, exception_args: Optional[Any] = None,
                    no_args: bool = False) -> Tuple[Any, bool]:
        """
        A rather complex locking and exception mechanism is used here so we can make sure we only raise an exception
        when we should. See `_exit_gracefully` for more information.

        :param func: Function to run
        :param exception_args: Arguments to pass to `_format_args` when an exception occurred
        :param no_args: Whether there were any args at all
        :return: True when the worker needs to shut down, False otherwise
        """
        try:

            try:
                # Obtain lock and try to run the function. During this block a StopWorker exception from the parent
                # process can come through to signal we should stop
                with self.is_running_lock:
                    self.is_running = True
                results = func()
                with self.is_running_lock:
                    self.is_running = False

            except StopWorker:
                # The main process tells us to stop working, shutting down
                raise

            except Exception as err:
                # An exception occurred inside the provided function. Let the signal handler know it shouldn't raise any
                # StopWorker exceptions from the parent process anymore, we got this.
                with self.is_running_lock:
                    self.is_running = False

                # Pass exception to parent process and stop
                self._raise(exception_args, no_args, err)
                raise StopWorker

        except StopWorker:
            # Stop working
            return None, True

        # Carry on
        return results, False

    def _raise(self, args: Any, no_args: bool, err: Exception) -> None:
        """
        Create exception and pass it to the parent process. Let other processes know an exception is set

        :param args: Funtion arguments where exception was raised
        :param no_args: Whether there were any args at all
        :param err: Exception that should be passed on to parent process
        """
        # Only one process can throw at a time
        with self.exception_lock:

            # Only raise an exception when this process is the first one to raise. We do this because when the first
            # exception is caught by the main process the workers are joined which can cause a deadlock on draining the
            # exception queue. By only allowing one process to throw we know for sure that the exception queue will be
            # empty when the first one arrives.
            if not self.exception_thrown.is_set():

                # Create traceback string
                traceback_str = "\n\nException occurred in Worker-%d with the following arguments:\n%s\n%s" % (
                    self.worker_id, self._format_args(args, no_args), traceback.format_exc()
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

    def _format_args(self, args: Any, no_args: bool = False, separator: str = '\n') -> str:
        """
        Format the function arguments to a string form.

        :param args: Funtion arguments
        :param no_args: Whether there were any args at all. If not, then return special string
        :param separator: String to use as separator between arguments
        :return: String containing the task arguments
        """
        # Determine function arguments
        func_args = args[1] if args and self.keep_order else args
        if no_args:
            return "N/A"
        elif isinstance(func_args, dict):
            return separator.join("Arg %s: %s" % (str(key), repr(value)) for key, value in func_args.items())
        elif isinstance(func_args, collections.abc.Iterable) and not isinstance(func_args, (str, bytes)):
            return separator.join("Arg %d: %s" % (arg_nr, repr(arg)) for arg_nr, arg in enumerate(func_args))
        else:
            return "Arg 0: %s" % func_args

    def _helper_func_with_idx(self, func: Callable, args: Tuple[int, Any]) -> Tuple[int, Any]:
        """
        Helper function which calls the function `func` but preserves the order index

        :param func: Function to call each time new task arguments become available
        :param args: Tuple of ``(idx, _args)`` where ``_args`` correspond to the arguments to pass on to the function.
            ``idx`` is used to preserve order
        :return: (idx, result of calling the function with the given arguments) tuple
        """
        return args[0], self._call_func(func, args[1])

    def _helper_func(self, func: Callable, args: Any) -> Any:
        """
        Helper function which calls the function `func`

        :param func: Function to call each time new task arguments become available
        :param args: Arguments to pass on to the function
        :return: Result of calling the function with the given arguments) tuple
        """
        return self._call_func(func, args)

    @staticmethod
    def _call_func(func: Callable, args: Any) -> Any:
        """
        Helper function which calls the function `func` and passes the arguments in the correct way

        :param func: Function to call each time new task arguments become available
        :param args: Arguments to pass on to the function
        :return: Result of calling the function with the given arguments) tuple
        """
        if isinstance(args, dict):
            return func(**args)
        elif isinstance(args, collections.abc.Iterable) and not isinstance(args, (str, bytes, np.ndarray)):
            return func(*args)
        else:
            return func(args)

    def _update_task_insights(self) -> None:
        """
        Update synced containers with new top 5 max task duration + args
        """
        if self.max_task_duration is not None:
            task_durations, task_args = zip(*self.max_task_duration_list)
            self.max_task_duration[self.worker_id * 5:(self.worker_id + 1) * 5] = task_durations
            self.max_task_args[self.worker_id * 5:(self.worker_id + 1) * 5] = task_args
            self.max_task_duration_last_updated = datetime.now()


class ForkWorker(AbstractWorker, MP_CONTEXTS['fork'].Process):
    pass


class ForkServerWorker(AbstractWorker, MP_CONTEXTS['forkserver'].Process):
    pass


class SpawnWorker(AbstractWorker, MP_CONTEXTS['spawn'].Process):
    pass


class ThreadingWorker(AbstractWorker, MP_CONTEXTS['threading'].Thread):
    pass


def worker_factory(start_method):
    """
    Returns the appropriate worker class given the start method

    :param start_method: What Process/Threading start method to use, see the WorkerPool constructor
    :return: Worker class where the start_method is already filled in
    """
    if start_method == 'fork':
        return partial(ForkWorker, start_method)
    elif start_method == 'forkserver':
        return partial(ForkServerWorker, start_method)
    elif start_method == 'spawn':
        return partial(SpawnWorker, start_method)
    elif start_method == 'threading':
        return partial(ThreadingWorker, start_method)
    else:
        raise ValueError("Unknown start method: '{}'".format(start_method))
