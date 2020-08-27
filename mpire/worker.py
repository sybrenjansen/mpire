import collections
import queue
import signal
import traceback
from functools import partial
from typing import Any, Callable, Optional, Tuple

import numpy as np

from mpire.context import MP_CONTEXTS
from mpire.exception import CannotPickleExceptionError, StopWorker

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
                 results_queue: mp.JoinableQueue, worker_done_array: mp.Array, task_completed_queue: mp.JoinableQueue,
                 exception_queue: mp.JoinableQueue, exception_lock: mp.Lock, exception_thrown: mp.Event,
                 func_pointer: Callable, keep_order: bool, shared_objects: Any = None,
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
        :param func_pointer: Function pointer to call each time new task arguments become available
        :param keep_order: Boolean flag which signals if the task arguments contain an order index which should be
            preserved and not fed to the function pointer (e.g., used in ``map``)
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
            if next_chunked_args == '\0' or next_chunked_args is None:
                if next_chunked_args == '\0':
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

        :return: Chunked args
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

        :param args: Funtion arguments where exception was raised
        :param err: Exception that should be passed on to parent process
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
                elif isinstance(func_args, collections.abc.Iterable) and not isinstance(func_args, (str, bytes)):
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

    def _helper_func_with_idx(self, func: Callable, args: Tuple[int, Any]) -> Tuple[int, Any]:
        """
        Helper function which calls the function pointer but preserves the order index

        :param func: Function pointer to call each time new task arguments become available
        :param args: Tuple of ``(idx, _args)`` where ``_args`` correspond to the arguments to pass on to the function.
            ``idx`` is used to preserve order
        :return: (idx, result of calling the function with the given arguments) tuple
        """
        return args[0], self._call_func(func, args[1])

    def _helper_func(self, func: Callable, args: Any) -> Any:
        """
        Helper function which calls the function pointer

        :param func: Function pointer to call each time new task arguments become available
        :param args: Arguments to pass on to the function
        :return: Result of calling the function with the given arguments) tuple
        """
        return self._call_func(func, args)

    @staticmethod
    def _call_func(func: Callable, args: Any) -> Any:
        """
        Helper function which calls the function pointer and passes the arguments in the correct way

        :param func: Function pointer to call each time new task arguments become available
        :param args: Arguments to pass on to the function
        :return: Result of calling the function with the given arguments) tuple
        """
        if isinstance(args, dict):
            return func(**args)
        elif isinstance(args, collections.abc.Iterable) and not isinstance(args, (str, bytes, np.ndarray)):
            return func(*args)
        else:
            return func(args)


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
