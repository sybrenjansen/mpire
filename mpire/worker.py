import collections
import dill
import signal
import traceback
from datetime import datetime
from functools import partial
from typing import Any, Callable, List, Optional, Tuple

import numpy as np

from mpire.comms import WorkerComms, POISON_PILL
from mpire.context import MP_CONTEXTS
from mpire.exception import CannotPickleExceptionError, StopWorker
from mpire.insights import WorkerInsights
from mpire.params import WorkerPoolParams
from mpire.utils import TimeIt


class AbstractWorker:
    """
    A multiprocessing helper class which continuously asks the queue for new jobs, until a poison pill is inserted
    """

    def __init__(self, worker_id: int, params: WorkerPoolParams, worker_comms: WorkerComms,
                 worker_insights: WorkerInsights, start_time: datetime) -> None:
        """
        :param worker_id: Worker ID
        :param params: WorkerPool parameters
        :param worker_comms: Worker communication objects (queues, locks, events, ...)
        :param worker_insights: WorkerInsights object which stores the worker insights
        :param start_time: `datetime` object indicating at what time the Worker instance was created and started
        """
        super().__init__()

        # Parameters
        self.worker_id = worker_id
        self.params = params
        self.worker_comms = worker_comms
        self.worker_insights = worker_insights
        self.start_time = start_time

        # Worker state
        self.worker_state = {}

        # Initialize worker comms and insights for this worker
        self.worker_comms.init_worker(self.worker_id)
        self.worker_insights.init_worker(self.worker_id)

        # Exception handling variables
        ctx = MP_CONTEXTS[self.params.start_method]
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
        self.worker_comms.set_worker_alive()

        try:
            # Store how long it took to start up
            self.worker_insights.update_start_up_time(self.start_time)

            # Obtain additional args to pass to the function
            additional_args = []
            if self.params.pass_worker_id:
                additional_args.append(self.worker_id)
            if self.params.shared_objects is not None:
                additional_args.append(self.params.shared_objects)
            if self.params.use_worker_state:
                additional_args.append(self.worker_state)

            # Run initialization function. If it returns True it means an exception occurred and we should exit
            if self.params.worker_init and self._run_init_func(additional_args):
                return

            # Determine what function to call. If we have to keep in mind the order (for map) we use the helper function
            # with idx support which deals with the provided idx variable.
            func = partial(self._helper_func_with_idx if self.worker_comms.keep_order() else self._helper_func,
                           partial(self.params.func, *additional_args))

            n_tasks_executed = 0
            while self.params.worker_lifespan is None or n_tasks_executed < self.params.worker_lifespan:

                # Obtain new chunk of jobs
                with TimeIt(self.worker_insights.worker_waiting_time, self.worker_id):
                    next_chunked_args = self.worker_comms.get_task()

                # If we obtained a poison pill, we stop. When the _retrieve_task function returns None this means we
                # stop because of an exception in the main process
                if next_chunked_args == POISON_PILL or next_chunked_args is None:
                    self.worker_insights.update_task_insights()
                    if next_chunked_args == POISON_PILL:
                        self.worker_comms.task_done()

                        # Run exit function when a poison pill was received
                        if self.params.worker_exit:
                            self._run_exit_func(additional_args)
                    return

                # Execute jobs in this chunk
                try:
                    results = []
                    for args in next_chunked_args:

                        # Try to run this function and save results
                        results_part, should_return = self._run_func(func, args)
                        if should_return:
                            return
                        results.append(results_part)

                        # Notify that we've completed a task once in a while (only when using a progress bar)
                        if self.worker_comms.has_progress_bar():
                            self.worker_comms.task_completed_progress_bar()

                    # Send results back to main process
                    self.worker_comms.add_results(results)
                    n_tasks_executed += len(results)

                # In case an exception occurred and we need to return, we want to call task_done no matter what
                finally:
                    self.worker_comms.task_done()

                # Update task insights every once in a while (every 2 seconds)
                self.worker_insights.update_task_insights_once_in_a_while()

            # Run exit function and store results
            if self.params.worker_exit and self._run_exit_func(additional_args):
                return

            # Notify WorkerPool to start a new worker if max lifespan is reached
            self.worker_insights.update_task_insights()
            if self.params.worker_lifespan is not None and n_tasks_executed == self.params.worker_lifespan:
                self.worker_comms.signal_worker_restart()

            # Force update the number of tasks completed for this worker (only when using a progress bar)
            if self.worker_comms.has_progress_bar():
                self.worker_comms.task_completed_progress_bar(force_update=True)

        finally:
            self.worker_comms.set_worker_dead()

    def _run_init_func(self, additional_args: List) -> bool:
        """
        Runs the init function when provided.

        :param additional_args: Additional args to pass to the function (worker ID, shared objects, worker state)
        :return: True when the worker needs to shut down, False otherwise
        """
        def _init_func():
            with TimeIt(self.worker_insights.worker_init_time, self.worker_id):
                self.params.worker_init(*additional_args)

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
            with TimeIt(self.worker_insights.worker_working_time, self.worker_id,
                        self.worker_insights.max_task_duration_list, lambda: self._format_args(args, separator=' | ')):
                results = func(args)
            self.worker_insights.update_n_completed_tasks()
            return results

        return self._run_safely(_func, args)

    def _run_exit_func(self, additional_args: List) -> bool:
        """
        Runs the exit function when provided and stores its results.

        :param additional_args: Additional args to pass to the function (worker ID, shared objects, worker state)
        :return: True when the worker needs to shut down, False otherwise
        """
        def _exit_func():
            with TimeIt(self.worker_insights.worker_exit_time, self.worker_id):
                return self.params.worker_exit(*additional_args)

        results, should_return = self._run_safely(_exit_func, no_args=True)

        if should_return:
            return True
        else:
            self.worker_comms.add_exit_results(results)
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
        if self.worker_comms.exception_thrown():
            return None, True

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
        with self.worker_comms.exception_lock:

            # Only raise an exception when this process is the first one to raise. We do this because when the first
            # exception is caught by the main process the workers are joined which can cause a deadlock on draining the
            # exception queue. By only allowing one process to throw we know for sure that the exception queue will be
            # empty when the first one arrives.
            if not self.worker_comms.exception_thrown():

                # Create traceback string
                traceback_str = "\n\nException occurred in Worker-%d with the following arguments:\n%s\n%s" % (
                    self.worker_id, self._format_args(args, no_args), traceback.format_exc()
                )

                # Sometimes an exception cannot be pickled (i.e., we get the _pickle.PickleError: Can't pickle
                # <class ...>: it's not the same object as ...). We check that here by trying the pickle.dumps manually.
                # The call to `queue.put` creates a thread in which it pickles and when that raises an exception we
                # cannot catch it.
                try:
                    dill.dumps(type(err))
                except dill.PicklingError:
                    err = CannotPickleExceptionError()

                # Add exception
                self.worker_comms.add_exception(type(err), traceback_str)

    def _format_args(self, args: Any, no_args: bool = False, separator: str = '\n') -> str:
        """
        Format the function arguments to a string form.

        :param args: Funtion arguments
        :param no_args: Whether there were any args at all. If not, then return special string
        :param separator: String to use as separator between arguments
        :return: String containing the task arguments
        """
        # Determine function arguments
        func_args = args[1] if args and self.worker_comms.keep_order() else args
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
    :return: Worker class
    """
    if start_method == 'fork':
        return ForkWorker
    elif start_method == 'forkserver':
        return ForkServerWorker
    elif start_method == 'spawn':
        return SpawnWorker
    elif start_method == 'threading':
        return ThreadingWorker
    else:
        raise ValueError("Unknown start method: '{}'".format(start_method))
