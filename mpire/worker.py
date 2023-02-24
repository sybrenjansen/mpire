import collections.abc
try:
    import dill as pickle
except ImportError:
    import pickle
import multiprocessing as mp
import signal
import traceback
import _thread
from datetime import datetime
from functools import partial
from threading import current_thread, main_thread, Thread
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

try:
    import multiprocess
    DILL_INSTALLED = True
except ImportError:
    DILL_INSTALLED = False
try:
    import numpy as np
    NUMPY_INSTALLED = True
except ImportError:
    np = None
    NUMPY_INSTALLED = False

from mpire.comms import (APPLY_PILL, EXIT_FUNC, INIT_FUNC, NEW_MAP_PARAMS_PILL, NON_LETHAL_POISON_PILL, POISON_PILL,
                         WorkerComms)
from mpire.context import FORK_AVAILABLE, MP_CONTEXTS, RUNNING_WINDOWS
from mpire.dashboard.connection_utils import DashboardConnectionDetails, set_dashboard_connection
from mpire.exception import CannotPickleExceptionError, StopWorker
from mpire.insights import WorkerInsights
from mpire.params import WorkerMapParams, WorkerPoolParams
from mpire.tqdm_utils import TqdmConnectionDetails, TqdmManager
from mpire.utils import TimeIt


class AbstractWorker:
    """
    A multiprocessing helper class which continuously asks the queue for new jobs, until a poison pill is inserted
    """

    def __init__(self, worker_id: int, pool_params: WorkerPoolParams, map_params: WorkerMapParams,
                 worker_comms: WorkerComms, worker_insights: WorkerInsights,
                 tqdm_connection_details: TqdmConnectionDetails,
                 dashboard_connection_details: DashboardConnectionDetails, start_time: datetime) -> None:
        """
        :param worker_id: Worker ID
        :param pool_params: WorkerPool parameters
        :param map_params: WorkerPool map parameters
        :param worker_comms: Worker communication objects (queues, locks, events, ...)
        :param worker_insights: WorkerInsights object which stores the worker insights
        :param tqdm_connection_details: Tqdm manager host, and whether the manager is started/connected
        :param dashboard_connection_details: Dashboard manager host, port_nr and whether a dashboard is
            started/connected
        :param start_time: `datetime` object indicating at what time the Worker instance was created and started
        """
        super().__init__()

        # Parameters
        self.worker_id = worker_id
        self.pool_params = pool_params
        self.map_params = map_params
        self.worker_comms = worker_comms
        self.worker_insights = worker_insights
        self.tqdm_connection_details = tqdm_connection_details
        self.dashboard_connection_details = dashboard_connection_details
        self.start_time = start_time

        # Worker state
        self.worker_state = {}

        # Local variables needed for each worker
        self.progress_bar_last_updated = datetime.now()
        self.progress_bar_n_tasks_completed = 0
        self.max_task_duration_last_updated = datetime.now()
        self.max_task_duration_list = self.worker_insights.get_max_task_duration_list(self.worker_id)
        self.last_job_id = None

        # Exception handling variables
        if self.pool_params.start_method == 'threading':
            ctx = MP_CONTEXTS['threading']
        else:
            ctx = MP_CONTEXTS['mp_dill' if self.pool_params.use_dill else 'mp'][self.pool_params.start_method]
        self.is_running = False
        self.is_running_lock = ctx.Lock()

        # Register handler for graceful shutdown. This doesn't work on Windows
        if not RUNNING_WINDOWS and current_thread() == main_thread():
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
        self.worker_comms.signal_kill_signal_received()
        with self.is_running_lock:
            if self.is_running:
                self.is_running = False
                raise StopWorker

    def _exit_gracefully_windows(self) -> None:
        """
        Windows doesn't fully support signals as Unix-based systems do. Therefore, we have to work around it. This
        function is started in a thread. We wait for a kill signal (Event object) and interrupt the main thread if we
        got it (derived from https://stackoverflow.com/a/40281422). This will raise a KeyboardInterrupt, which is then
        caught by the signal handler, which in turn checks if we need to raise a StopWorker.

        Note: functions that release the GIL won't be interupted by this procedure (e.g., time.sleep). If graceful
        shutdown takes too long the process will be terminated by the main process. If anyone has a better approach for
        graceful interrupt in Windows, please create a PR.
        """
        while self.worker_comms.is_worker_alive(self.worker_id):
            if self.worker_comms.wait_for_exception_thrown(timeout=0.1):
                _thread.interrupt_main()
                return

    def run(self) -> None:
        """
        Continuously asks the tasks queue for new task arguments. When not receiving a poisonous pill or when the max
        life span is not yet reached it will execute the new task and put the results in the results queue.
        """
        # Enable graceful shutdown for Windows. Note that we can't kill threads in Python
        if RUNNING_WINDOWS and self.pool_params.start_method != "threading" and current_thread() == main_thread():
            signal.signal(signal.SIGINT, self._exit_gracefully)
            t = Thread(target=self._exit_gracefully_windows)
            t.start()

        self.worker_comms.signal_worker_alive(self.worker_id)

        # Set tqdm and dashboard connection details. This is needed for nested pools and in the case forkserver or
        # spawn is used as start method
        TqdmManager.set_connection_details(self.tqdm_connection_details)
        set_dashboard_connection(self.dashboard_connection_details, auto_connect=False)

        n_tasks_executed = 0
        try:
            # Store how long it took to start up
            self.worker_insights.update_start_up_time(self.worker_id, self.start_time)

            # Obtain additional args to pass to the function
            additional_args = []
            if self.pool_params.pass_worker_id:
                additional_args.append(self.worker_id)
            if self.pool_params.shared_objects is not None:
                additional_args.append(self.pool_params.shared_objects)
            if self.pool_params.use_worker_state:
                additional_args.append(self.worker_state)

            # Run initialization function. If it returns True it means an exception occurred and we should exit
            if self.map_params.worker_init and self._run_init_func(additional_args):
                return

            # Determine what function to call. If we have to keep in mind the order (for map) we use the helper function
            # with idx support which deals with the provided idx variable.
            func = self._get_func(self.map_params.func, additional_args)

            while self.map_params.worker_lifespan is None or n_tasks_executed < self.map_params.worker_lifespan:

                # Obtain new chunk of jobs
                with TimeIt(self.worker_insights.worker_waiting_time, self.worker_id):
                    next_chunked_args = self.worker_comms.get_task(self.worker_id)
                    is_apply_func = False

                # Force update task insights and progress bar when we got a (non-lethal) poison pill. At this point, we
                # know for sure that all results have been processed. In case of a lethal pill we additionally run the
                # worker exit function, wait for all the exit results to be obtained, wait for the progress bar to be
                # done, and stop. Otherwise, we simply continue
                if next_chunked_args == POISON_PILL or next_chunked_args == NON_LETHAL_POISON_PILL:
                    self._update_task_insights(force_update=True)
                    self._update_progress_bar(force_update=True)
                    self.worker_comms.task_done(self.worker_id)
                    if next_chunked_args == POISON_PILL:
                        if self.map_params.worker_exit:
                            self._run_exit_func(additional_args)
                        if self.map_params.progress_bar:
                            self.worker_comms.wait_until_progress_bar_is_complete()
                        return
                    else:
                        continue

                # Update the map parameters of this function when new parameters are provided
                elif next_chunked_args == NEW_MAP_PARAMS_PILL:
                    self.worker_comms.task_done(self.worker_id)
                    self.map_params = self.worker_comms.get_task(self.worker_id)
                    func = self._get_func(self.map_params.func, additional_args)
                    self.worker_comms.task_done(self.worker_id)
                    continue

                # When an apply pill is received, we simply execute the function and put the result in the results queue
                elif next_chunked_args == APPLY_PILL:
                    self.worker_comms.task_done(self.worker_id)
                    job_id, (apply_func, args) = self.worker_comms.get_task(self.worker_id)
                    func = self._get_func(apply_func, additional_args)
                    next_chunked_args = job_id, (args,)
                    is_apply_func = True

                # When we recieved None this means we need to stop because of an exception in the main process
                elif next_chunked_args is None:
                    return

                # Execute jobs in this chunk
                try:
                    results = []
                    job_id, next_chunked_args = next_chunked_args
                    if self.last_job_id != job_id:
                        self.worker_comms.signal_worker_working_on_job(self.worker_id, job_id)
                        self.last_job_id = job_id
                    for args in next_chunked_args:

                        # Try to run this function and save results
                        results_part, success_part, should_shut_down = self._run_func(func, job_id, args, is_apply_func)
                        if should_shut_down:
                            return
                        results.append((job_id, success_part, results_part))

                        # Update progress bar info
                        self._update_progress_bar()

                    # Send results back to main process
                    self.worker_comms.add_results(self.worker_id, results)
                    n_tasks_executed += len(results)

                # In case an exception occurred and we need to return, we want to call task_done no matter what
                finally:
                    self.worker_comms.task_done(self.worker_id)

                # Update task insights
                self._update_task_insights()

            # Max lifespan reached
            self._update_task_insights(force_update=True)
            self._update_progress_bar(force_update=True)
            if self.map_params.worker_exit and self._run_exit_func(additional_args):
                return

        finally:
            # Wait until all results have been received, otherwise the main process might deadlock
            self.worker_comms.wait_for_all_results_received(self.worker_id)

            # Notify WorkerPool to start a new worker
            if (not self.worker_comms.exception_thrown() and self.map_params.worker_lifespan is not None and
                    n_tasks_executed >= self.map_params.worker_lifespan):
                self.worker_comms.signal_worker_restart(self.worker_id)

            self.worker_comms.signal_worker_dead(self.worker_id)

    def _get_func(self, func: Callable, additional_args: List) -> Callable:
        """
        Determine what function to call. If we have to keep in mind the order (for map) we use the helper function with
        idx support which deals with the provided idx variable.

        :param additional_args: Additional args list
        :return: Function to call
        """
        helper_func = self._helper_func_with_idx if self.worker_comms.keep_order() else self._helper_func
        return partial(helper_func, partial(func, *additional_args))

    def _run_init_func(self, additional_args: List) -> bool:
        """
        Runs the init function when provided.

        :param additional_args: Additional args to pass to the function (worker ID, shared objects, worker state)
        :return: True when the worker needs to shut down, False otherwise
        """
        self.worker_comms.signal_worker_working_on_job(self.worker_id, INIT_FUNC)
        self.last_job_id = INIT_FUNC

        def _init_func():
            with TimeIt(self.worker_insights.worker_init_time, self.worker_id):
                self.map_params.worker_init(*additional_args)

        # Optionally update timeout info
        if self.map_params.worker_init_timeout is not None:
            try:
                self.worker_comms.signal_worker_init_started(self.worker_id)
                _, _, should_shut_down = self._run_safely(_init_func, INIT_FUNC, is_apply_func=False)
            finally:
                self.worker_comms.signal_worker_init_completed(self.worker_id)
        else:
            _, _, should_shut_down = self._run_safely(_init_func, INIT_FUNC, is_apply_func=False)

        return should_shut_down

    def _run_func(self, func: Callable, job_id: Optional[int], args: Optional[List],
                  is_apply_func: bool) -> Tuple[Any, bool, bool]:
        """
        Runs the main function when provided.

        :param func: Function to call
        :param job_id: Job ID
        :param args: Args to pass to the function
        :param is_apply_func: Whether this is an apply function
        :return: Tuple containing results from the function, a boolean value indicating whether the function was run
            successfully, and a boolean value indicating whether the worker needs to shut down
        """
        def _func():
            with TimeIt(self.worker_insights.worker_working_time, self.worker_id,
                        self.max_task_duration_list, lambda: self._format_args(args, is_apply_func, separator=' | ')):
                _results = func(*args) if is_apply_func else func(args)
            self.worker_insights.update_n_completed_tasks(self.worker_id)
            return _results

        # Optionally update timeout info
        if self.map_params.task_timeout is not None:
            try:
                self.worker_comms.signal_worker_task_started(self.worker_id)
                results, success, should_shut_down = self._run_safely(_func, job_id, args, is_apply_func)
            finally:
                self.worker_comms.signal_worker_task_completed(self.worker_id)
        else:
            results, success, should_shut_down = self._run_safely(_func, job_id, args, is_apply_func)

        return results, success, should_shut_down

    def _run_exit_func(self, additional_args: List) -> bool:
        """
        Runs the exit function when provided and stores its results.

        :param additional_args: Additional args to pass to the function (worker ID, shared objects, worker state)
        :return: True when the worker needs to shut down, False otherwise
        """
        self.worker_comms.signal_worker_working_on_job(self.worker_id, EXIT_FUNC)
        self.last_job_id = EXIT_FUNC

        def _exit_func():
            with TimeIt(self.worker_insights.worker_exit_time, self.worker_id):
                return self.map_params.worker_exit(*additional_args)

        # Optionally update timeout info
        if self.map_params.worker_exit_timeout is not None:
            try:
                self.worker_comms.signal_worker_exit_started(self.worker_id)
                results, success, should_shut_down = self._run_safely(_exit_func, EXIT_FUNC, is_apply_func=False)
            finally:
                self.worker_comms.signal_worker_exit_completed(self.worker_id)
        else:
            results, success, should_shut_down = self._run_safely(_exit_func, EXIT_FUNC, is_apply_func=False)

        if should_shut_down:
            return True
        else:
            self.worker_comms.add_results(self.worker_id, [(EXIT_FUNC, True, results)])
            return False

    def _run_safely(self, func: Callable, job_id: Optional[int], exception_args: Optional[Any] = None,
                    is_apply_func: bool = False) -> Tuple[Any, bool, bool]:
        """
        A rather complex locking and exception mechanism is used here so we can make sure we only raise an exception
        when we should. See `_exit_gracefully` for more information.

        :param func: Function to run
        :param job_id: Job ID
        :param exception_args: Arguments to pass to `_format_args` when an exception occurred
        :param is_apply_func: Whether this is an apply function
        :return: Tuple containing results from the function, a boolean value indicating whether the function was run
            successfully, and a boolean value indicating whether the worker needs to shut down
        """
        if self.worker_comms.exception_thrown():
            return None, True, True

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

            except (Exception, SystemExit) as err:
                # An exception occurred inside the provided function. Let the signal handler know it shouldn't raise any
                # StopWorker exceptions from the parent process anymore, we got this.
                with self.is_running_lock:
                    self.is_running = False

                if is_apply_func:
                    # Obtain exception and send it back as normal results. False indicates the job has failed
                    exception = self._get_exception(exception_args, True, err)
                    return exception, False, False
                else:
                    # Pass exception to parent process and stop
                    self._raise(job_id, exception_args, err)
                    raise StopWorker

        except StopWorker:
            # Stop working
            return None, True, True

        # Carry on
        return results, True, False

    def _raise(self, job_id: Optional[int], args: Optional[Any], err: Union[Exception, SystemExit]) -> None:
        """
        Create exception and pass it to the parent process. Let other processes know an exception is set

        :param job_id: Job ID
        :param args: Funtion arguments where exception was raised
        :param err: Exception that should be passed on to parent process
        """
        # Only raise an exception when this process is the first one to raise
        if not self.worker_comms.exception_thrown():

            # Let others know we need to stop
            self.worker_comms.signal_exception_thrown(job_id)

            # Get exception and traceback string
            exception = self._get_exception(args, False, err)

            # Add exception
            self.worker_comms.add_results(self.worker_id, [(job_id, False, exception)])

    def _get_exception(self, args: Optional[Any], is_apply_func: bool,
                       err: Union[Exception, SystemExit]) -> Tuple[type, Tuple, Dict, str]:
        """
        Try to pickle the exception and create a traceback string

        :param args: Funtion arguments where exception was raised
        :param is_apply_func: Whether this is an apply function
        :param err: Exception that was raised
        :return: Tuple containing the exception type, args, state, and a traceback string
        """
        # Create traceback string
        traceback_str = "\n\nException occurred in Worker-%d with the following arguments:\n%s\n%s" % (
            self.worker_id, self._format_args(args, is_apply_func), traceback.format_exc()
        )

        # Sometimes an exception cannot be pickled (i.e., we get the _pickle.PickleError: Can't pickle
        # <class ...>: it's not the same object as ...). We check that here by trying the pickle.dumps manually.
        # The call to `queue.put` creates a thread in which it pickles and when that raises an exception we
        # cannot catch it.
        try:
            pickle.dumps(type(err))
            pickle.dumps(err.args)
            pickle.dumps(err.__dict__)
        except pickle.PicklingError:
            err = CannotPickleExceptionError(repr(err))

        return type(err), err.args, err.__dict__, traceback_str

    def _format_args(self, args: Optional[Any], is_apply_func: bool = False, separator: str = '\n') -> str:
        """
        Format the function arguments to a string form.

        :param args: Funtion arguments
        :param is_apply_func: Whether this is an apply function
        :param separator: String to use as separator between arguments
        :return: String containing the task arguments
        """
        # Determine function arguments
        if is_apply_func:
            func_args, func_kwargs = args
        else:
            func_args = args[1] if args and self.worker_comms.keep_order() else args
            func_kwargs = None

        func_args, func_kwargs = self._convert_args_kwargs(func_args, func_kwargs)

        # Format arguments
        formatted_args = []
        formatted_args.extend(["Arg %d: %s" % (arg_nr, repr(arg)) for arg_nr, arg in enumerate(func_args)])
        formatted_args.extend(["Arg %s: %s" % (str(key), repr(value)) for key, value in func_kwargs.items()])

        return separator.join(formatted_args)

    def _helper_func_with_idx(self, func: Callable, args: Tuple[int, Any]) -> Tuple[int, Any]:
        """
        Helper function which calls the function `func` but preserves the order index

        :param func: Function to call each time new task arguments become available
        :param args: Tuple of ``(idx, _args)`` where ``_args`` correspond to the arguments to pass on to the function.
            ``idx`` is used to preserve order
        :return: (idx, result of calling the function with the given arguments) tuple
        """
        return args[0], self._call_func(func, args[1])

    def _helper_func(self, func: Callable, args: Any, kwargs: Optional[Dict] = None) -> Any:
        """
        Helper function which calls the function `func`

        :param func: Function to call each time new task arguments become available
        :param args: Arguments to pass on to the function
        :param kwargs: Keyword arguments to pass to the function
        :return: Result of calling the function with the given arguments) tuple
        """
        return self._call_func(func, args, kwargs)

    def _call_func(self, func: Callable, args: Any, kwargs: Optional[Dict] = None) -> Any:
        """
        Helper function which calls the function `func` and passes the arguments in the correct way

        :param func: Function to call each time new task arguments become available
        :param args: Arguments to pass on to the function. If this is a dictionary and kwargs is not provided, then
            these args will be treated as keyword arguments. If this is an iterable, then the arguments will be
            unpacked.
        :param kwargs: Keyword arguments to pass to the function
        :return: Result of calling the function with the given arguments) tuple
        """
        args, kwargs = self._convert_args_kwargs(args, kwargs)
        return func(*args, **kwargs)

    @staticmethod
    def _convert_args_kwargs(args: Any, kwargs: Optional[Dict] = None) -> Tuple[Tuple, Dict]:
        """
        Convert the arguments to a tuple and keyword arguments to a dictionary.

        If args is a dictionary and kwargs is not provided, then these args will be treated as keyword arguments. If
        this is an iterable (but not str, bytes, or numpy array), then these arguments will be unpacked.

        :param args: Arguments
        :param kwargs: Keyword arguments
        :return: Args and kwargs
        """
        if isinstance(args, dict) and kwargs is None:
            kwargs = args
            args = ()
        elif (isinstance(args, collections.abc.Iterable) and not isinstance(args, (str, bytes)) and not
                (NUMPY_INSTALLED and isinstance(args, np.ndarray))):
            args = args
        else:
            args = args,
        if kwargs is None:
            kwargs = {}

        return args, kwargs

    def _update_progress_bar(self, force_update: bool = False) -> None:
        """
        Update the progress bar data

        :param force_update: Whether to force an update
        """
        if self.map_params.progress_bar:
            (self.progress_bar_last_updated,
             self.progress_bar_n_tasks_completed) = self.worker_comms.task_completed_progress_bar(
                self.worker_id, self.progress_bar_last_updated, self.progress_bar_n_tasks_completed, force_update
             )

    def _update_task_insights(self, force_update: bool = False) -> None:
        """
        Update the task insights data

        :param force_update: Whether to force an update
        """
        self.max_task_duration_last_updated = self.worker_insights.update_task_insights(
            self.worker_id, self.max_task_duration_last_updated, self.max_task_duration_list, force_update=force_update
        )


if FORK_AVAILABLE:
    class ForkWorker(AbstractWorker, MP_CONTEXTS['mp']['fork'].Process):
        pass

    class ForkServerWorker(AbstractWorker, MP_CONTEXTS['mp']['forkserver'].Process):
        pass


class SpawnWorker(AbstractWorker, MP_CONTEXTS['mp']['spawn'].Process):
    pass


class ThreadingWorker(AbstractWorker, MP_CONTEXTS['threading'].Thread):
    pass


if DILL_INSTALLED:
    if FORK_AVAILABLE:
        class DillForkWorker(AbstractWorker, MP_CONTEXTS['mp']['fork'].Process):
            pass

        class DillForkServerWorker(AbstractWorker, MP_CONTEXTS['mp_dill']['forkserver'].Process):
            pass

    class DillSpawnWorker(AbstractWorker, MP_CONTEXTS['mp_dill']['spawn'].Process):
        pass


def worker_factory(start_method: str, use_dill: bool) -> Type[Union[AbstractWorker, mp.Process, Thread]]:
    """
    Returns the appropriate worker class given the start method

    :param start_method: What Process/Threading start method to use, see the WorkerPool constructor
    :param use_dill: Whether to use dill has serialization backend. Some exotic types (e.g., lambdas, nested functions)
        don't work well when using ``spawn`` as start method. In such cased, use ``dill`` (can be a bit slower
        sometimes)
    :return: Worker class
    """
    if start_method == 'threading':
        return ThreadingWorker
    elif use_dill:
        if not DILL_INSTALLED:
            raise ImportError("Can't use dill as the dependencies are not installed. Use `pip install mpire[dill]` to "
                              "install the required dependencies.")
        elif start_method == 'fork':
            if not FORK_AVAILABLE:
                raise ValueError("Start method 'fork' is not available")
            return DillForkWorker
        elif start_method == 'forkserver':
            if not FORK_AVAILABLE:
                raise ValueError("Start method 'forkserver' is not available")
            return DillForkServerWorker
        elif start_method == 'spawn':
            return DillSpawnWorker
        else:
            raise ValueError(f"Unknown start method with dill: '{start_method}'")
    else:
        if start_method == 'fork':
            if not FORK_AVAILABLE:
                raise ValueError("Start method 'fork' is not available")
            return ForkWorker
        elif start_method == 'forkserver':
            if not FORK_AVAILABLE:
                raise ValueError("Start method 'forkserver' is not available")
            return ForkServerWorker
        elif start_method == 'spawn':
            return SpawnWorker
        else:
            raise ValueError("Unknown start method: '{}'".format(start_method))
