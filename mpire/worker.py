import collections.abc
import queue
try:
    import dill as pickle
except ImportError:
    import pickle
import multiprocessing as mp
import signal
import time
import traceback
import _thread
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from threading import current_thread, main_thread, Thread
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type, Union

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

from mpire.comms import JOB_DONE_PILL, NEW_TASK_PARAMS_PILL, POISON_PILL, SKIP_JOB_PILL, Comms, WorkerComms, TaskType
from mpire.context import FORK_AVAILABLE, MP_CONTEXTS, RUNNING_WINDOWS
from mpire.dashboard.connection_utils import DashboardConnectionDetails, set_dashboard_connection
from mpire.exception import CannotPickleExceptionError, InterruptWorker, StopWorker
from mpire.task_comms import TimeIt, TaskWorkerComms, WorkerStatsType
from mpire.params import JobType, WorkerTaskParams, WorkerPoolParams
from mpire.tqdm_utils import TqdmConnectionDetails, TqdmManager


@dataclass
class RunResult:
    results: Any
    success: bool
    send_results: bool
    
    
# TODO: run with worker_lifespan. This is not working yet! This has to do with the pbar I think! The progress has been
#       reset somehow. Because of the local? we create again?


class AbstractWorker:
    """
    A multiprocessing helper class which continuously asks the queue for new jobs, until a poison pill is inserted
    """

    def __init__(self, worker_id: int, pool_params: WorkerPoolParams, worker_comms: WorkerComms, 
                 active_task_params: Dict[int, WorkerTaskParams], skipped_job_ids: Set[int],
                 tqdm_connection_details: TqdmConnectionDetails, 
                 dashboard_connection_details: DashboardConnectionDetails, start_time: float) -> None:
        """
        :param worker_id: Worker ID
        :param pool_params: WorkerPool parameters
        :param worker_comms: Worker communication objects (queues, locks, events, ...)
        :param active_task_params: Active task parameters (used in case a worker gets restarted)
        :param skipped_job_ids: Job IDs that have been skipped (used in case a worker gets restarted)
        :param tqdm_connection_details: Tqdm manager host, and whether the manager is started/connected
        :param dashboard_connection_details: Dashboard manager host, port_nr and whether a dashboard is
            started/connected
        :param start_time: Timestamp indicating at what time the Worker instance was created and started
        """
        super().__init__()

        # Parameters
        self.worker_id = worker_id
        self.pool_params = pool_params
        self.worker_comms = worker_comms
        self.active_task_params = active_task_params
        self.skipped_job_ids = skipped_job_ids
        self.tqdm_connection_details = tqdm_connection_details
        self.dashboard_connection_details = dashboard_connection_details
        self.start_time = start_time

        # Worker state
        self.worker_state = {}

        # Local variables needed for each worker
        self.n_tasks_executed: Dict[int, int] = {}
        self.task_params: Dict[int, WorkerTaskParams] = {}
        self.task_function: Dict[int, Callable] = {}
        self.additional_args = None
        self.task_worker_comms: Dict[int, TaskWorkerComms] = {}
        self.poison_pill_received = False
        self.last_job_id = None
        self.last_task_type = None
        
        self.enable_prints = not True

    def run(self) -> None:
        """
        Continuously asks the tasks queue for new task arguments. When not receiving a poisonous pill or when the max
        life span is not yet reached it will execute the new task and put the results in the results queue.
        """
        if self.enable_prints:
            print(datetime.now().isoformat(), f"Worker-{self.worker_id} started", flush=True)
        
        # Register handlers for graceful shutdown
        self._set_signal_handlers()

        try:
            self.worker_comms.signal_worker_alive()
            self.worker_comms.wait_for_started_acknowledgement()

            # Set tqdm and dashboard connection details. This is needed for nested pools and in the case forkserver or
            # spawn is used as start method
            TqdmManager.set_connection_details(self.tqdm_connection_details)
            set_dashboard_connection(self.dashboard_connection_details, auto_connect=False)

            # Store how long it took to start up
            start_up_time = time.time() - self.start_time

            # Gather and set additional args to pass to the function
            self._set_additional_args()
            
            # Set active task parameters. We sort the list so the init functions are run in the correct order
            for job_id, task_params in sorted(self.active_task_params.items()):
                self._handle_new_task_params(job_id, task_params)

            while all(params.worker_lifespan is None or self.n_tasks_executed[job_id] < params.worker_lifespan 
                      for job_id, params in self.task_params.items()):
                
                # When we received a poison pill and there are no more tasks to execute, we can return
                if self.poison_pill_received and not self.task_params:
                    if self.enable_prints:
                        print(datetime.now().isoformat(), f"Worker-{self.worker_id} received poison pill and has no more tasks to execute, returning")
                    return

                # Obtain new chunk of jobs
                with TimeIt() as wait_timer:
                    # When we recieved None this means we need to stop because the pool is terminating
                    if (next_chunked_args := self._get_new_task()) is None:
                        if self.enable_prints:
                            print(datetime.now().isoformat(), f"Worker-{self.worker_id} received None, stopping")
                        return

                # Execute jobs in this chunk
                try:
                    job_id, next_chunked_args = next_chunked_args
                    
                    # A job could have been removed in the meantime (e.g., when a skip job pill was received)
                    if job_id not in self.task_params:
                        continue
                    
                    # Apply jobs don't have task comms
                    if job_id in self.task_worker_comms:

                        # Start up time only counts for the first job
                        if start_up_time > 0.0:
                            self.task_worker_comms[job_id].add_duration(WorkerStatsType.START_UP_TIME, start_up_time)
                            start_up_time = 0.0
                    
                        # Update waiting time
                        self.task_worker_comms[job_id].add_duration(WorkerStatsType.WAITING_TIME, wait_timer.duration)
                    
                    # When we're dealing with a map function the first argument is an index, so we need to unpack it
                    if self.task_params[job_id].type == JobType.MAP:
                        batch_idx, next_chunked_args = next_chunked_args
                    else:
                        batch_idx = None
                        
                    if self.enable_prints:
                        print(datetime.now().isoformat(), f"Worker-{self.worker_id} is about to proceess job {job_id} with batch idx {batch_idx} and args {str(next_chunked_args)[:500]}")

                    results = []
                    success = True
                    for args in next_chunked_args:

                        # Try to run this function and save results. If we got a failed result, we break the loop as 
                        # there's no point in processing the remaining tasks in the batch
                        run_results = self._run_func(job_id, args)
                        if run_results.send_results:
                            results.append(run_results.results)
                        if not run_results.success:
                            success = False
                            break

                        # Send progress bar info and insights updates for map tasks
                        if self.task_params[job_id].type == JobType.MAP:
                            self._send_progress_bar_update(job_id)
                            self._send_max_duration_tasks_update(job_id)

                    # Send results back to main process
                    if results:
                        if self.enable_prints:
                            print(datetime.now().isoformat(), f"Worker-{self.worker_id} adding results for job {job_id}")
                        self.worker_comms.add_results(job_id, TaskType.TARGET_FUNC, success, batch_idx, results)
                        self.n_tasks_executed[job_id] += len(results)
                    
                    # A job is done when an apply task is done or when a map task failed
                    if self.task_params[job_id].type == JobType.APPLY or not success:
                        self._handle_job_done(job_id, job_is_skipped=True)

                # In case an exception occurred and we need to return, we want to call task_done no matter what
                finally:
                    self.worker_comms.task_done()

            # Max lifespan reached. Force send all updates and run exit functions in order
            self._send_progress_bar_update(force_update=True)
            self._send_max_duration_tasks_update(force_update=True)
            for job_id, params in sorted(self.task_params.items()):
                if params.worker_exit:
                    self._run_exit_func(job_id)

        except Exception as err:
            # Print traceback
            traceback.print_exc()
        finally:
            # Wait until all results have been received, otherwise the main process might deadlock
            # TODO: should also wait for max duration tasks and pbar updatees? For some reason sometimes a worker doesn't start again            
            if self.enable_prints:
                print(datetime.now().isoformat(), f"Worker-{self.worker_id} waiting for all results to be received")
            self.worker_comms.wait_for_all_results_received()
            
            # Close any remaining shared memory connections
            if self.enable_prints:
                print(datetime.now().isoformat(), f"Worker-{self.worker_id} closing shm connections")
            for comms in self.task_worker_comms.values():
                comms.close_shm()

            # Notify parent process to start a new worker
            if not self.worker_comms.should_terminate() and any(
                task_params.worker_lifespan is not None 
                and self.n_tasks_executed[job_id] >= task_params.worker_lifespan 
                for job_id, task_params in self.task_params.items()
            ):
                if self.enable_prints:
                    print(datetime.now().isoformat(), f"Worker-{self.worker_id} requesting restart")
                self.worker_comms.signal_worker_restart()

            if self.enable_prints:
                print(datetime.now().isoformat(), f"Worker-{self.worker_id} finished")
            self.worker_comms.signal_worker_dead()

    def _set_signal_handlers(self) -> None:
        """
        Set signal handlers for graceful shutdown
        """
        # Don't set signals when we're using threading as start method
        if current_thread() != main_thread():
            return

        # When on unix, we can make use of signals
        if not RUNNING_WINDOWS:
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            signal.signal(signal.SIGHUP, self._on_kill_exit_gracefully)
            signal.signal(signal.SIGTERM, self._on_kill_exit_gracefully)
            signal.signal(signal.SIGUSR1, self._on_exception_exit_gracefully)

        # On Windows, not all signals are available so we have to work around it.
        elif RUNNING_WINDOWS and self.pool_params.start_method != "threading" and current_thread() == main_thread():
            signal.signal(signal.SIGINT, self._on_exception_exit_gracefully)
            t = Thread(target=self._on_exception_exit_gracefully_windows, daemon=True)
            t.start()

    def _on_kill_exit_gracefully(self, *_) -> None:
        """
        When someone manually sends a kill signal to this process, we want to exit gracefully. We do this by raising an
        exception when a task is running. Otherwise, we call raise() ourselves with the exception. Both will ensure
        exception_thrown() is set and will shutdown the pool.
        """
        if self.enable_prints:
            print(datetime.now().isoformat(), f"Worker-{self.worker_id} was killed")
        err = RuntimeError(f"Worker-{self.worker_id} was killed")
        with self.worker_comms.get_running_task_lock():
            if self.worker_comms.get_running_task():
                raise err

    def _on_exception_exit_gracefully(self, *_) -> None:
        """
        This function is called when the main process sends a kill signal to this process. This can mean a few things:
        - A keyboard interrupt was issued by the user and we should stop the worker
        - An exception was encountered in another worker and this worker is running a task for the same job, so we
          should interrupt it
        - The current task timed out and we should interrupt it

        When on Windows, this function can be invoked when no function is running. This means we will need to check if
        there is a running task and only raise if there is. If we don't do that, we could interrupt the worker while,
        for example, it is busy adding results to the queue, which could lead to a deadlock. If the worker is not 
        running a function, the worker will have received a signal to stop working or cancel the current job in another
        way.
        
        When a function is running, the exception thrown here will be caught by the ``_run_safely()`` function, which
        handles the exception accordingly.
        """
        if self.enable_prints:
            print(datetime.now().isoformat(), f"Worker-{self.worker_id} got exception signal")
        with self.worker_comms.get_running_task_lock():
            if self.worker_comms.get_running_task():
                self.worker_comms.set_running_task(False)
                if self.worker_comms.should_terminate():
                    raise StopWorker
                else:
                    raise InterruptWorker

    def _on_exception_exit_gracefully_windows(self) -> None:
        """
        Windows doesn't fully support signals as Unix-based systems do. Therefore, we have to work around it. This
        function is started in a thread. We wait for a kill signal (Event object) and interrupt the main thread if we
        got it (derived from https://stackoverflow.com/a/40281422) and only when a function is running. This will raise
        a KeyboardInterrupt, which is then caught by the signal handler, which in turn checks if we need to raise a 
        StopWorker or InterruptWorker. If the worker is not running a function, the worker will have received a signal 
        to stop working or cancel the current job in another way.

        Note: functions that release the GIL won't be interupted by this procedure (e.g., time.sleep). If graceful
        shutdown takes too long the process will be terminated by the main process.
        """
        while self.worker_comms.is_worker_alive():
            if self.worker_comms.wait_for_terminate_pool(timeout=0.1):
                with self.worker_comms.get_running_task_lock():
                    if self.worker_comms.get_running_task():
                        _thread.interrupt_main()
                return
            
    def _get_new_task(self):
        """
        Obtain new chunk of tasks. Occassionally, we check for new control signals.
        """
        job_id = chunk_of_task = None
        while not self.worker_comms.should_terminate() and (not self.poison_pill_received or self.task_params):
            
            # Obtain control signals
            if (control_signal := self.worker_comms.get_control_signal(block=False)) is not None:
                if self.enable_prints:
                    print(datetime.now().isoformat(), f"Worker-{self.worker_id} got control signal: {control_signal}")
                self._handle_control_signal(*control_signal)
                
            # Obtain new task
            try:
                job_id, chunk_of_task = self.worker_comms.get_task(block=True, timeout=0.01)
                if self.enable_prints:
                    print(datetime.now().isoformat(), f"Worker-{self.worker_id} got: {job_id}, {str(chunk_of_task)[:500]}")
            except queue.Empty:
                continue
            
            # When we don't have the task parameters yet and the job is not skipped, obtain the task parameters
            while (not self.worker_comms.should_terminate() and job_id not in self.task_params 
                and job_id not in self.skipped_job_ids):
                if (
                    (control_signal := self.worker_comms.get_control_signal(block=True, timeout=0.01))
                    is not None
                ):
                    if self.enable_prints:
                        print(datetime.now().isoformat(), f"Worker-{self.worker_id} got control signal: {control_signal}")
                    self._handle_control_signal(*control_signal)
                
            # Return job ID and chunk of task when we have a valid job ID
            if job_id is not None and job_id in self.task_params:
                return job_id, chunk_of_task
            
        return None
        
    def _handle_control_signal(self, control_signal: str, metadata: Any) -> None:
        """
        Handle control signals
        
        :param control_signal: Control signal
        :param metadata: Metadata for this signal
        """
        # New task parameters
        if control_signal == NEW_TASK_PARAMS_PILL:
            job_id, task_params = metadata
            if self.enable_prints:
                print(datetime.now().isoformat(), f"Worker-{self.worker_id} received new task params for job {job_id}")
            self._handle_new_task_params(job_id, task_params)
            
        # Job done
        elif control_signal == JOB_DONE_PILL:
            job_id = metadata
            if self.enable_prints:
                print(datetime.now().isoformat(), f"Worker-{self.worker_id} received job done pill for job {job_id}")
            self._handle_job_done(job_id, job_is_skipped=False)
        
        # Skip job
        elif control_signal == SKIP_JOB_PILL:
            job_id = metadata
            if self.enable_prints:
                print(datetime.now().isoformat(), f"Worker-{self.worker_id} received skip job pill for job {job_id}")
            self._handle_job_done(job_id, job_is_skipped=True)
            
        # Poison pill
        elif control_signal == POISON_PILL:
            if self.enable_prints:
                print(datetime.now().isoformat(), f"Worker-{self.worker_id} received poison pill")
            self.poison_pill_received = True
            
        self.worker_comms.task_done_control_signal()
        
    def _handle_new_task_params(self, job_id: int, task_params: WorkerTaskParams) -> None:
        """
        Handle new task parameters.

        :param job_id: Job ID
        :param task_params: Task parameters
        """
        if task_params.type == JobType.MAP:
            if self.enable_prints:
                print(datetime.now().isoformat(), f"Worker-{self.worker_id} initializing task worker comms for job {job_id} with shm {task_params.task_comms_shm_names[self.worker_id]}")
            
            # A worker could get restarted while the last job it was processing just finished. This means the shared memory
            # object has been cleaned up, while we're still trying to connect to it here. If connecting fails, we can 
            # assume the task has been completed
            try:
                self.task_worker_comms[job_id] = TaskWorkerComms(job_id, self.worker_comms.task_comms_lock,
                                                                 task_params.task_comms_shm_names[self.worker_id])
            except FileNotFoundError:
                if self.enable_prints:
                    print(datetime.now().isoformat(), f"Worker-{self.worker_id} failed to connect to shm. Skipping job {job_id}")
                return
            
            if self.enable_prints:
                print(datetime.now().isoformat(), f"Worker-{self.worker_id} initialized task worker comms with shm {task_params.task_comms_shm_names[self.worker_id]}, pbar n: {self.task_worker_comms[job_id].progress_bar_n_tasks_completed}")
        
        self.task_params[job_id] = task_params
        self.task_function[job_id] = partial(self._call_func, partial(task_params.func, *self.additional_args))
        self.n_tasks_executed[job_id] = 0
        
        # Run init function when available
        if task_params.worker_init:
            self._run_init_func(job_id)
            
    def _handle_job_done(self, job_id: int, job_is_skipped: bool = False) -> None:
        """
        When a job done or skip job pill is received, we send a final progress bar and max duration tasks update, and 
        run the exit function if available. In case of a job done, we wait for the progress bar to be done. Finally, 
        the task specific objects are removed.
        
        :param job_id: Job ID
        :param job_is_skipped: Whether the job was skipped
        :param call_task_done: Whether to call task_done
        """
        # Job ID could have been removed in the meantime, e.g. when a task this worker was working on failed
        if job_id in self.task_params:
            
            if self.task_params[job_id].type == JobType.MAP:
                # Send progress bar info and max duration tasks updates
                self._send_progress_bar_update(job_id, force_update=True)
                self._send_max_duration_tasks_update(job_id, force_update=True)
            
            # Run exit function when available
            if self.task_params[job_id].worker_exit:
                self._run_exit_func(job_id)
            
            if self.task_params[job_id].type == JobType.MAP:
                # Wait until progress bar data has been received
                if not job_is_skipped and self.task_params[job_id].progress_bar:
                    self.task_worker_comms[job_id].wait_for_progress_bar_complete()
                    
                # Close shared memory connection
                self.task_worker_comms[job_id].close_shm()
                
        # Let the pool know we're done with this job and wait for the main process to acknowledge. When the job is 
        # skipped, this means this worker or another worker got an exception or a timeout and we don't need to signal
        if not job_is_skipped:
            self.worker_comms.add_results(job_id, TaskType.JOB_DONE, success=True, batch_idx=None, results=None)
            self.worker_comms.wait_for_all_results_received()
                
        else:
            self.skipped_job_ids.add(job_id)
            
        # Remove job ID from task specific objects
        for container in (self.task_params, self.task_function, self.n_tasks_executed, self.task_worker_comms):
            if job_id in container:
                del container[job_id]

    def _set_additional_args(self) -> None:
        """
        Gather additional args to pass to the function (worker ID, shared objects, worker state)
        """
        self.additional_args = []
        if self.pool_params.pass_worker_id:
            self.additional_args.append(self.worker_id)
        if self.pool_params.shared_objects is not None:
            self.additional_args.append(self.pool_params.shared_objects)
        if self.pool_params.use_worker_state:
            self.additional_args.append(self.worker_state)

    def _get_func(self, func: Callable, is_apply_func: bool = False) -> Callable:
        """
        Determine what function to call. If we're dealing with a map function we use the helper function with idx 
        support which deals with the provided idx variable. If we are dealing with an apply function, however, we don't
        get an idx variable.

        :param func: Function to call
        :param is_apply_func: Whether this is an apply function
        :return: Function to call
        """
        return partial(self._call_func, partial(func, *self.additional_args))

    def _run_init_func(self, job_id: int) -> None:
        """
        Runs the init function when provided.

        :param job_id: Job ID
        """
        if not self.task_params[job_id].worker_init:
            return False
        
        if self.enable_prints:
            print(datetime.now().isoformat(), f"Worker-{self.worker_id} is about to run init function for job {job_id}")
        
        self.worker_comms.signal_working_on_job(job_id, TaskType.WORKER_INIT)
        self.last_job_id = job_id
        self.last_task_type = TaskType.WORKER_INIT

        def _init_func():
            with TimeIt(self.task_worker_comms.get(job_id), WorkerStatsType.INIT_TIME):
                self.task_params[job_id].worker_init(*self.additional_args)

        # Optionally update timeout info
        if self.task_params[job_id].worker_init_timeout is not None:
            try:
                self.worker_comms.signal_worker_init_started()
                result = self._run_safely(_init_func)
            finally:
                self.worker_comms.signal_worker_init_completed()
        else:
            result = self._run_safely(_init_func)
            
        if self.enable_prints:
            print(datetime.now().isoformat(), f"Worker-{self.worker_id} finished running init function for job {job_id} with result {str(result)[:500]}")
            
        # If the init function failed and wasn't interrupt by the main process, let the main process know
        if not result.success and result.send_results:
            if self.enable_prints:
                print(datetime.now().isoformat(), f"Worker-{self.worker_id} failed running init function for job {job_id}, sending exception and skipping job")
            self.worker_comms.add_results(job_id, TaskType.WORKER_INIT, False, None, result.results)
            self._handle_job_done(job_id, job_is_skipped=True)

    def _run_func(self, job_id: int, args: Optional[List]) -> RunResult:
        """
        Runs the main function when provided.

        :param job_id: Job ID
        :param args: Args to pass to the function
        :return: Run results
        """
        if self.last_job_id != job_id or self.last_task_type != TaskType.TARGET_FUNC:
            self.worker_comms.signal_working_on_job(job_id, TaskType.TARGET_FUNC)
            self.last_job_id = job_id
            self.last_task_type = TaskType.TARGET_FUNC

        def _func():
            func = self.task_function[job_id]
            with TimeIt(self.task_worker_comms.get(job_id), WorkerStatsType.WORKING_TIME, track_max_duration=True, 
                        format_args_func=lambda: self._format_args(args, separator=' | ')):
                _results = func(*args) if self.task_params[job_id].type == JobType.APPLY else func(args)
            if job_id in self.task_worker_comms:
                self.task_worker_comms[job_id].task_completed()
            return _results

        # Optionally update timeout info
        if self.task_params[job_id].task_timeout is not None:
            try:
                self.worker_comms.signal_worker_task_started()
                result = self._run_safely(_func, args)
            finally:
                self.worker_comms.signal_worker_task_completed()
        else:
            result = self._run_safely(_func, args)

        return result

    def _run_exit_func(self, job_id: int) -> None:
        """
        Runs the exit function when provided and stores its results.

        :param job_id: Job ID
        """
        self.worker_comms.signal_working_on_job(job_id, TaskType.WORKER_EXIT)
        self.last_job_id = job_id
        self.last_task_type = TaskType.WORKER_EXIT
        
        if self.enable_prints:
            print(datetime.now().isoformat(), f"Worker-{self.worker_id} is about to run exit function for job {job_id}")

        def _exit_func():
            with TimeIt(self.task_worker_comms.get(job_id), WorkerStatsType.EXIT_TIME):
                return self.task_params[job_id].worker_exit(*self.additional_args)

        # Optionally update timeout info
        if self.task_params[job_id].worker_exit_timeout is not None:
            try:
                self.worker_comms.signal_worker_exit_started()
                result = self._run_safely(_exit_func)
            finally:
                self.worker_comms.signal_worker_exit_completed()
        else:
            result = self._run_safely(_exit_func)
            
        if self.enable_prints:
            print(datetime.now().isoformat(), f"Worker-{self.worker_id} finished running exit function for job {job_id} with result {str(result)[:500]}")

        if result.send_results:
            self.worker_comms.add_results(job_id, TaskType.WORKER_EXIT, result.success, None, result.results)

    def _run_safely(self, func: Callable, exception_args: Optional[Any] = None) -> RunResult:
        """
        A rather complex locking and exception mechanism is used here so we can make sure we only raise an exception
        when we should. See `_exit_gracefully` for more information.

        :param func: Function to run
        :param exception_args: Arguments to pass to `_format_args` when an exception occurred
        :return: Run results
        """
        if self.worker_comms.should_terminate():
            return RunResult(results=None, success=True, send_results=False)

        try:

            try:
                # Obtain lock and try to run the function. During this block a StopWorker exception from the parent
                # process can come through to signal we should stop
                self.worker_comms.set_running_task(True)
                results = func()
                self.worker_comms.set_running_task(False)

            except InterruptWorker:
                # The main process tells us to interrupt the current task. This means a timeout has expired and we
                # need to stop this task and continue with the next one
                return RunResult(results=None, success=False, send_results=False)

            except (Exception, SystemExit) as err:
                # An exception occurred inside the provided function. Let the signal handler know it shouldn't raise 
                # any StopWorker or InterruptWorker exceptions from the parent process anymore, we got this.
                self.worker_comms.set_running_task(False)
                
                # Obtain exception and send it back as normal results
                exception = self._get_exception(exception_args, err)
                return RunResult(results=exception, success=False, send_results=True)

        except StopWorker:
            # The main process tells us to stop working. If this happens the terminate pool event is set as well, so
            # actual termination will happen soon
            return RunResult(results=None, success=False, send_results=False)

        # Carry on
        return RunResult(results=results, success=True, send_results=True)

    def _get_exception(self, args: Optional[Any], err: Union[Exception, SystemExit]) -> Tuple[type, Tuple, Dict, str]:
        """
        Try to pickle the exception and create a traceback string

        :param args: Funtion arguments where exception was raised
        :param err: Exception that was raised
        :return: Tuple containing the exception type, args, state, and a traceback string
        """
        # Create traceback string
        traceback_str = f"\n\nException occurred in Worker-{self.worker_id} with the following arguments:\n" \
                        f"{self._format_args(args)}\n{traceback.format_exc()}"

        # Sometimes an exception cannot be pickled (i.e., we get the _pickle.PickleError: Can't pickle
        # <class ...>: it's not the same object as ...). We check that here by trying the pickle.dumps manually.
        # The call to `queue.put` creates a thread in which it pickles and when that raises an exception we
        # cannot catch it.
        try:
            pickle.dumps(type(err))
            pickle.dumps(err.args)
            pickle.dumps(err.__dict__)
        except (pickle.PicklingError, TypeError):
            err = CannotPickleExceptionError(repr(err))

        return type(err), err.args, err.__dict__, traceback_str

    def _format_args(self, args: Optional[Any], separator: str = '\n') -> str:
        """
        Format the function arguments to a string form.

        :param args: Funtion arguments
        :param separator: String to use as separator between arguments
        :return: String containing the task arguments
        """
        # Determine function arguments. args can be None when we're dealing with an apply task, for example when 
        # calling the init or exit function
        if args is not None and self.task_params[self.last_job_id].type == JobType.APPLY:
            func_args, func_kwargs = args
        else:
            func_args = args
            func_kwargs = None

        func_args, func_kwargs = self._convert_args_kwargs(func_args, func_kwargs)

        # Format arguments
        formatted_args = []
        formatted_args.extend([f"Arg {arg_nr}: {repr(arg)}" for arg_nr, arg in enumerate(func_args)])
        formatted_args.extend([f"Arg {str(key)}: {repr(value)}" for key, value in func_kwargs.items()])

        return separator.join(formatted_args)

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
            pass
        else:
            args = args,
        if kwargs is None:
            kwargs = {}

        return args, kwargs

    def _send_progress_bar_update(self, job_id: Optional[int] = None, force_update: bool = False) -> None:
        """
        Send progress bar update

        :param job_id: Job ID or None when to send all progress bars updates
        :param force_update: Whether to force an update
        """
        if self.enable_prints:
            print(datetime.now().isoformat(), f"Worker-{self.worker_id} sending progress bar update for job {job_id} with force update: {force_update}")
        for job_id_ in [job_id] if job_id is not None else self.task_params.keys():
            if self.task_params[job_id_].progress_bar:
                self.task_worker_comms[job_id_].send_progress_bar_update(
                    Comms.progress_bar_update_interval, force_update
                )

    def _send_max_duration_tasks_update(self, job_id: Optional[int] = None, force_update: bool = False) -> None:
        """
        Send max duration tasks update

        :param job_id: Job ID or None when to send all max duration tasks updates
        :param force_update: Whether to force an update
        """
        if self.pool_params.insights_enabled:
            for job_id_ in [job_id] if job_id is not None else self.task_params.keys():
                if job_id_ in self.task_worker_comms:
                    self.worker_comms.send_max_duration_tasks_update(
                        job_id_, self.task_worker_comms[job_id_], force_update
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
            raise ImportError("Can't use dill as the dependency \"multiprocess\" is not installed. Use `pip install "
                              "mpire[dill]` to install the required dependency")
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
            raise ValueError(f"Unknown start method: '{start_method}'")
