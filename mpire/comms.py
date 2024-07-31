import collections
import ctypes
from dataclasses import dataclass
import multiprocessing as mp
import queue
import threading
import time
from enum import Enum, auto
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

from mpire.task_comms import TaskComms, TaskWorkerComms
from mpire.params import WorkerTaskParams
from mpire.signal import DelayedKeyboardInterrupt

# Pill for killing workers
POISON_PILL = '\0'

# Pill for letting workers know the work is done so they can send cached stats
JOB_DONE_PILL = '\1'

# Pill for letting workers know that the accompanying job should be skipped
SKIP_JOB_PILL = '\2'

# Pill for letting workers know that the next item in the queue are new map params
NEW_TASK_PARAMS_PILL = '\3'

# The job done wait type is used to signal the main process that the worker has received the job done signal for a
# specific job. The max duration tasks type is used to signal the main process that the worker has sent the max
# duration tasks update.
class TaskType(Enum):
    WORKER_INIT = auto()
    TARGET_FUNC = auto()
    WORKER_EXIT = auto()
    JOB_DONE = auto()
    MAX_DURATION_TASKS = auto()
    

@dataclass(init=True, frozen=True)
class Result:
    worker_id: Optional[int] = None
    job_id: Optional[int] = None
    task_type: Optional[TaskType] = None
    success: bool = True
    batch_idx: Optional[int] = None
    within_batch_idx: Optional[int] = None
    data: Any = None
    
    
class WorkerComms:
    """
    Class that contains the inter-process communication objects (locks, events, queues, etc.) and functionality to
    interact with them, for a single worker.
    """

    def __init__(self, ctx: mp.context.BaseContext, worker_id: int, results_queue: mp.JoinableQueue, 
                 request_worker_restart_condition: mp.Condition, terminate_pool_event: mp.Event, 
                 task_comms_lock: mp.Lock) -> None:
        """
        :param ctx: Multiprocessing context
        :param worker_id: Worker ID
        :param results_queue: Queue to pass on results
        :param max_duration_tasks_queue: Queue to pass on max duration tasks
        :param request_worker_restart_condition: Condition primitive to signal the main process that the worker needs 
            to be restarted
        :param terminate_pool_event: Event to indicate that the worker needs to terminate
        :param task_comms_lock: Lock for the worker task comms
        """
        self.ctx = ctx
        self.worker_id = worker_id
        
        # Control queue to pass on control signals (job done, skip job, poison pill) and task params, and a task queue
        # to obtain tasks
        self.control_queue = ctx.JoinableQueue()
        self.task_queue = ctx.JoinableQueue()
        
        # Value to indicate whether the worker is running a task, and values to keep track of which job and task the
        # worker is working on. We reuse the running_task lock for the working_on_job and working_on_task values, as 
        # we need a single lock to ensure atomic updates.
        self.running_task = ctx.Value(ctypes.c_bool, False, lock=ctx.RLock())
        self.working_on_job = ctx.Value("i", -1, lock=False)
        self.working_on_task = ctx.Value("i", -1, lock=False)

        # Queue where the worker can pass on results, and counters to keep track of how many results have been added 
        # and received for this worker
        self.results_queue = results_queue
        self.results_added = 0
        self.results_received = ctx.Value("L", 0, lock=ctx.RLock())

        # Value to indicate whether the worker needs to be restarted, and a condition primitive to signal the main 
        # process that the worker needs to be restarted
        self.request_restart = ctx.Value(ctypes.c_bool, False, lock=ctx.RLock())
        self.request_restart_condition = request_worker_restart_condition
        self.start_acknowledged = ctx.Event()

        # Value to indicate whether the worker is alive, which is used to check whether a worker was terminated by the 
        # OS
        self.worker_dead = ctx.Value(ctypes.c_bool, True, lock=ctx.RLock())

        # Array to indicate when the worker started a task, worker_init, and worker_exit, used for checking timeouts. 
        # i=0 is used for the worker_init, i=1 for the main task, and i=2 for the worker_exit function.
        self.time_task_started = ctx.Array("d", 3, lock=ctx.RLock())

        # Event to indicate that the worker needs to terminate
        self.terminate_pool = terminate_pool_event

        # The task comms is used for worker task profiling and handling progress bar communications. The queue is used
        # to pass on max duration tasks.
        self.task_comms_lock = task_comms_lock
        
        self.enable_prints = not True

    ################
    # Initialization
    ################
    
    def reinitialize(self) -> None:
        """
        Reinitialize the worker comms, except for the queues which can still contain tasks
        """
        self.running_task = self.ctx.Value(ctypes.c_bool, False, lock=self.ctx.RLock())
        self.working_on_job = self.ctx.Value("i", -1, lock=False)
        self.working_on_task = self.ctx.Value("i", -1, lock=False)
        self.results_added = 0
        self.results_received = self.ctx.Value("L", 0, lock=self.ctx.RLock())
        self.request_restart = self.ctx.Value(ctypes.c_bool, False, lock=self.ctx.RLock())
        self.start_acknowledged = self.ctx.Event()
        self.worker_dead = self.ctx.Value(ctypes.c_bool, True, lock=self.ctx.RLock())
        self.time_task_started = self.ctx.Array("d", 3, lock=self.ctx.RLock())

    ################
    # Max duration tasks comms
    ################
    
    def send_max_duration_tasks_update(self, job_id: int, task_comms: TaskWorkerComms, 
                                       force_update: bool = False) -> None:
        """
        Send worker insights update every 2 seconds

        :param job_id: Job ID
        :param task_comms: Task comms object
        :param force_update: Whether to force an update
        """
        # Check if we need to update
        now = time.time()
        if force_update or (
            (now - task_comms.max_duration_tasks_last_updated) > self.max_duration_tasks_update_interval
        ):
            self.add_results(job_id, TaskType.MAX_DURATION_TASKS, True, None, task_comms.max_duration_tasks)
            task_comms.max_duration_tasks_last_updated = now

    ################
    # Control
    ################
    
    def add_task_params(self, job_id: int, task_params: WorkerTaskParams) -> None:
        """
        Submits new task params to the workers

        :param job_id: Job ID
        :param task_params: New task params
        """
        if self.enable_prints:
            print(f"WorkerComms-{self.worker_id} - Adding task params:", job_id, task_params)
        self.control_queue.put((NEW_TASK_PARAMS_PILL, (job_id, task_params)))
            
    def add_job_done(self, job_id: int) -> None:
        """
        Let the worker know this specific job is done. This will ensure the worker wraps up its work (like sending the
        latest progress bar/insights updates)
        
        :param job_id: Job ID
        """
        if self.enable_prints:
            print(f"WorkerComms-{self.worker_id} - Adding job done:", job_id)
        self.control_queue.put((JOB_DONE_PILL, job_id))
    
    def add_skip_job(self, job_id: int) -> None:
        """
        Let the worker know this specific job should be skipped. This is used when a worker encounters an exception
        
        :param job_id: Job ID
        """
        if self.enable_prints:
            print(f"WorkerComms-{self.worker_id} - Adding skip job:", job_id)
        self.control_queue.put((SKIP_JOB_PILL, job_id))
            
    def add_poison_pill(self) -> None:
        """
        'Tell' the worker it should wrap up and shut down.
        """
        if self.enable_prints:
            print(f"WorkerComms-{self.worker_id} - Adding poison pill")
        self.control_queue.put((POISON_PILL, None))
            
    def get_control_signal(self, block: bool = False, timeout: Optional[float] = None) -> Optional[Tuple[str, Any]]:
        """
        Obtain the next control signal from the queue
        
        :param block: Whether to block
        :param timeout: How long to wait before giving up
        :return: Control signal + metadata, or None when the queue is empty
        """
        try:
            return self.control_queue.get(block=block, timeout=timeout)
        except queue.Empty:
            return None
        
    def task_done_control_signal(self) -> None:
        """
        Signal that the worker completed a control signal task
        """
        self.control_queue.task_done()
        
    ################
    # Tasks & results
    ################

    def add_task(self, job_id: Optional[int], task: Any) -> None:
        """
        Add a task to the queue so the worker can process it.

        :param job_id: Job ID or None
        :param task: A tuple of arguments to pass to the worker, which acts upon it
        """
        with DelayedKeyboardInterrupt():
            task = (job_id, task) if job_id is not None else task
            if self.enable_prints:
                print(f"WorkerComms-{self.worker_id} - Adding task:", task)
            self.task_queue.put(task, block=True)

    def add_apply_task(self, job_id: int, params: WorkerTaskParams, args: Tuple = (), kwargs: Optional[Dict] = None):
        """
        Add an apply task to the queue so the worker can process it.

        :param params: Parameters to pass on
        :param job_id: Job ID
        :param args: Arguments to pass to the function
        :param kwargs: Keyword arguments to pass to the function
        """
        if kwargs is None:
            kwargs = {}

        self.add_task_params(job_id, params)
        self.add_task(job_id, ((args, kwargs),))

    def get_task(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        """
        Obtain new chunk of tasks.

        :param block: Whether to block
        :param timeout: How long to wait before giving up
        :return: Task
        :raises queue.Empty: When the queue is empty
        """
        return self.task_queue.get(block=block, timeout=timeout)

    def task_done(self) -> None:
        """
        Signal that we've completed a task
        """
        self.task_queue.task_done()

    def set_running_task(self, running: bool) -> None:
        """
        Set the worker is currently running (or not) a task

        :param running: Whether the worker is running a task
        """
        self.running_task.value = running

    def get_running_task(self) -> bool:
        """
        Obtain whether the worker is running a task

        :return: Whether the worker is running a task
        """
        return self.running_task.value

    def get_running_task_lock(self) -> mp.RLock:
        """
        Obtain the lock for the worker running task

        :return: RLock
        """
        return self.running_task.get_lock()

    def signal_working_on_job(self, job_id: int, task_type: TaskType) -> None:
        """
        Signal that the worker is working on the job ID

        :param job_id: Job ID
        :param task: Task value
        """
        with self.get_running_task_lock():
            self.working_on_job.value = job_id
            self.working_on_task.value = task_type.value

    def get_working_on_job(self) -> Tuple[int, TaskType]:
        """
        Obtain the job ID and task the worker is working on

        :return: Job ID and task type
        """
        with self.get_running_task_lock():
            return (
                self.working_on_job.value, 
                TaskType(self.working_on_task.value) if self.working_on_task.value != -1 else None
            )

    def add_results(self, job_id: Optional[int], task_type: Optional[TaskType], success: bool, 
                    batch_idx: Optional[int], results: Any) -> None:
        """
        Add results to the results queue

        :param job_id: Job ID. None when inserting a pill
        :param task_type: Task type. None when inserting a pill
        :param success: Whether the task was successful
        :param batch_idx: Batch index. None when dealing with apply tasks or when inserting a pill
        :param results: Results to add. Can be a batch of results, an exception, or a poison pill
        """
        self.results_added += 1
        self.results_queue.put(Result(worker_id=self.worker_id, job_id=job_id, task_type=task_type, success=success,
                                      batch_idx=batch_idx, data=results))
        if self.enable_prints:
            print("WorkerComms: Added results:", str(results)[:500])
        
    def signal_result_received(self) -> None:
        """
        Signal that the result has been received
        """
        # Acquire lock to ensure atomic update
        with self.results_received.get_lock():
            self.results_received.value += 1

    def wait_for_all_results_received(self) -> None:
        """
        Wait for the main process to receive all the results
        """
        while self.results_received.value != self.results_added:
            time.sleep(0.001)

    ################
    # Exceptions
    ################

    def should_terminate(self) -> bool:
        """
        :return: Whether the pool should terminate
        """
        return self.terminate_pool.is_set()

    def wait_for_terminate_pool(self, timeout: Optional[float]) -> bool:
        """
        Waits until the terminate pool event is set

        :param timeout: How long to wait before giving up
        :return: True when the pool should terminate, False when the timeout was reached
        """
        return self.terminate_pool.wait(timeout=timeout)
    

    ################
    # Terminating & restarting
    ################

    def signal_worker_restart(self) -> None:
        """
        Signal to the main process that this worker needs to be restarted
        """
        self.request_restart.value = True
        with self.request_restart_condition:
            self.request_restart_condition.notify()
        
    def get_worker_restart(self) -> bool:
        """
        Obtain whether the worker needs to be restarted

        :return: Whether the worker needs to be restarted
        """
        return self.request_restart.value

    def signal_worker_alive(self) -> None:
        """
        Indicate that the worker is alive
        """
        self.worker_dead.value = False

    def signal_worker_dead(self) -> None:
        """
`       Indicate that the worker is dead
        """
        self.worker_dead.value = True

    def is_worker_alive(self) -> bool:
        """
        Check whether the worker is alive

        :return: Whether the worker is alive
        """
        return not self.worker_dead.value
    
    def acknowledge_worker_started(self) -> None:
        """
        Acknowledge that the worker has started. This is used to ensure that the worker has started after a restart.
        """
        self.start_acknowledged.set()
        
    def wait_for_started_acknowledgement(self) -> None:
        """
        Wait for the worker to acknowledge that it has started
        """
        self.start_acknowledged.wait()

    ################
    # Timeouts
    ################

    def signal_worker_init_started(self) -> None:
        """
        Sets the worker_init started timestamp
        """
        self.time_task_started[0] = time.time()

    def signal_worker_task_started(self) -> None:
        """
        Sets the task started timestamp
        """
        self.time_task_started[1] = time.time()

    def signal_worker_exit_started(self) -> None:
        """
        Sets the worker_exit started timestamp
        """
        self.time_task_started[2] = time.time()

    def signal_worker_init_completed(self) -> None:
        """
        Resets the worker_init started timestamp
        """
        self.time_task_started[0] = 0

    def signal_worker_task_completed(self) -> None:
        """
        Resets the task started timestamp
        """
        self.time_task_started[1] = 0

    def signal_worker_exit_completed(self) -> None:
        """
        Resets the worker_exit started timestamp
        """
        self.time_task_started[2] = 0

    def has_worker_init_timed_out(self, timeout: float) -> bool:
        """
        Checks whether a worker_init takes longer than the timeout value

        :param timeout: Timeout in seconds
        :return: True when time has expired, False otherwise
        """
        return self._has_worker_timed_out(self.time_task_started[0], timeout)

    def has_worker_task_timed_out(self, timeout: float) -> bool:
        """
        Checks whether a worker task takes longer than the timeout value

        :param timeout: Timeout in seconds
        :return: True when time has expired, False otherwise
        """
        return self._has_worker_timed_out(self.time_task_started[1], timeout)

    def has_worker_exit_timed_out(self, timeout: float) -> bool:
        """
        Checks whether a worker_exit takes longer than the timeout value

        :param timeout: Timeout in seconds
        :return: True when time has expired, False otherwise
        """
        return self._has_worker_timed_out(self.time_task_started[2], timeout)

    @staticmethod
    def _has_worker_timed_out(started_time: float, timeout: float) -> bool:
        """
        Checks whether time has passed beyond the timeout

        :param started_time: Timestamp
        :param timeout: Timeout in seconds
        :return: True when time has expired, False otherwise
        """
        return False if started_time == 0.0 else (time.time() - started_time) >= timeout


class Comms:

    """
    Class that contains all the inter-process communication objects (locks, events, queues, etc.) and functionality to
    interact with them.
    
    General overview of how the comms work:  TODO: update
    - When ``map`` or ``imap`` is used, the workers need to return the ``idx`` of the task they just completed. This is
        needed to return the results in order. This is communicated by using the ``_keep_order`` boolean value.
    - The main process assigns tasks to the workers by using their respective task queue (``_task_queues``). When no
        tasks have been completed yet, the main process assigns tasks in order. To determine which worker to assign the
        next task to, the main process uses the ``_task_idx`` counter. When tasks have been completed, the main process
        assigns the next task to the worker that completed the last task. This is communicated by using the
        ``_last_completed_task_worker_id`` deque.
    - Each worker keeps track of whether it is running a task by using the ``_worker_running_task`` boolean value. This
        is used by the main process in case a worker needs to be interrupted (due to an exception somewhere else). 
        When a worker is not busy with any task at the moment, the worker will exit itself because of the 
        ``_exception_thrown`` event that is set in such cases. However, when it is running a task, we want to interrupt
        it. The RLock object is used to ensure that there are no race conditions when accessing the 
        ``_worker_running_task`` boolean value. E.g., when getting and setting the value without another process
        doing something in between.
    - Each worker also keeps track of which job it is working on by using the ``_worker_working_on_job`` array. This is
        needed to assess whether a certain task times out, and we need to know which job to set to failed.
    - The workers communicate their results to the main process by using the results queue (``_results_queue``). Each
        worker keeps track of how many results it has added to the queue (``_results_added``), and the main process 
        keeps track of how many results it has received from each worker (``_results_received``). This is used by the
        workers to know when they can safely exit.
    - Workers can request a restart when a maximum lifespan is configured and reached. This is done by setting the
        ``_worker_restart_array`` boolean array. The main process listens to this array and restarts the worker when
        needed. The ``_worker_restart_condition`` is used to signal the main process that a worker needs to be 
        restarted.
    - The ``_workers_dead`` array is used to keep track of which workers are alive and which are not. Sometimes, a
        worker can be terminated by the OS (e.g., OOM), which we want to pick up on. The main process checks regularly
        whether a worker is still alive according to the OS and according to the worker itself. If the OS says it's 
        dead, but the value in ``_workers_dead`` is still False, we know something went wrong.
    - The ``_workers_time_task_started`` array is used to keep track of when a worker started a task. This is used by
        the main process to check whether a worker times out. 
    - Exceptions are communicated by using the ``_exception_thrown`` event. Both the main process as the workers can set
        this event. The main process will set this when, for example, a timeout has been reached when running a ``map``
        task. The source of the exception is stored in the ``_exception_job_id`` value, which is used by the main 
        process to obtain the exception and raise accordingly.
    - The workers communicate every 0.1 seconds how many tasks they have completed. This is used by the main process to
        update the progress bar. The workers communicate this by using the ``_tasks_completed_array`` array. The 
        ``_progress_bar_last_updated`` datetime object is used to keep track of when the last update was sent. The
        ``_progress_bar_shutdown`` boolean value is used to signal the progress bar handler thread to shut down. The
        ``_progress_bar_complete`` event is used to signal the main process and workers that the progress bar is 
        complete and that it's safe to exit.
    """

    # Amount of time in between each progress bar update
    progress_bar_update_interval = 0.1
    max_duration_tasks_update_interval = 2.0

    def __init__(self, ctx: mp.context.BaseContext, n_jobs: int, order_tasks: bool) -> None:
        """
        :param ctx: Multiprocessing context
        :param n_jobs: Number of workers
        :param order_tasks: Whether to provide tasks to the workers in order, such that worker 0 will get chunk 0,
            worker 1 will get chunk 1, etc.
        """
        self.ctx = ctx
        self.n_jobs = n_jobs
        self.order_tasks = order_tasks
        self._initialized = False
        
        # Communication objects for the workers
        self._worker_comms: List[WorkerComms] = []
        
        # The task comms is used for worker task profiling and handling progress bar communications.
        self._task_comms: Dict[int, TaskComms] = {}
        self._task_comms_locks: List[mp.Lock] = []

        # To keep track of which worker completed the last task per job. The -1 job ID is used for apply tasks
        self._task_idx: Dict[int, int] = {}
        self._last_completed_task_worker_id = collections.deque()

        # Queue where the child processes can pass on results
        self._results_queue: Optional[mp.JoinableQueue] = None

        # Condition primitive to signal the main process that a worker needs to be restarted
        self._worker_restart_condition = self.ctx.Condition(self.ctx.Lock())

        # Event to indicate that the pool needs to terminate
        self._terminate_pool = self.ctx.Event()
        
        self.enable_prints = not True
        
        # Initialize
        self.init_comms()

    ################
    # Initialization
    ################

    def is_initialized(self) -> bool:
        """
        :return: Whether the comms have been initialized
        """
        return self._initialized

    def reset(self) -> None:
        """
        Resets initialization state. Note: doesn't actually reset the comms, just resets the state.
        """
        for comms in self._task_comms.values():
            comms.clean_up_shm()
        self._task_comms.clear()
        self._initialized = False

    def init_comms(self) -> None:
        """
        Initialize/Reset comms containers.

        Threading doesn't have a JoinableQueue, so the threading context returns a multiprocessing.JoinableQueue
        instead. However, in the (unlikely) scenario that threading does get one, we explicitly switch to a
        multiprocessing.JoinableQueue for both the exception queue and progress bar tasks completed queue, because the
        progress bar handler needs process-aware objects.
        """
        if self.is_initialized():
            return
        
        if self.enable_prints:
            print("Comms: Initializing comms")
        
        # Task comms
        self._task_comms_locks = [self.ctx.Lock() for _ in range(self.n_jobs)]
        
        # Last task related. -1 is used for apply tasks
        self._task_idx = {-1: 0}
        self._last_completed_task_worker_id.clear()

        # Results related
        self._results_queue = self.ctx.JoinableQueue()

        # Exception related
        self._terminate_pool.clear()

        # Worker comms
        self._worker_comms = [
            WorkerComms(
                ctx=self.ctx, worker_id=worker_id, results_queue=self._results_queue, 
                request_worker_restart_condition=self._worker_restart_condition, 
                terminate_pool_event=self._terminate_pool, task_comms_lock=self._task_comms_locks[worker_id]
            ) for worker_id in range(self.n_jobs)
        ]
        
        # init_comms is called when the pool is started for the first time. We need to acknowledge that the workers
        # have started, so the workers know they can start working. Waiting for acknowledgement is only useful in case
        # the workers are restarted.
        for comms in self._worker_comms:
            comms.acknowledge_worker_started()

        self._initialized = True
        
    def reinit_worker_comms(self, worker_id: int) -> None:
        """
        Reinitialize the comms for a worker. This is used when a worker is restarted.
        """
        self._worker_comms[worker_id].reinitialize()
        
    def get_worker_comms(self, worker_id: int) -> WorkerComms:
        """
        Obtain the worker comms for a specific worker ID

        :param worker_id: Worker ID
        :return: Worker comms
        """
        return self._worker_comms[worker_id]

    ################
    # Task comms
    ################
    
    def create_task_comms(self, job_id: int) -> None:
        """
        Create task comms for a specific job ID

        :param job_id: Job ID
        """
        self._task_comms[job_id] = TaskComms(self.n_jobs, job_id, self._task_comms_locks)
        
    def get_task_comms_shm_names(self, job_id: int) -> List[str]:
        """
        Obtain the shared memory names for the task comms
        
        :param job_id: Job ID
        :return: List of shared memory names
        """
        return self._task_comms[job_id].get_shm_names()
    
    def clean_up_task_comms_shm(self, job_id: int) -> None:
        """
        Clean up the shared memory objects in the task comms
        
        :param job_id: Job ID
        """
        if self.enable_prints:
            print("Comms cleaning up task comms SHM:", job_id, job_id in self._task_comms)
        if job_id in self._task_comms:
            self._task_comms[job_id].clean_up_shm()
        if self.enable_prints:
            print("Comms cleaned up task comms SHM:", job_id)
            
    def get_n_tasks_completed(self, job_id: int) -> int:
        """
        Obtain the number of tasks completed for a specific job ID. Waits until the next progress bar update interval
        
        :param job_id: Job ID
        :return: Number of tasks completed
        """
        try:
            # Check if we need to wait a bit for the next update
            time_diff = time.time() - self._task_comms[job_id].progress_bar_last_updated
            if time_diff < self.progress_bar_update_interval:
                time.sleep(self.progress_bar_update_interval - time_diff)
            
            # Get the total number of completed tasks
            n_completed_tasks = self._task_comms[job_id].get_n_tasks_completed()
            self._task_comms[job_id].progress_bar_last_updated = time.time()
            return n_completed_tasks
        
        # A KeyError can occur when pool.terminate() was called while the worker was still working on a task
        except KeyError:
            return 0
    
    def signal_progress_bar_complete(self, job_id: int) -> None:
        """
        Signals that the progress bar is complete (i.e., n_tasks_completed == total_tasks)
        
        :param job_id: Job ID
        """
        self._task_comms[job_id].signal_progress_bar_complete()
        
    def wait_for_progress_bar_complete(self, job_id: int) -> None:
        """
        Waits until the progress bar is complete
        
        :param job_id: Job ID
        """
        self._task_comms[job_id].wait_for_progress_bar_complete()
            
    def wait_until_progress_bar_is_complete(self, job_id: int) -> None:
        """
        Waits until the progress bar is completed
        
        :param job_id: Job ID
        """
        self._task_comms[job_id].wait_for_progress_bar_complete()
    
    def update_max_duration_tasks(self, job_id: int, max_duration_tasks: List[Tuple[float, Optional[str]]]) -> None:
        """
        Update the max duration tasks for a specific job
        
        :param job_id: Job ID
        :param max_duration_tasks: List of max duration tasks
        """
        self._task_comms[job_id].update_max_duration_tasks(max_duration_tasks)
        
    def get_all_task_insights(self, as_str: bool = False) -> Union[Dict, str]:
        """
        Obtain the task insights for all jobs
        
        :param as_str: Whether to return the insights as a string
        :return: Task insights as a dict or string
        """
        return {job_id: self.get_task_insights(job_id, as_str=as_str) for job_id in self._task_comms}
        
    def get_task_insights(self, job_id: int, as_str: bool = False) -> Union[Dict, str]:
        """
        Obtain the task insights for a specific job
        
        :param job_id: Job ID
        :param as_str: Whether to return the insights as a string
        :return: Task insights as a dict or string
        """
        return self._task_comms[job_id].get_insights_string() if as_str else self._task_comms[job_id].get_insights()

    ################
    # Control
    ################
    
    def add_task_params(self, job_id: int, task_params: WorkerTaskParams) -> None:
        """
        Submits new task params to the workers

        :param job_id: Job ID
        :param task_params: New task params
        """
        for comms in self._worker_comms:
            comms.add_task_params(job_id, task_params)
            
    def add_job_done(self, job_id: int) -> None:
        """
        Let the workers know this specific job is done. This will ensure workers wrap up their work (like sending the
        latest progress bar/insights updates)
        
        :param job_id: Job ID
        """
        for comms in self._worker_comms:
            comms.add_job_done(job_id)
    
    def add_skip_job(self, job_id: int) -> None:
        """
        Add a job ID to the skip jobs queue. This is used when a job is skipped because of an exception.
        
        :param job_id: Job ID
        """
        for comms in self._worker_comms:
            comms.add_skip_job(job_id)
            
    def add_poison_pill(self) -> None:
        """
        'Tell' the workers they should wrap up and shut down.
        """
        for comms in self._worker_comms:
            comms.add_poison_pill()
        
    ################
    # Tasks & results
    ################

    def add_task(self, job_id: int, task: Any, worker_id: Optional[int] = None) -> None:
        """
        Add a task to the queue so a worker can process it.

        :param job_id: Job ID
        :param task: A tuple of arguments to pass to a worker, which acts upon it
        :param worker_id: If provided, give the task to the worker ID
        """
        worker_id = self._get_task_worker_id(job_id, worker_id)
        if self.enable_prints:
            print("Comms adding task:", job_id, task, worker_id, self.order_tasks)
        self._worker_comms[worker_id].add_task(job_id, task)

    def add_apply_task(self, job_id: int, params: WorkerTaskParams, args: Tuple = (), kwargs: Optional[Dict] = None):
        """
        Add an apply task to the queue so a worker can process it.

        :param params: Parameters to pass on
        :param job_id: Job ID
        :param args: Arguments to pass to the function
        :param kwargs: Keyword arguments to pass to the function
        """
        worker_id = self._get_task_worker_id(job_id=-1)
        self._worker_comms[worker_id].add_apply_task(job_id, params, args, kwargs)

    def _get_task_worker_id(self, job_id: int, worker_id: Optional[int] = None) -> int:
        """
        Get the worker ID for the next task.

        When a worker ID is not present, we first check if we need to pass on the tasks in order. If not, we check
        whether we got results already. If so, we give the next task to the worker who completed that task. Otherwise,
        we decide based on order

        :param job_id: Job ID
        :param worker_id: Worker ID or None. When worker ID is provided, we give the task to that worker
        :return: Worker ID
        """
        if worker_id is None:
            if self.order_tasks or not self._last_completed_task_worker_id:
                if job_id not in self._task_idx:
                    self._task_idx[job_id] = 0
                worker_id = self._task_idx[job_id] % self.n_jobs
                self._task_idx[job_id] += 1
            else:
                worker_id = self._last_completed_task_worker_id.popleft()

        return worker_id

    def get_worker_running_task_lock(self, worker_id: int) -> mp.RLock:
        """
        Obtain the lock for the worker running task

        :param worker_id: Worker ID
        :return: RLock
        """
        return self._worker_comms[worker_id].get_running_task_lock()

    def get_worker_running_task(self, worker_id: int) -> bool:
        """
        Obtain whether the worker is running a task

        :param worker_id: Worker ID
        :return: Whether the worker is running a task
        """
        return self._worker_comms[worker_id].get_running_task()

    def get_worker_working_on_job(self, worker_id: int) -> Tuple[int, TaskType]:
        """
        Obtain the job ID and task the worker is working on

        :param worker_id: Worker ID
        :return: Job ID and task type
        """
        return self._worker_comms[worker_id].get_working_on_job()

    def get_results(self, block: bool = True, timeout: Optional[float] = None
                    ) -> Generator[Tuple[Optional[int], Optional[TaskType], bool, Optional[int], Any], None, None]:
        """
        Obtain the next result from the results queue

        :param block: Whether to block (wait for results)
        :param timeout: How long to wait for results in case ``block==True``
        :return: The next result from the queue, which is the result of calling the target function or worker exit
            function. The tuple contains the job ID, task type, success bool, batch_idx and the output from the worker
        """
        try:
            with DelayedKeyboardInterrupt():
                result = self._results_queue.get(block=block, timeout=timeout)
                self._results_queue.task_done()
                
                if self.enable_prints:
                    print("Comms got result:", str(result)[:500])
                
                if result.worker_id is not None:
                    self._worker_comms[result.worker_id].signal_result_received()
                    self._last_completed_task_worker_id.append(result.worker_id)

                # Yield batch of results one by one. If we got a failure, the last result will always contain the
                # exception. The batch idx is only added when not None and when it's a success
                batch = result.data if isinstance(result.data, list) else [result.data]
                for idx, single_result in enumerate(batch):
                    yield Result(worker_id=result.worker_id, job_id=result.job_id, task_type=result.task_type,
                                 success=result.success if idx == len(batch) - 1 else True, batch_idx=result.batch_idx,
                                 within_batch_idx=idx, data=single_result)
                    
        except EOFError:
            # This can occur when an imap function was running, while at the same time terminate() was called
            yield Result(data=POISON_PILL)

    ################
    # Exceptions
    ################

    def signal_terminate_pool(self) -> None:
        """
        Set the terminate pool event
        """
        self._terminate_pool.set()

    def should_terminate(self) -> bool:
        """
        :return: Whether the pool should terminate
        """
        return self._terminate_pool.is_set()
    

    ################
    # Terminating & restarting
    ################

    def insert_poison_pill_results_listener(self) -> None:
        """
        'Tell' the results listener its job is done.
        """
        self._results_queue.put(Result(data=POISON_PILL))

    def signal_worker_restart_condition(self) -> None:
        """
        Signal the condition primitive, such that the worker restart handler thread can continue. This is useful when
        an exception has been thrown and the thread needs to exit.
        """
        with self._worker_restart_condition:
            self._worker_restart_condition.notify()
            
    def get_worker_restart(self, worker_id: int) -> bool:
        """
        Obtain whether the worker needs to be restarted

        :param worker_id: Worker ID
        :return: Whether the worker needs to be restarted
        """
        return self._worker_comms[worker_id].get_worker_restart()

    def get_worker_restarts(self) -> List[int]:
        """
        Obtain the worker IDs that need to be restarted. Blocks until at least one worker needs to be restarted.
        It returns an empty list when an exception has been thrown (which also notifies the worker_done_condition)

        :return: List of worker IDs
        """
        def _get_worker_restarts():
            return [worker_id for worker_id, comms in enumerate(self._worker_comms) if comms.get_worker_restart()]

        with self._worker_restart_condition:

            # If there aren't any workers to restart, wait until there are
            worker_ids = _get_worker_restarts()
            if not worker_ids:
                self._worker_restart_condition.wait()
                worker_ids = _get_worker_restarts()

            return worker_ids

    def is_worker_alive(self, worker_id: int) -> bool:
        """
        Check whether the worker is alive

        :param worker_id: Worker ID
        :return: Whether the worker is alive
        """
        return self._worker_comms[worker_id].is_worker_alive()
    
    def acknowledge_worker_started(self, worker_id: int) -> None:
        """
        Acknowledge a worker started. I.e., the worker has set the worker alive status to True. This is used to ensure
        that the worker has started after a restart.
        
        :param worker_id: Worker ID
        """
        self._worker_comms[worker_id].acknowledge_worker_started()
    
    def signal_worker_dead(self, worker_id: int) -> None:
        """
`       Indicate that a worker is dead

        :param worker_id: Worker ID
        """
        self._worker_comms[worker_id].signal_worker_dead()

    def join_results_queues(self, keep_alive: bool = False) -> None:
        """
        Join results and exit results queues

        :param keep_alive: Whether to keep the queues alive
        """
        self._results_queue.join()
        if not keep_alive:
            self._results_queue.close()
            self._results_queue.join_thread()

    def drain_results_queue(self, dont_wait_event: threading.Event) -> None:
        """
        Drain the results queue without blocking. This is done when terminating workers, while they could still be busy
        putting something in the queues. This function will always be called from within a thread.

        :param dont_wait_event: Event object to indicate whether other termination threads should continue. I.e., when
            we set it to False, threads should wait.
        """
        # Get results from the results queue. If we got any, keep going and inform the other termination threads to wait
        # until this one's finished
        got_results = False
        try:
            while True:
                list(self.get_results(block=False))
                dont_wait_event.clear()
                got_results = True
        except (queue.Empty, OSError):
            if got_results:
                dont_wait_event.set()

    ################
    # Timeouts
    ################

    def has_worker_init_timed_out(self, worker_id: int, timeout: float) -> bool:
        """
        Checks whether a worker_init takes longer than the timeout value

        :param worker_id: Worker ID
        :param timeout: Timeout in seconds
        :return: True when time has expired, False otherwise
        """
        return self._worker_comms[worker_id].has_worker_init_timed_out(timeout)

    def has_worker_task_timed_out(self, worker_id: int, timeout: float) -> bool:
        """
        Checks whether a worker task takes longer than the timeout value

        :param worker_id: Worker ID
        :param timeout: Timeout in seconds
        :return: True when time has expired, False otherwise
        """
        return self._worker_comms[worker_id].has_worker_task_timed_out(timeout)

    def has_worker_exit_timed_out(self, worker_id: int, timeout: float) -> bool:
        """
        Checks whether a worker_exit takes longer than the timeout value

        :param worker_id: Worker ID
        :param timeout: Timeout in seconds
        :return: True when time has expired, False otherwise
        """
        return self._worker_comms[worker_id].has_worker_exit_timed_out(timeout)
