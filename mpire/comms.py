import collections
import ctypes
import multiprocessing as mp
import queue
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from mpire.context import RUNNING_WINDOWS
from mpire.params import WorkerMapParams
from mpire.signal import DelayedKeyboardInterrupt

# Pill for killing workers
POISON_PILL = '\0'

# Pill for letting workers know the work is done so they can send cached stats
NON_LETHAL_POISON_PILL = '\1'

# Pill for letting workers know that the next item in the queue are new map params
NEW_MAP_PARAMS_PILL = '\2'

# Pill for letting workers know that the next item in the queue is a task being supplied by an apply function, which
# need to be processed slightly differently
APPLY_PILL = '\3'

# Fixed job IDs for the main process, worker_init, and worker_exit functions
MAIN_PROCESS = -1
INIT_FUNC = -2
EXIT_FUNC = -3


class WorkerComms:

    """
    Class that contains all the inter-process communication objects (locks, events, queues, etc.) and functionality to
    interact with them, except for the worker insights comms.

    Contains:
    - Progress bar comms
    - Tasks & (exit) results comms
    - Exception handling comms
    - Terminating and restarting comms
    
    General overview of how the comms work:
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

        # Whether or not to inform the child processes to keep order in mind (for the map functions)
        self._keep_order = self.ctx.Value(ctypes.c_bool, False, lock=True)

        # Queue to pass on tasks to child processes. We keep track of which worker completed the last task and which
        # worker is working on what task
        self._task_queues: List[mp.JoinableQueue] = []
        self._task_idx: Optional[int] = None
        self._worker_running_task: List[mp.Value] = []
        self._last_completed_task_worker_id = collections.deque()
        self._worker_working_on_job: Optional[mp.Array] = None

        # Queue where the child processes can pass on results, and counters to keep track of how many results have been
        # added and received per worker. results_added is a simple list of integers which is only accessed by the worker
        # itself
        self._results_queue: Optional[mp.JoinableQueue] = None
        self._results_added: List[int] = []
        self._results_received: Optional[mp.Array] = None

        # Array where the child processes can request a restart
        self._worker_restart_array: Optional[mp.Array] = None
        self._worker_restart_condition = self.ctx.Condition(self.ctx.Lock())

        # List of Event objects to indicate whether workers are alive
        self._workers_dead: Optional[mp.Array] = None

        # Array where the child processes indicate when they started a task, worker_init, and worker_exit used for
        # checking timeouts. The array size is n_jobs * 3, where [worker_id * 3 + i] is used for indexing. i=0 is used
        # for the worker_init, i=1 for the main task, and i=2 for the worker_exit function.
        self._workers_time_task_started: Optional[mp.Array] = None

        # Lock object such that child processes can only throw one at a time. The Event object ensures only one
        # exception can be thrown
        self.exception_lock = self.ctx.Lock()
        self._exception_thrown = self.ctx.Event()
        self._exception_job_id: Optional[mp.Value] = None
        self._kill_signal_received = self.ctx.Value(ctypes.c_bool, False, lock=True)

        # Array where the number of completed tasks is stored for the progress bar
        self._tasks_completed_array: Optional[mp.Array] = None
        self._progress_bar_last_updated: Optional[float] = None
        self._progress_bar_shutdown: Optional[mp.Value] = None
        self._progress_bar_complete: Optional[mp.Event] = None

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
        self._initialized = False

    def init_comms(self) -> None:
        """
        Initialize/Reset comms containers.

        Threading doesn't have a JoinableQueue, so the threading context returns a multiprocessing.JoinableQueue
        instead. However, in the (unlikely) scenario that threading does get one, we explicitly switch to a
        multiprocessing.JoinableQueue for both the exception queue and progress bar tasks completed queue, because the
        progress bar handler needs process-aware objects.
        """
        # Task related
        self._task_queues = [self.ctx.JoinableQueue() for _ in range(self.n_jobs)]
        self._worker_running_task = [
            self.ctx.Value(ctypes.c_bool, False, lock=self.ctx.RLock()) for _ in range(self.n_jobs)
        ]
        self._worker_working_on_job = self.ctx.Array('i', self.n_jobs, lock=True)

        # Results related
        self._results_queue = self.ctx.JoinableQueue()
        self._results_added = [0 for _ in range(self.n_jobs)]
        self._results_received = self.ctx.Array('L', self.n_jobs, lock=self.ctx.RLock())

        # Worker status
        self._worker_restart_array = self.ctx.Array(ctypes.c_bool, self.n_jobs, lock=True)
        self._workers_dead = self.ctx.Array(ctypes.c_bool, self.n_jobs, lock=True)
        self._workers_dead[:] = [True] * self.n_jobs
        self._workers_time_task_started = self.ctx.Array('d', self.n_jobs * 3, lock=True)

        # Exception related
        self._exception_thrown.clear()
        self._exception_job_id = self.ctx.Value('i', 0, lock=True)
        self._kill_signal_received.value = False

        # Progress bar related
        self._tasks_completed_array = self.ctx.Array('L', self.n_jobs, lock=self.ctx.RLock())
        self._progress_bar_last_updated = time.time()
        self._progress_bar_shutdown = self.ctx.Value(ctypes.c_bool, False, lock=True)
        self._progress_bar_complete = self.ctx.Event()

        self.reset_progress()
        self._initialized = True

    def reset_progress(self) -> None:
        """
        Resets the task_idx and last_completed_task_worker_id
        """
        self._task_idx = 0
        self._last_completed_task_worker_id.clear()
        self._tasks_completed_array[:] = [0] * self.n_jobs
        self.clear_progress_bar_shutdown()
        self.clear_progress_bar_complete()

    ################
    # Progress bar
    ################

    def task_completed_progress_bar(self, worker_id: int, progress_bar_last_updated: float,
                                    progress_bar_n_tasks_completed: Optional[int] = None,
                                    force_update: bool = False) -> Tuple[float, int]:
        """
        Signal that we've completed a task every 0.1 seconds, for the progress bar

        :param worker_id: Worker ID
        :param progress_bar_last_updated: Last time the progress bar update was send
        :param progress_bar_n_tasks_completed: Number of tasks completed since last update
        :param force_update: Whether to force an update
        :return: Tuple containing new last updated time and number of tasks completed since last update
        """
        # If it's not forced we updated the number of completed tasks
        if not force_update:
            progress_bar_n_tasks_completed += 1

        # Check if we need to update
        now = time.time()
        if force_update or (now - progress_bar_last_updated) > self.progress_bar_update_interval:
            with self._tasks_completed_array.get_lock():
                self._tasks_completed_array[worker_id] += progress_bar_n_tasks_completed
            progress_bar_last_updated = now
            progress_bar_n_tasks_completed = 0

        return progress_bar_last_updated, progress_bar_n_tasks_completed

    def get_tasks_completed_progress_bar(self) -> Union[int, str]:
        """
        Obtain the number of tasks completed by the workers. As the progress bar handler lives inside a thread we don't
        poll continuously, but every 0.1 seconds.

        :return: The number of tasks done or a poison pill
        """
        # Check if we need to wait a bit for the next update
        time_diff = time.time() - self._progress_bar_last_updated
        if time_diff < self.progress_bar_update_interval:
            time.sleep(self.progress_bar_update_interval - time_diff)

        # Sum the tasks completed and return
        while (not self.exception_thrown() and not self.kill_signal_received() and
               not self._progress_bar_shutdown.value):
            n_tasks_completed = sum(self._tasks_completed_array)
            self._progress_bar_last_updated = time.time()
            return n_tasks_completed

        return POISON_PILL

    def signal_progress_bar_shutdown(self) -> None:
        """
        Signals the progress bar handling process to shut down
        """
        self._progress_bar_shutdown.value = True

    def clear_progress_bar_shutdown(self) -> None:
        """
        Clears the progress bar shutdown signal
        """
        if self._progress_bar_shutdown is not None:
            self._progress_bar_shutdown.value = False

    def signal_progress_bar_complete(self) -> None:
        """
        Signal that the progress bar is complete
        """
        self._progress_bar_complete.set()

    def clear_progress_bar_complete(self) -> None:
        """
        Clear that the progress bar is complete
        """
        if self._progress_bar_complete is not None:
            self._progress_bar_complete.clear()

    def wait_until_progress_bar_is_complete(self) -> None:
        """
        Waits until the progress bar is completed
        """
        if self._progress_bar_complete is not None:
            self._progress_bar_complete.wait()

    ################
    # Order modifiers
    ################

    def signal_keep_order(self) -> None:
        """
        Set that we need to keep order in mind
        """
        self._keep_order.value = True

    def clear_keep_order(self) -> None:
        """
        Forget that we need to keep order in mind
        """
        self._keep_order.value = False

    def keep_order(self) -> bool:
        """
        :return: Whether we need to keep order in mind
        """
        return self._keep_order.value

    ################
    # Tasks & results
    ################

    def add_task(self, job_id: Optional[int], task: Any, worker_id: Optional[int] = None) -> None:
        """
        Add a task to the queue so a worker can process it.

        :param job_id: Job ID or None
        :param task: A tuple of arguments to pass to a worker, which acts upon it
        :param worker_id: If provided, give the task to the worker ID
        """
        worker_id = self._get_task_worker_id(worker_id)
        with DelayedKeyboardInterrupt():
            task = (job_id, task) if job_id is not None else task
            self._task_queues[worker_id].put(task, block=True)

    def add_apply_task(self, job_id: int, func: Callable, args: Tuple = (), kwargs: Dict = None):
        """
        Add a task to the queue so a worker can process it. First though, add an APPLY_PILL such that the worker knows
        it needs to treat this task differently.

        :param func: Function to apply
        :param job_id: Job ID
        :param args: Arguments to pass to the function
        :param kwargs: Keyword arguments to pass to the function
        """
        if kwargs is None:
            kwargs = {}

        worker_id = self._get_task_worker_id()
        self.add_task(None, APPLY_PILL, worker_id)
        self.add_task(job_id, (func, (args, kwargs)), worker_id)

    def _get_task_worker_id(self, worker_id: Optional[int] = None) -> int:
        """
        Get the worker ID for the next task.

        When a worker ID is not present, we first check if we need to pass on the tasks in order. If not, we check
        whether we got results already. If so, we give the next task to the worker who completed that task. Otherwise,
        we decide based on order

        :return: Worker ID
        """
        if worker_id is None:
            if self.order_tasks or not self._last_completed_task_worker_id:
                worker_id = self._task_idx % self.n_jobs
                self._task_idx += 1
            else:
                worker_id = self._last_completed_task_worker_id.popleft()

        return worker_id

    def get_task(self, worker_id: int) -> Any:
        """
        Obtain new chunk of tasks. Occasionally we check if an exception has been thrown. If so, we should quit.

        :param worker_id: Worker ID
        :return: Chunk of tasks or None when an exception was thrown
        """
        while not self.exception_thrown():
            try:
                return self._task_queues[worker_id].get(block=True, timeout=0.01)
            except queue.Empty:
                pass
        return None

    def task_done(self, worker_id: int) -> None:
        """
        Signal that we've completed a task

        :param worker_id: Worker ID
        """
        self._task_queues[worker_id].task_done()

    def set_worker_running_task(self, worker_id: int, running: bool) -> None:
        """
        Set the task the worker is currently running

        :param worker_id: Worker ID
        :param running: Whether the worker is running a task
        """
        self._worker_running_task[worker_id].value = running

    def get_worker_running_task_lock(self, worker_id: int) -> mp.RLock:
        """
        Obtain the lock for the worker running task

        :param worker_id: Worker ID
        :return: RLock
        """
        return self._worker_running_task[worker_id].get_lock()

    def get_worker_running_task(self, worker_id: int) -> bool:
        """
        Obtain whether the worker is running a task

        :param worker_id: Worker ID
        :return: Whether the worker is running a task
        """
        return self._worker_running_task[worker_id].value

    def signal_worker_working_on_job(self, worker_id: int, job_id: int) -> None:
        """
        Signal that the worker is working on the job ID

        :param worker_id: Worker ID
        :param job_id: Job ID
        """
        self._worker_working_on_job[worker_id] = job_id

    def get_worker_working_on_job(self, worker_id: int) -> int:
        """
        Obtain the job ID the worker is working on

        :param worker_id: Worker ID
        :return: Job ID
        """
        return self._worker_working_on_job[worker_id]

    def add_results(self, worker_id: Optional[int], results: List[Tuple[Optional[int], bool, Any]]) -> None:
        """
        Add results to the results queue

        :param worker_id: Worker ID
        :param results: A list of tuples of job ID, success bool, and output from the worker
        """
        if worker_id is not None:
            self._results_added[worker_id] += 1
        self._results_queue.put((worker_id, results))

    def get_results(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        """
        Obtain the next result from the results queue

        :param block: Whether to block (wait for results)
        :param timeout: How long to wait for results in case ``block==True``
        :return: The next result from the queue, which is the result of calling the function
        """
        try:
            with DelayedKeyboardInterrupt():
                worker_id, results = self._results_queue.get(block=block, timeout=timeout)
                self._results_queue.task_done()
                if worker_id is not None:
                    with self._results_received.get_lock():
                        self._results_received[worker_id] += 1
                    self._last_completed_task_worker_id.append(worker_id)
                return results
        except EOFError:
            # This can occur when an imap function was running, while at the same time terminate() was called
            return [(None, None, POISON_PILL)]

    def reset_results_received(self, worker_id: int) -> None:
        """
        Reset the number of results received from a worker

        :param worker_id: Worker ID
        """
        self._results_received[worker_id] = 0

    def wait_for_all_results_received(self, worker_id: int) -> None:
        """
        Wait for the main process to receive all the results from a specific worker

        :param worker_id: Worker ID
        """
        while self._results_received[worker_id] != self._results_added[worker_id]:
            time.sleep(0.01)

    def add_new_map_params(self, map_params: WorkerMapParams) -> None:
        """
        Submits new map params for each worker

        :param map_params: New map params
        """
        for worker_id in range(self.n_jobs):
            self.add_task(None, NEW_MAP_PARAMS_PILL, worker_id)
            self.add_task(None, map_params, worker_id)

    ################
    # Exceptions
    ################

    def signal_exception_thrown(self, job_id: int) -> None:
        """
        Set the exception event

        :param job_id: Job ID which triggered the exception
        """
        self._exception_job_id.value = job_id
        self._exception_thrown.set()

    def exception_thrown(self) -> bool:
        """
        :return: Whether an exception was thrown by one of the workers
        """
        return self._exception_thrown.is_set()

    def wait_for_exception_thrown(self, timeout: Optional[float]) -> bool:
        """
        Waits until the exception thrown event is set

        :param timeout: How long to wait before giving up
        :return: True when exception was thrown, False if timeout was reached
        """
        return self._exception_thrown.wait(timeout=timeout)

    def get_exception_thrown_job_id(self) -> int:
        """
        :return: Job ID which triggered the exception
        """
        return self._exception_job_id.value

    def signal_kill_signal_received(self) -> None:
        """
        Set the kill signal received event
        """
        self._kill_signal_received.value = True

    def kill_signal_received(self) -> bool:
        """
        :return: Whether a kill signal was received in one of the workers
        """
        return self._kill_signal_received.value

    ################
    # Terminating & restarting
    ################

    def insert_poison_pill(self) -> None:
        """
        'Tell' the workers their job is done.
        """
        for worker_id in range(self.n_jobs):
            self.add_task(None, POISON_PILL, worker_id)

    def insert_poison_pill_results_listener(self) -> None:
        """
        'Tell' the apply results listener their job is done.
        """
        self.add_results(None, [(None, True, POISON_PILL)])

    def insert_non_lethal_poison_pill(self) -> None:
        """
        When ``keep_alive=True``, the workers should stay alive, but they need to wrap up their work (like sending the
        latest progress bar update)
        """
        for worker_id in range(self.n_jobs):
            self.add_task(None, NON_LETHAL_POISON_PILL, worker_id)

    def signal_worker_restart(self, worker_id: int) -> None:
        """
        Signal to the main process that this worker needs to be restarted

        :param worker_id: Worker ID
        """
        self._worker_restart_array[worker_id] = True
        with self._worker_restart_condition:
            self._worker_restart_condition.notify()

    def signal_worker_restart_condition(self) -> None:
        """
        Signal the condition primitive, such that the worker restart handler thread can continue. This is useful when
        an exception has been thrown and the thread needs to exit.
        """
        with self._worker_restart_condition:
            self._worker_restart_condition.notify()

    def get_worker_restarts(self) -> List[int]:
        """
        Obtain the worker IDs that need to be restarted. Blocks until at least one worker needs to be restarted.
        It returns an empty list when an exception has been thrown (which also notifies the worker_done_condition)

        :return: List of worker IDs
        """
        def _get_worker_restarts():
            return [worker_id for worker_id, restart in enumerate(self._worker_restart_array) if restart]

        with self._worker_restart_condition:

            # If there aren't any workers to restart, wait until there are
            worker_ids = _get_worker_restarts()
            if not worker_ids:
                self._worker_restart_condition.wait()
                worker_ids = _get_worker_restarts()

            return worker_ids

    def reset_worker_restart(self, worker_id) -> None:
        """
        Worker has been restarted, reset signal.

        :param worker_id: Worker ID
        """
        self._worker_restart_array[worker_id] = False

    def signal_worker_alive(self, worker_id: int) -> None:
        """
        Indicate that a worker is alive

        :param worker_id: Worker ID
        """
        self._workers_dead[worker_id] = False

    def signal_worker_dead(self, worker_id: int) -> None:
        """
`       Indicate that a worker is dead

        :param worker_id: Worker ID
        """
        self._workers_dead[worker_id] = True

    def is_worker_alive(self, worker_id: int) -> bool:
        """
        Check whether the worker is alive

        :param worker_id: Worker ID
        :return: Whether the worker is alive
        """
        return not self._workers_dead[worker_id]

    def join_results_queues(self, keep_alive: bool = False) -> None:
        """
        Join results and exit results queues

        :param keep_alive: Whether to keep the queues alive
        """
        self._results_queue.join()
        if not keep_alive:
            self._results_queue.close()
            self._results_queue.join_thread()

    def join_task_queues(self, keep_alive: bool = False) -> None:
        """
        Join task queues

        :param keep_alive: Whether to keep the queues alive
        """
        [q.join() for q in self._task_queues]
        if not keep_alive:
            [q.close() for q in self._task_queues]
            [q.join_thread() for q in self._task_queues]

    def drain_results_queue_terminate_worker(self, dont_wait_event: threading.Event) -> None:
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
                self.get_results(block=False)
                dont_wait_event.clear()
                got_results = True
        except (queue.Empty, OSError):
            if got_results:
                dont_wait_event.set()

    def drain_queues(self) -> None:
        """
        Drain tasks and results queues
        """
        [self.drain_and_join_queue(q) for q in self._task_queues]
        self.drain_and_join_queue(self._results_queue)

    def drain_and_join_queue(self, q: mp.JoinableQueue, join: bool = True) -> None:
        """
        Drains a queue completely, such that it is joinable. If a timeout is reached, we give up and terminate. So far,
        I've only seen it happen when an exception is thrown when using spawn as start method, and even then it only
        happens once every 1000 runs or so.

        :param q: Queue to join
        :param join: Whether to join the queue or not
        """
        # Running this in a separate process on Windows can cause errors
        if RUNNING_WINDOWS:
            self._drain_and_join_queue(q, join)
        else:
            try:
                process = self.ctx.Process(target=self._drain_and_join_queue, args=(q, join))
                process.start()
                process.join(timeout=5)
                if process.is_alive():
                    process.terminate()
                    process.join()

                if join:
                    # The above was done in a separate process where the queue had a different feeder thread
                    q.close()
                    q.join_thread()
            except OSError:
                # Queue could be just closed when starting the drain_and_join_queue process
                pass

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

        # Call task done up to the point where we get a ValueError. We need to do this when child processes already
        # started processing on some tasks and got terminated half-way.
        n = 0
        try:
            while True:
                q.task_done()
                n += 1
        except (OSError, ValueError):
            pass

        try:
            while not q.empty() or n != 0:
                q.get(block=True, timeout=1.0)
                n -= 1
        except (OSError, EOFError, queue.Empty, ValueError):
            pass

        # Join
        if join:
            try:
                q.join()
                q.close()
                q.join_thread()
            except (OSError, ValueError):
                pass

    ################
    # Timeouts
    ################

    def signal_worker_init_started(self, worker_id: int) -> None:
        """
        Sets the worker_init started timestamp for a specific worker

        :param worker_id: Worker ID
        """
        self._workers_time_task_started[worker_id * 3] = time.time()

    def signal_worker_task_started(self, worker_id: int) -> None:
        """
        Sets the task started timestamp for a specific worker

        :param worker_id: Worker ID
        """
        self._workers_time_task_started[worker_id * 3 + 1] = time.time()

    def signal_worker_exit_started(self, worker_id: int) -> None:
        """
        Sets the worker_exit started timestamp for a specific worker

        :param worker_id: Worker ID
        """
        self._workers_time_task_started[worker_id * 3 + 2] = time.time()

    def signal_worker_init_completed(self, worker_id: int) -> None:
        """
        Resets the worker_init started timestamp for a specific worker

        :param worker_id: Worker ID
        """
        self._workers_time_task_started[worker_id * 3] = 0

    def signal_worker_task_completed(self, worker_id: int) -> None:
        """
        Resets the task started timestamp for a specific worker

        :param worker_id: Worker ID
        """
        self._workers_time_task_started[worker_id * 3 + 1] = 0

    def signal_worker_exit_completed(self, worker_id: int) -> None:
        """
        Resets the worker_exit started timestamp for a specific worker

        :param worker_id: Worker ID
        """
        self._workers_time_task_started[worker_id * 3 + 2] = 0

    def has_worker_init_timed_out(self, worker_id: int, timeout: float) -> bool:
        """
        Checks whether a worker_init takes longer than the timeout value

        :param worker_id: Worker ID
        :param timeout: Timeout in seconds
        :return: True when time has expired, False otherwise
        """
        started_time = self._workers_time_task_started[worker_id * 3]
        return self._has_worker_timed_out(started_time, timeout)

    def has_worker_task_timed_out(self, worker_id: int, timeout: float) -> bool:
        """
        Checks whether a worker task takes longer than the timeout value

        :param worker_id: Worker ID
        :param timeout: Timeout in seconds
        :return: True when time has expired, False otherwise
        """
        started_time = self._workers_time_task_started[worker_id * 3 + 1]
        return self._has_worker_timed_out(started_time, timeout)

    def has_worker_exit_timed_out(self, worker_id: int, timeout: float) -> bool:
        """
        Checks whether a worker_exit takes longer than the timeout value

        :param worker_id: Worker ID
        :param timeout: Timeout in seconds
        :return: True when time has expired, False otherwise
        """
        started_time = self._workers_time_task_started[worker_id * 3 + 2]
        return self._has_worker_timed_out(started_time, timeout)

    @staticmethod
    def _has_worker_timed_out(started_time: float, timeout: float) -> bool:
        """
        Checks whether time has passed beyond the timeout

        :param started_time: Timestamp
        :param timeout: Timeout in seconds
        :return: True when time has expired, False otherwise
        """
        return False if started_time == 0.0 else (time.time() - started_time) >= timeout
