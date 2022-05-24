import logging
import multiprocessing as mp
import queue
import threading
import time
from datetime import datetime
from typing import Any, Generator, Optional, Tuple, Union

from mpire.params import WorkerMapParams
from mpire.signal import DelayedKeyboardInterrupt

logger = logging.getLogger(__name__)

POISON_PILL = '\0'
NON_LETHAL_POISON_PILL = '\1'
NEW_MAP_PARAMS_PILL = '\2'


class WorkerComms:

    """
    Class that contains all the inter-process communication objects (locks, events, queues, etc.) and functionality to
    interact with them, except for the worker insights comms.

    Contains:
    - Progress bar comms
    - Tasks & (exit) results comms
    - Exception handling comms
    - Terminating and restarting comms
    """

    # Amount of time in between each progress bar update
    progress_bar_update_interval = 0.1

    def __init__(self, ctx: mp.context.BaseContext, n_jobs: int) -> None:
        """
        :param ctx: Multiprocessing context
        :param n_jobs: Number of workers
        """
        self.ctx = ctx
        self.n_jobs = n_jobs

        # Whether or not to inform the child processes to keep order in mind (for the map functions)
        self._keep_order = self.ctx.Event()

        # Queue to pass on tasks to child processes
        self._task_queues = None
        self._task_idx = None
        self._last_completed_task_worker_id = None

        # Queue where the child processes can pass on results
        self._results_queue = None

        # Queue where the child processes can store exit results in
        self._exit_results_queues = []
        self._all_exit_results_obtained = None

        # Array where the child processes can request a restart
        self._worker_done_array = None

        # List of Event objects to indicate whether workers are alive, together with accompanying locks
        self._workers_dead = None
        self._workers_dead_locks = None

        # Array where the child processes indicate when they started a task, worker_init, and worker_exit used for
        # checking timeouts. The array size is n_jobs * 3, where worker_id * 3 + i is used for indexing. i=0 is used for
        # the worker_init, i=1 for the main task, and i=2 for the worker_exit function.
        self._workers_time_task_started = None

        # Queue where the child processes can pass on an encountered exception
        self._exception_queue = None

        # Lock object such that child processes can only throw one at a time. The Event object ensures only one
        # exception can be thrown. When the threading backend is used we switch to a multiprocessing Event, because the
        # progress bar handler needs a process-aware object
        self.exception_lock = self.ctx.Lock()
        self._exception_thrown = self.ctx.Event()
        self._kill_signal_received = self.ctx.Event()

        # Array where the number of completed tasks is stored for the progress bar. We don't use the vanilla lock from
        # multiprocessing.Array, but create a lock per worker such that workers can write concurrently.
        self._tasks_completed_array = None
        self._tasks_completed_locks = None
        self._progress_bar_last_updated = None
        self._progress_bar_shutdown = None
        self._progress_bar_complete = None

    ################
    # Initialization
    ################

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
        self.reset_last_completed_task_info()

        # Results related
        self._results_queue = self.ctx.JoinableQueue()
        self._exit_results_queues = [self.ctx.JoinableQueue() for _ in range(self.n_jobs)]
        self._all_exit_results_obtained = self.ctx.Event()

        # Worker status
        self._worker_done_array = self.ctx.Array('b', self.n_jobs, lock=False)
        self._workers_dead = [self.ctx.Event() for _ in range(self.n_jobs)]
        [worker_dead.set() for worker_dead in self._workers_dead]
        self._workers_dead_locks = [self.ctx.Lock() for _ in range(self.n_jobs)]
        self._workers_time_task_started = self.ctx.Array('d', self.n_jobs * 3, lock=True)

        # Exception related
        self._exception_queue = self.ctx.JoinableQueue()
        self._exception_thrown.clear()
        self._kill_signal_received.clear()

        # Progress bar related
        self._tasks_completed_array = self.ctx.Array('L', self.n_jobs, lock=False)  # ULong (should be enough)
        self._tasks_completed_locks = [self.ctx.Lock() for _ in range(self.n_jobs)]
        self._progress_bar_last_updated = datetime.now()
        self._progress_bar_shutdown = self.ctx.Event()
        self._progress_bar_complete = self.ctx.Event()

    def reset_last_completed_task_info(self) -> None:
        """
        Resets the task_idx and last_completed_task_worker_id
        """
        self._task_idx = 0
        self._last_completed_task_worker_id = None

    ################
    # Progress bar
    ################

    def task_completed_progress_bar(self, worker_id: int, progress_bar_last_updated: datetime,
                                    progress_bar_n_tasks_completed: Optional[int] = None,
                                    force_update: bool = False) -> Tuple[datetime, int]:
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
        now = datetime.now()
        if force_update or (now - progress_bar_last_updated).total_seconds() > self.progress_bar_update_interval:
            with self._tasks_completed_locks[worker_id]:
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
        time_diff = (datetime.now() - self._progress_bar_last_updated).total_seconds()
        if time_diff < self.progress_bar_update_interval:
            time.sleep(self.progress_bar_update_interval - time_diff)

        # Sum the tasks completed and return
        while (not self.exception_thrown() and not self.kill_signal_received() and
               not self._progress_bar_shutdown.is_set()):
            n_tasks_completed = 0
            for worker_id in range(self.n_jobs):
                with self._tasks_completed_locks[worker_id]:
                    n_tasks_completed += self._tasks_completed_array[worker_id]
            self._progress_bar_last_updated = datetime.now()
            return n_tasks_completed

        return POISON_PILL

    def signal_progress_bar_shutdown(self) -> None:
        """
        Signals the progress bar handling process to shut down
        """
        self._progress_bar_shutdown.set()

    def signal_progress_bar_complete(self) -> None:
        """
        Signal that the progress bar is complete
        """
        self._progress_bar_complete.set()

    def wait_until_progress_bar_is_complete(self) -> None:
        """
        Waits until the progress bar is completed
        """
        while not self.exception_thrown():
            if self._progress_bar_complete.wait(timeout=0.01):
                return

    ################
    # Order modifiers
    ################

    def signal_keep_order(self) -> None:
        """
        Set that we need to keep order in mind
        """
        return self._keep_order.set()

    def clear_keep_order(self) -> None:
        """
        Forget that we need to keep order in mind
        """
        return self._keep_order.clear()

    def keep_order(self) -> bool:
        """
        :return: Whether we need to keep order in mind
        """
        return self._keep_order.is_set()

    ################
    # Tasks & results
    ################

    def add_task(self, task: Any, worker_id: Optional[int] = None) -> None:
        """
        Add a task to the queue so a worker can process it.

        :param task: A tuple of arguments to pass to a worker, which acts upon it
        :param worker_id: If provided, give the task to the worker ID
        """
        # When a worker ID is not present, we check whether we got results already. If so, we give the next task to the
        # worker who completed that task. Otherwise, we decide based on order
        if worker_id is None:
            if self._last_completed_task_worker_id is not None:
                worker_id = self._last_completed_task_worker_id
                self._last_completed_task_worker_id = None
            else:
                worker_id = self._task_idx % self.n_jobs
                self._task_idx += 1

        with DelayedKeyboardInterrupt():
            self._task_queues[worker_id].put(task, block=True)

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

    def add_results(self, worker_id: int, results: Any) -> None:
        """
        :param worker_id: Worker ID
        :param results: Results from the main function
        """
        self._results_queue.put((worker_id, results))

    def get_results(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        """
        Obtain the next result from the results queue. We catch the queue.Empty and raise it again after the
        DelayedKeyboardInterrupt, to clean up the exception traceback whenever a keyboard interrupt is issued. If we
        wouldn't have done this, there would be an additional queue.Empty error traceback, which we don't want.

        :param block: Whether to block (wait for results)
        :param timeout: How long to wait for results in case ``block==True``
        :return: The next result from the queue, which is the result of calling the function
        """
        queue_empty_error = None
        with DelayedKeyboardInterrupt():
            try:
                self._last_completed_task_worker_id, results = self._results_queue.get(block=block, timeout=timeout)
                self._results_queue.task_done()
            except queue.Empty as e:
                queue_empty_error = e
        if queue_empty_error is not None:
            raise queue_empty_error
        return results

    def add_exit_results(self, worker_id: int, results: Any) -> None:
        """
        :param worker_id: Worker ID
        :param results: Results from the exit function
        """
        self._exit_results_queues[worker_id].put(results)

    def get_exit_results(self, worker_id: int, timeout: Optional[float], block: bool = True) -> Any:
        """
        Obtain exit results from a specific worker. When block=False and the queue is empty we catch the queue.Empty and
        raise it again after the DelayedKeyboardInterrupt, to clean up the exception traceback whenever a keyboard
        interrupt is issued. If we wouldn't have done this, there would be an additional queue.Empty error traceback,
        which we don't want.

        :param worker_id: Worker ID
        :param timeout: Timeout in seconds for the worker_exit function
        :param block: Whether to block (wait for results)
        :return: Exit results
        """
        # We always check the queue when block=False. This is needed when keep_alive=True and the workers have not been
        # explicitly stopped and joined. In that case terminate() is called from WorkerPool.__exit__ and we need to
        # drain the queue. When terminate is called the exception_thrown is set
        while not self.exception_thrown() or not block:
            if timeout is not None and self.has_worker_exit_timed_out(worker_id, timeout):
                raise TimeoutError
            queue_empty_error = None
            with DelayedKeyboardInterrupt():
                try:
                    results = self._exit_results_queues[worker_id].get(block=block, timeout=0.01)
                    self._exit_results_queues[worker_id].task_done()
                    return results
                except queue.Empty as e:
                    if not block:
                        queue_empty_error = e
            if queue_empty_error is not None:
                raise queue_empty_error

    def signal_all_exit_results_obtained(self) -> None:
        """
        Signal that all exit results have been obtained
        """
        self._all_exit_results_obtained.set()

    def wait_until_all_exit_results_obtained(self) -> None:
        """
        Waits until all exit results have been obtained
        """
        while not self.exception_thrown():
            if self._all_exit_results_obtained.wait(timeout=0.01):
                return

    def add_new_map_params(self, map_params: WorkerMapParams) -> None:
        """
        Submits new map params for each worker

        :param map_params: New map params
        """
        for worker_id in range(self.n_jobs):
            self.add_task(NEW_MAP_PARAMS_PILL, worker_id)
            self.add_task(map_params, worker_id)

    ################
    # Exceptions
    ################

    def add_exception(self, err_type: type, traceback_str: str) -> None:
        """
        Add exception details to the queue and set the exception event

        :param err_type: Type of the exception
        :param traceback_str: Traceback string
        """
        self._exception_queue.put((err_type, traceback_str))

    def add_exception_poison_pill(self) -> None:
        """
        Signals the exception handling thread to shut down
        """
        with DelayedKeyboardInterrupt():
            self._exception_queue.put((POISON_PILL, POISON_PILL))

    def get_exception(self) -> Tuple[type, str]:
        """
        :return: Tuple containing the type of the exception and the traceback string
        """
        with DelayedKeyboardInterrupt():
            return self._exception_queue.get(block=True)

    def task_done_exception(self) -> None:
        """
        Signal that we've completed a task for the exception queue
        """
        self._exception_queue.task_done()

    def signal_exception_thrown(self) -> None:
        """
        Set the exception event
        """
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

    def signal_kill_signal_received(self) -> None:
        """
        Set the kill signal received event
        """
        self._kill_signal_received.set()

    def kill_signal_received(self) -> bool:
        """
        :return: Whether a kill signal was received in one of the workers
        """
        return self._kill_signal_received.is_set()

    ################
    # Terminating & restarting
    ################

    def insert_poison_pill(self) -> None:
        """
        'Tell' the workers their job is done.
        """
        for worker_id in range(self.n_jobs):
            self.add_task(POISON_PILL, worker_id)

    def insert_non_lethal_poison_pill(self) -> None:
        """
        When ``keep_alive=True``, the workers should stay alive, but they need to wrap up their work (like sending the
        latest progress bar update)
        """
        for worker_id in range(self.n_jobs):
            self.add_task(NON_LETHAL_POISON_PILL, worker_id)

    def signal_worker_restart(self, worker_id: int) -> None:
        """
        Signal to the main process that this worker needs to be restarted

        :param worker_id: Worker ID
        """
        self._worker_done_array[worker_id] = True

    def get_worker_restarts(self) -> Generator[int, None, None]:
        """
        Obtain the worker IDs that need to be restarted.

        :return: Generator of worker IDs
        """
        return (worker_id for worker_id, restart_worker in enumerate(self._worker_done_array) if restart_worker)

    def reset_worker_restart(self, worker_id) -> None:
        """
        Worker has been restarted, reset signal.

        :param worker_id: Worker ID
        """
        self._worker_done_array[worker_id] = False

    def get_worker_dead_lock(self, worker_id: int) -> mp.Lock:
        """
        Returns the worker dead lock for a specific worker

        :param worker_id: Worker ID
        :return: Lock object
        """
        return self._workers_dead_locks[worker_id]

    def signal_worker_alive(self, worker_id: int) -> None:
        """
        Indicate that a worker is alive

        :param worker_id: Worker ID
        """
        self._workers_dead[worker_id].clear()

    def signal_worker_dead(self, worker_id: int) -> None:
        """
`       Indicate that a worker is dead

        :param worker_id: Worker ID
        """
        self._workers_dead[worker_id].set()

    def is_worker_alive(self, worker_id: int) -> bool:
        """
        Check whether the worker is alive

        :param worker_id: Worker ID
        :return: Whether the worker is alive
        """
        return not self._workers_dead[worker_id].is_set()

    def wait_for_dead_worker(self, worker_id: int, timeout=None) -> None:
        """
        Wait until a worker is dead

        :param worker_id: Worker ID
        :param timeout: How long to wait for a worker to be dead
        """
        self._workers_dead[worker_id].wait(timeout=timeout)

    def join_results_queues(self, keep_alive: bool = False) -> None:
        """
        Join results and exit results queues

        :param keep_alive: Whether to keep the queues alive
        """
        self._results_queue.join()
        [q.join() for q in self._exit_results_queues]
        if not keep_alive:
            self._results_queue.close()
            self._results_queue.join_thread()
            [q.close() for q in self._exit_results_queues]
            [q.join_thread() for q in self._exit_results_queues]

    def join_task_queues(self, keep_alive: bool = False) -> None:
        """
        Join task queues

        :param keep_alive: Whether to keep the queues alive
        """
        [q.join() for q in self._task_queues]
        if not keep_alive:
            [q.close() for q in self._task_queues]
            [q.join_thread() for q in self._task_queues]

    def join_exception_queue(self, keep_alive: bool = False) -> None:
        """
        Join exception queue

        :param keep_alive: Whether to keep the queues alive
        """
        self._exception_queue.join()
        if not keep_alive:
            self._exception_queue.close()
            self._exception_queue.join_thread()

    def drain_queues_terminate_worker(self, worker_id: int, dont_wait_event: threading.Event) -> None:
        """
        Drain the results queues without blocking. This is done when terminating workers, while they could still be busy
        putting something in the queues. This function will always be called from within a thread.

        :param worker_id: Worker ID
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

        # Get results from the exit results queue. If we got any, keep going and inform the other termination threads to
        # wait until this one's finished
        got_results = False
        try:
            self.get_exit_results(worker_id, timeout=None, block=False)
            dont_wait_event.clear()
            got_results = True
        except (queue.Empty, OSError):
            if got_results:
                dont_wait_event.set()

    def drain_queues(self) -> None:
        """
        Drain tasks, results, and exit results queues. Note that the exception queue doesn't need to be drained.
        This one is properly cleaned up in the exception handling class.
        """
        [self.drain_and_join_queue(q) for q in self._task_queues]
        self.drain_and_join_queue(self._results_queue)
        [self.drain_and_join_queue(q) for q in self._exit_results_queues]

    def drain_and_join_queue(self, q: mp.JoinableQueue, join: bool = True) -> None:
        """
        Drains a queue completely, such that it is joinable. If a timeout is reached, we give up and terminate. So far,
        I've only seen it happen when an exception is thrown when using spawn as start method, and even then it only
        happens once every 1000 runs or so.

        :param q: Queue to join
        :param join: Whether to join the queue or not
        """
        try:
            process = mp.Process(target=self._drain_and_join_queue, args=(q, join))
            process.start()
            process.join(timeout=5)
            if process.is_alive():
                logger.debug("Draining queue failed, skipping")
                process.terminate()
                process.join()
        except (OSError, RuntimeError):
            # For Windows compatibility
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
        except (OSError, EOFError, queue.Empty):
            pass

        # Join
        if join:
            try:
                q.join()
                q.close()
                q.join_thread()
            except OSError:
                pass

    ################
    # Timeouts
    ################

    def signal_worker_init_started(self, worker_id: int) -> None:
        """
        Sets the worker_init started timestamp for a specific worker

        :param worker_id: Worker ID
        """
        self._workers_time_task_started[worker_id * 3 + 0] = datetime.now().timestamp()

    def signal_worker_task_started(self, worker_id: int) -> None:
        """
        Sets the task started timestamp for a specific worker

        :param worker_id: Worker ID
        """
        self._workers_time_task_started[worker_id * 3 + 1] = datetime.now().timestamp()

    def signal_worker_exit_started(self, worker_id: int) -> None:
        """
        Sets the worker_exit started timestamp for a specific worker

        :param worker_id: Worker ID
        """
        self._workers_time_task_started[worker_id * 3 + 2] = datetime.now().timestamp()

    def signal_worker_init_completed(self, worker_id: int) -> None:
        """
        Resets the worker_init started timestamp for a specific worker

        :param worker_id: Worker ID
        """
        self._workers_time_task_started[worker_id * 3 + 0] = 0

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
        return self._has_worker_timed_out(self._workers_time_task_started[worker_id * 3 + 0], timeout)

    def has_worker_task_timed_out(self, worker_id: int, timeout: float) -> bool:
        """
        Checks whether a worker task takes longer than the timeout value

        :param worker_id: Worker ID
        :param timeout: Timeout in seconds
        :return: True when time has expired, False otherwise
        """
        return self._has_worker_timed_out(self._workers_time_task_started[worker_id * 3 + 1], timeout)

    def has_worker_exit_timed_out(self, worker_id: int, timeout: float) -> bool:
        """
        Checks whether a worker_exit takes longer than the timeout value

        :param worker_id: Worker ID
        :param timeout: Timeout in seconds
        :return: True when time has expired, False otherwise
        """
        return self._has_worker_timed_out(self._workers_time_task_started[worker_id * 3 + 2], timeout)

    @staticmethod
    def _has_worker_timed_out(started_time: float, timeout: float) -> bool:
        """
        Checks whether time has passed beyond the timeout

        :param started_time: Timestamp
        :param timeout: Timeout in seconds
        :return: True when time has expired, False otherwise
        """
        return False if started_time == 0.0 else (datetime.now().timestamp() - started_time) >= timeout
