import multiprocessing as mp
import queue
from typing import Any, Generator, Optional, Tuple, Union

from mpire.signal import DelayedKeyboardInterrupt

POISON_PILL = '\0'


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
        self._tasks_queue = None

        # Queue where the child processes can pass on results
        self._results_queue = None

        # Queue where the child processes can store exit results in
        self._exit_results_queues = []

        # Array where the child processes can request a restart
        self._worker_done_array = None

        # Queue related to the progress bar. Child processes signal whenever they are finished with a task
        self._task_completed_queue = None

        # Queue where the child processes can pass on an encountered exception
        self._exception_queue = None

        # Lock object such that child processes can only throw one at a time. The Event object ensures only one
        # exception can be thrown
        self.exception_lock = self.ctx.Lock()
        self._exception_thrown = self.ctx.Event()

        # Whether or not an exception was caught by one of the child processes
        self._exception_caught = self.ctx.Event()

        # Worker ID
        self.worker_id = None

    ################
    # Initialization
    ################

    def init_comms(self, has_worker_exit: bool, has_progress_bar: bool) -> None:
        """
        Initialize/Reset comms containers

        :param has_worker_exit: Whether there's a worker_exit function provided
        :param has_progress_bar: Whether there's a progress bar
        """
        self._tasks_queue = self.ctx.JoinableQueue()
        self._results_queue = self.ctx.JoinableQueue()
        self._exit_results_queues = [self.ctx.JoinableQueue()
                                     for _ in range(self.n_jobs)] if has_worker_exit else []
        self._worker_done_array = self.ctx.Array('b', self.n_jobs, lock=False)
        self._exception_queue = self.ctx.JoinableQueue()
        self._exception_thrown.clear()
        self._exception_caught.clear()
        self._task_completed_queue = self.ctx.JoinableQueue() if has_progress_bar else None
        self.worker_id = None

    def init_worker(self, worker_id: int) -> None:
        """
        :param worker_id: Worker ID
        """
        self.worker_id = worker_id

    ################
    # Progress bar
    ################

    def has_progress_bar(self) -> bool:
        """
        :return: Whether we have a progress bar
        """
        return self._task_completed_queue is not None

    def task_completed_progress_bar(self) -> None:
        """
        Signal that we've completed a task, for the progress bar
        """
        self._task_completed_queue.put(1)

    def add_progress_bar_poison_pill(self) -> None:
        """
        Signals the progress bar handling process to shut down
        """
        self._task_completed_queue.put(POISON_PILL)

    def get_tasks_completed_progress_bar(self) -> Tuple[Union[int, str], bool]:
        """
        Sometimes, due to super small user-provided functions, this queue can get rather crowded. When an exception
        occurs in such a function after a while, this queue.get() can get deadlocked. So, we set a timeout and
        occasionally check for errors.

        :return: Tuple containing the number of tasks done or a poison pill, and a boolean indicating if the result
            came from the queue or not
        """
        while not self.exception_caught():
            try:
                return self._task_completed_queue.get(block=True, timeout=0.1), True
            except queue.Empty:
                pass

        return POISON_PILL, False

    def task_done_progress_bar(self) -> None:
        """
        Signal that we've completed a task for the progress bar
        """
        self._task_completed_queue.task_done()

    ################
    # Order modifiers
    ################

    def set_keep_order(self) -> None:
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

    def add_task(self, task: Any) -> None:
        """
        Add a task to the queue so a worker can process it.

        :param task: A tuple of arguments to pass to a worker, which acts upon it
        """
        with DelayedKeyboardInterrupt():
            self._tasks_queue.put(task, block=True)

    def get_task(self) -> Any:
        """
        Obtain new chunk of tasks. Occasionally we check if an exception has been thrown. If so, we should quit.

        :return: Chunk of tasks or None when an exception was thrown
        """
        while not self.exception_thrown():
            try:
                return self._tasks_queue.get(block=True, timeout=0.1)
            except queue.Empty:
                pass
        return None

    def task_done(self) -> None:
        """
        Signal that we've completed a task
        """
        self._tasks_queue.task_done()

    def add_results(self, results: Any) -> None:
        """
        :param results: Results from the main function
        """
        self._results_queue.put(results)

    def get_results(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        """
        Obtain the next result from the results queue.

        :param block: Whether to block (wait for results)
        :param timeout: How long to wait for results in case ``block==True``
        :return: The next result from the queue, which is the result of calling the function
        """
        with DelayedKeyboardInterrupt():
            results = self._results_queue.get(block=block, timeout=timeout)
            self._results_queue.task_done()
        return results

    def add_exit_results(self, results: Any) -> None:
        """
        :param results: Results from the exit function
        """
        self._exit_results_queues[self.worker_id].put(results)

    def get_exit_results(self, worker_id: int, timeout: Optional[float] = None) -> Any:
        """
        Obtain exit results from a specific worker.

        :param worker_id: Worker ID
        :param timeout: How long to wait for results
        :return: Exit results
        """
        results = self._exit_results_queues[worker_id].get(block=True, timeout=timeout)
        self._exit_results_queues[worker_id].task_done()
        return results

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
        self._exception_thrown.set()

    def add_exception_poison_pill(self) -> None:
        """
        Signals the exception handling thread to shut down
        """
        with DelayedKeyboardInterrupt():
            self._exception_queue.put((POISON_PILL, POISON_PILL))

    def get_exception(self, in_thread: bool = False) -> Tuple[type, str]:
        """
        :param in_thread: Whether this function is called from within a thrad
        :return: Tuple containing the type of the exception and the traceback string
        """
        with DelayedKeyboardInterrupt(in_thread):
            return self._exception_queue.get(block=True)

    def task_done_exception(self) -> None:
        """
        Signal that we've completed a task for the exception queue
        """
        self._exception_queue.task_done()

    def set_exception(self) -> None:
        """
        Set the exception event
        """
        self._exception_thrown.set()

    def exception_thrown(self) -> bool:
        """
        :return: Whether an exception was thrown by one of the workers
        """
        return self._exception_thrown.is_set()

    def wait_until_exception_is_caught(self) -> None:
        """
        Wait until the exception is caught by the main process
        """
        self._exception_caught.wait()

    def exception_caught(self) -> bool:
        """
        :return: Whether an exception was caught by the main process
        """
        return self._exception_caught.is_set()

    def set_exception_caught(self) -> None:
        """
        Signal that an exception was caught by the main process
        """
        return self._exception_caught.set()

    ################
    # Terminating & restarting
    ################

    def insert_poison_pill(self) -> None:
        """
        'Tell' the workers their job is done.
        """
        for _ in range(self.n_jobs):
            self.add_task(POISON_PILL)

    def signal_worker_restart(self) -> None:
        """
        Signal to the main process that this worker needs to be restarted
        """
        self._worker_done_array[self.worker_id] = True

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

    def join_results_queues(self) -> None:
        """
        Join results and exit results queues
        """
        self._results_queue.join()
        [q.join() for q in self._exit_results_queues]

    def join_tasks_queue(self) -> None:
        """
        Join tasks queue
        """
        self._tasks_queue.join()

    def join_progress_bar_task_completed_queue(self) -> None:
        """
        Join progress bar tasks completed queue
        """
        self._task_completed_queue.join()

    def join_exception_queue(self) -> None:
        """
        Join exception queue
        """
        self._exception_queue.join()

    def drain_queues(self) -> None:
        """
        Drain tasks, results, and exit results queues. Note that the progress bar tasks_done and exception queues never
        have to be drained. They are properly cleaned up in the respective __exit__ functions.
        """
        self._drain_and_join_queue(self._tasks_queue, block=True)
        self._drain_and_join_queue(self._results_queue, block=False)
        [self._drain_and_join_queue(q) for q in self._exit_results_queues]

    @staticmethod
    def _drain_and_join_queue(q: mp.JoinableQueue, join: bool = True, block: bool = False) -> None:
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
        except ValueError:
            pass

        try:
            while n != 0:
                q.get(block=block)
                n -= 1
        except (queue.Empty, EOFError):
            pass

        # Join
        if join:
            q.join()
