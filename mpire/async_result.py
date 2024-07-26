import collections
import itertools
import queue
import threading
from typing import Any, Callable, Dict, List, Optional, Union


class JobCounter:
    """JobCounter is a simpel auto-incrementing counter for job ids."""
    
    def __init__(self) -> None:
        self.idx = 0
        
    def __next__(self) -> int:
        self.idx += 1
        return self.idx
    
    def last_job_id(self) -> Optional[int]:
        return None if self.idx == 0 else self.idx
    
    def next_job_id(self) -> int:
        return self.idx + 1
    

JOB_COUNTER = JobCounter()


class AsyncResult:
    """Adapted from ``multiprocessing.pool.ApplyResult``."""

    def __init__(
        self,
        cache: Dict,
        task_params_dict: Dict,
        callback: Optional[Callable],
        error_callback: Optional[Callable],
        job_id: Optional[int] = None,
        delete_from_cache: bool = True,
        timeout: Optional[float] = None,
    ) -> None:
        """
        :param cache: Cache for storing intermediate results
        :param task_params_dict: Dictionary with the parameters of the task
        :param callback: Callback function to call when the task is finished. The callback function receives the output
            of the function as its argument
        :param error_callback: Callback function to call when the task has failed. The callback function receives the
            exception as its argument
        :param job_id: Job ID of the task. If None, a new job ID is generated
        :param delete_from_cache: If True, the result is deleted from the cache when the task is finished
        :param timeout: Timeout in seconds for a single task. When the timeout is exceeded, MPIRE will raise a
            ``TimeoutError``. Use ``None`` to disable (default)
        """
        self._cache = cache
        self._task_params_dict = task_params_dict
        self._callback = callback
        self._error_callback = error_callback
        self._delete_from_cache = delete_from_cache
        self._timeout = timeout

        self.job_id = next(JOB_COUNTER) if job_id is None else job_id
        self._ready_event = threading.Event()
        self._success = None
        self._value = None
        if self.job_id in self._cache:
            raise ValueError(f"Job ID {job_id} already exists in cache")
        self._cache[self.job_id] = self

    def ready(self) -> bool:
        """
        :return: Returns True if the task is finished
        """
        return self._ready_event.is_set()

    def successful(self) -> bool:
        """
        :return: Returns True if the task has finished successfully
        :raises: ValueError if the task is not finished yet
        """
        if not self.ready():
            raise ValueError(f"{self.job_id} is not ready")
        return self._success

    def wait(self, timeout: Optional[float] = None) -> None:
        """
        Wait until the task is finished

        :param timeout: Timeout in seconds. If None, wait indefinitely
        """
        self._ready_event.wait(timeout)

    def get(self, timeout: Optional[float] = None) -> Any:
        """
        Wait until the task is finished and return the output of the function

        :param timeout: Timeout in seconds. If None, wait indefinitely
        :return: Output of the function
        :raises: TimeoutError if the task is not finished within the timeout. When the task has failed, the exception
            raised by the function is re-raised
        """
        self.wait(timeout)
        if not self.ready():
            raise TimeoutError
        if self._success:
            return self._value
        else:
            raise self._value

    def _set(self, success: bool, result: Any) -> None:
        """
        Set the result of the task and call any callbacks, when provided. This also removes the task from the cache, as
        it's no longer needed there. The user should store a reference to the result object

        :param success: True if the task has finished successfully
        :param result: Output of the function or the exception raised by the function
        """
        self._success = success
        self._value = result

        if self._callback and self._success:
            self._callback(self._value)

        if self._error_callback and not self._success:
            self._error_callback(self._value)

        self._ready_event.set()
        if self._delete_from_cache:
            del self._cache[self.job_id], self._task_params_dict[self.job_id]


class UnorderedAsyncResultIterator:
    """Stores results of a task and provides an iterator to obtain the results in an unordered fashion"""

    def __init__(
        self, cache: Dict, n_tasks: Optional[int], job_id: Optional[int] = None, timeout: Optional[float] = None
    ) -> None:
        """
        :param cache: Cache for storing intermediate results
        :param n_tasks: Number of tasks that will be executed. If None, we don't know the lenght yet
        :param job_id: Job ID of the task. If None, a new job ID is generated
        :param timeout: Timeout in seconds for a single task. When the timeout is exceeded, MPIRE will raise a
            ``TimeoutError``. Use ``None`` to disable (default)
        """
        self._cache = cache
        self._n_tasks = None
        self._timeout = timeout

        self.job_id = next(JOB_COUNTER) if job_id is None else job_id
        self._items = collections.deque()
        self._condition = threading.Condition(lock=threading.Lock())
        self._n_received = 0
        self._n_returned = 0
        self._exception = None
        self._got_exception = threading.Event()
        if self.job_id in self._cache:
            raise ValueError(f"Job ID {job_id} already exists in cache")
        self._cache[self.job_id] = self

        if n_tasks is not None:
            self.set_length(n_tasks)
            
        self.enable_prints = False

    def __iter__(self) -> "UnorderedAsyncResultIterator":
        return self

    def next(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        """
        Obtain the next unordered result for the task

        :param block: If True, wait until the next result is available. If False, raise queue.Empty if no result is
            available
        :param timeout: Timeout in seconds. If None, wait indefinitely
        :return: The next result
        """
        if self._items:
            self._n_returned += 1
            r = self._items.popleft()
            if self.enable_prints:
                print("Imap iterator: next returning (1):", str(r)[:500], self._n_tasks, self._n_returned)
            return r

        if self._n_tasks is not None and self._n_returned == self._n_tasks:
            if self.enable_prints:
                print("Imap iterator: raising StopIteration:", self._n_tasks, self._n_returned)
            raise StopIteration

        if not block:
            raise queue.Empty

        # We still expect results. Wait until the next result is available
        with self._condition:
            while not self._items:
                timed_out = not self._condition.wait(timeout=timeout)
                if timed_out:
                    raise queue.Empty
                if self._n_tasks is not None and self._n_returned == self._n_tasks:
                    raise StopIteration

            self._n_returned += 1
            r = self._items.popleft()
            if self.enable_prints:
                print("Imap iterator: next returning (2):", str(r)[:500], self._n_tasks, self._n_returned)
            return r

    __next__ = next
    
    def exception_thrown(self) -> bool:
        """
        :return: True if an exception was thrown during the task execution
        """
        return self._got_exception.is_set()

    def wait(self) -> None:
        """
        Wait until all results are available
        """
        with self._condition:
            while self._n_tasks is None or self._n_received < self._n_tasks:
                self._condition.wait()

    def _set(self, success: bool, result: Any) -> None:
        """
        Set the result of the task

        :param success: True if the task has finished successfully
        :param result: Output of the function or the exception raised by the function
        """
        if success:
            # Add the result to the queue and notify the iterator
            self._n_received += 1
            self._items.append(result)
            with self._condition:
                self._condition.notify()
        else:
            self._exception = result
            self._got_exception.set()

    def set_length(self, length: int) -> None:
        """
        Set the length of the iterator

        :param length: Length of the iterator
        """
        if self._n_tasks is not None:
            if self._n_tasks != length:
                raise ValueError(
                    f"Length of iterator has already been set to {self._n_tasks}, but is now set to {length}"
                )
            # Length has already been set. No need to do anything
            return

        with self._condition:
            self._n_tasks = length
            self._condition.notify()

    def get_exception(self) -> Exception:
        """
        :return: The exception raised by the function
        """
        self._got_exception.wait()
        return self._exception

    def remove_from_cache(self) -> None:
        """
        Remove the iterator from the cache
        """
        del self._cache[self.job_id]


class UnorderedAsyncExitResultIterator(UnorderedAsyncResultIterator):

    def __init__(self) -> None:
        super().__init__(cache={}, n_tasks=None, job_id=0, timeout=None)

    def get_results(self) -> List[Any]:
        """
        :return: List of exit results
        """
        if self.exception_thrown():
            raise self.get_exception()
        
        return list(self._items)


AsyncResultType = Union[AsyncResult, UnorderedAsyncResultIterator, UnorderedAsyncExitResultIterator]
