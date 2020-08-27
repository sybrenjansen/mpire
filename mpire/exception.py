from multiprocessing import JoinableQueue
from threading import Event, Thread
from typing import Any, Callable

from mpire.signal import DelayedKeyboardInterrupt


class StopWorker(Exception):
    """ Exception used to kill workers from the main process """
    pass


class CannotPickleExceptionError(Exception):
    """ Exception used when Pickle has trouble pickling the actual Exception """
    pass


class ExceptionHandler:

    def __init__(self, terminate: Callable, exception_queue: JoinableQueue, exception_caught: Event, keep_order: Event,
                 has_progress_bar: bool) -> None:
        """
        :param terminate: terminate function of the WorkerPool
        :param exception_queue: Queue where the workers can pass on an encountered exception
        :param exception_caught: Event indicating whether or not an exception was caught by one of the workers
        :param keep_order: Event that we need to clear in case of an exception
        :param has_progress_bar: Whether or not a progress bar is active
        """
        self.terminate = terminate
        self.exception_queue = exception_queue
        self.exception_caught = exception_caught
        self.keep_order = keep_order
        self.has_progress_bar = has_progress_bar
        self.thread = None

    def __enter__(self) -> 'ExceptionHandler':
        """
        Enables the use of the ``with`` statement. Starts a new exception handling thread

        :return: self
        """
        # Start a thread that handles exceptions. We start a thread, and not a process, because we need to be able to
        # raise an exception from here such that the main process will shutdown
        self.thread = Thread(target=self._exception_handler)
        self.thread.start()

        return self

    def __exit__(self, *_: Any) -> None:
        """
        Enables the use of the ``with`` statement. Terminates the exception handling thread
        """
        # Shutdown the exception handling thread. Insert a poison pill when no exception was raised by the workers.
        # If there was an exception, the exception handling thread would already be joinable.
        if not self.exception_caught.is_set():
            with DelayedKeyboardInterrupt():
                self.exception_queue.put((None, None), block=True)
        self.thread.join()

    def _exception_handler(self) -> None:
        """
        Keeps an eye on any exceptions being passed on by workers
        """
        # Wait for an exception to occur
        err, traceback_str = self.exception_queue.get(block=True)

        # If we received None, we should just quit quietly
        if err is not None:

            # Let main process know we can stop working
            self.exception_caught.set()

            # Kill processes
            self.terminate()

            # Pass error to main process so it can be raised there (exceptions raised from threads or child processes
            # cannot be caught directly). Pass another error in case we have a progress bar
            self.exception_queue.put((err, traceback_str), block=True)
            if self.has_progress_bar:
                self.exception_queue.put((err, traceback_str), block=True)

        self.exception_queue.task_done()

    def raise_on_exception(self) -> None:
        """
        Raise error when we have caught one
        """
        if self.exception_caught.is_set():

            # Clear keep order event so we can safely reuse the WorkerPool and use (i)map_unordered after an (i)map call
            self.keep_order.clear()

            with DelayedKeyboardInterrupt():
                err, traceback_str = self.exception_queue.get(block=True)
                self.exception_queue.task_done()

            raise err(traceback_str)
