import logging
from threading import Thread
from typing import Any, Callable

from mpire.comms import POISON_PILL, WorkerComms

logger = logging.getLogger(__name__)


class StopWorker(Exception):
    """ Exception used to kill workers from the main process """
    pass


class CannotPickleExceptionError(Exception):
    """ Exception used when Pickle has trouble pickling the actual Exception """
    pass


class ExceptionHandler:

    def __init__(self, terminate: Callable, worker_comms: WorkerComms, has_progress_bar: bool) -> None:
        """
        :param terminate: terminate function of the WorkerPool
        :param worker_comms: Worker communication objects (queues, locks, events, ...)
        :param has_progress_bar: Whether or not a progress bar is active
        """
        self.terminate = terminate
        self.worker_comms = worker_comms
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
        if not self.worker_comms.exception_caught():
            self.worker_comms.add_exception_poison_pill()
        self.thread.join()

    def _exception_handler(self) -> None:
        """
        Keeps an eye on any exceptions being passed on by workers
        """
        logger.debug("Exception handler started")

        # Wait for an exception to occur
        err, traceback_str = self.worker_comms.get_exception(in_thread=True)

        # If we received a poison pill, we should just quit quietly
        if err is not POISON_PILL:
            # Let main process know we can stop working
            logger.debug(f"Exception caught: {err}")
            self.worker_comms.set_exception_caught()

            # Kill processes
            self.terminate()

            # Pass error to main process so it can be raised there (exceptions raised from threads or child processes
            # cannot be caught directly). Pass another error in case we have a progress bar
            self.worker_comms.add_exception(err, traceback_str)
            if self.has_progress_bar:
                self.worker_comms.add_exception(err, traceback_str)

        self.worker_comms.task_done_exception()
        logger.debug("Terminating exception handler")

    def raise_on_exception(self) -> None:
        """
        Raise error when we have caught one
        """
        # If we know an exception will come through from a worker process, wait for the exception to be obtained in the
        # main process
        if self.worker_comms.exception_thrown():
            self.worker_comms.wait_until_exception_is_caught()

        if self.worker_comms.exception_caught():
            # Clear keep order event so we can safely reuse the WorkerPool and use (i)map_unordered after an (i)map call
            self.worker_comms.clear_keep_order()

            # Get exception and raise here
            err, traceback_str = self.worker_comms.get_exception()
            self.worker_comms.task_done_exception()
            logger.debug("Raising caught exception")
            raise err(traceback_str)
