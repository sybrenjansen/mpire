from datetime import datetime, timedelta
from multiprocessing import JoinableQueue, Process
from typing import Any, Callable, Dict, Optional

from tqdm import tqdm

from mpire.dashboard.manager import get_manager_client_dicts
from mpire.dashboard.utils import get_function_details
from mpire.signal import DisableSignal

# If a user has not installed the dashboard dependencies than the imports below will fail
try:
    from mpire.dashboard.dashboard import DASHBOARD_STARTED_EVENT
except ImportError:
    DASHBOARD_STARTED_EVENT = None

DATETIME_FORMAT = "%Y-%m-%d, %H:%M:%S"


class ProgressBarHandler:

    def __init__(self, func_pointer: Callable, progress_bar: Optional[tqdm], task_completed_queue: JoinableQueue) \
            -> None:
        """
        :param func_pointer: Function pointer passed on to a WorkerPool map function
        :param progress_bar: tqdm progress bar or None when no progress bar should be shown
        :param task_completed_queue: Queue related to the progress bar. Child processes can pass on a random value
            whenever they are finished with a job
        """
        self.progress_bar = progress_bar
        self.task_completed_queue = task_completed_queue
        self.function_details = get_function_details(func_pointer) if progress_bar else None

        self.process = None
        self.progress_bar_id = None
        self.dashboard_dict = None
        self.dashboard_details_dict = None
        self.start_t = datetime.fromtimestamp(progress_bar.start_t) if progress_bar else None

    def __enter__(self) -> 'ProgressBarHandler':
        """
        Enables the use of the ``with`` statement. Starts a new progress handler process if a progress bar was provided

        :return: self
        """
        if self.progress_bar is not None:

            # Disable the interrupt signal. We let the process die gracefully
            with DisableSignal():

                # We start a new process because updating the progress bar in a thread can slow down processing of
                # results and can fail to show real-time updates
                self.process = Process(target=self._progress_bar_handler)
                self.process.start()

        return self

    def __exit__(self, *_: Any) -> None:
        """
        Enables the use of the ``with`` statement. Terminates the progress handler process if there is one
        """
        if self.progress_bar is not None:

            # Insert poison pill and close the progress bar and its handling process
            self.task_completed_queue.put(None)
            self.process.join()
            self.progress_bar.close()

    def _progress_bar_handler(self) -> None:
        """
        Keeps track of the progress made by the workers and updates the progress bar accordingly
        """
        # Register progress bar to dashboard in case a dashboard is started
        self._register_progress_bar()

        while True:
            # Wait for a job to finish
            task_completed = self.task_completed_queue.get(block=True)

            # If we received None, we should quit right away. We do force a final refresh of the progress bar to show
            # the latest status
            if task_completed is None:
                self.task_completed_queue.task_done()
                self.progress_bar.refresh()

                # If, at this point, the progress bar is not at 100% it means we had a failure. We send the failure to
                # the dashboard in the case a dashboard is started
                if self.progress_bar.n != self.progress_bar.total:
                    self._send_update(failed=True)

                break

            # Update progress bar. Note that we also update a separate counter which is used to check if the progress
            # bar is completed. I realize that tqdm has a public variable `n` which should keep track of the current
            # progress, but for some reason that variable doesn't work here, it equals the `total` variable all the
            # time.
            self.progress_bar.update(1)
            self.task_completed_queue.task_done()

            # Force a refresh when we're at 100%. Tqdm doesn't always show the last update. It does when we close the
            # progress bar, but because that happens in the main process it won't show it properly (tqdm and pickle
            # don't like eachother that much)
            if self.progress_bar.n == self.progress_bar.total:
                self.progress_bar.refresh()

            # Register progress bar to dashboard in case a dashboard is started after the progress bar was created
            self._register_progress_bar()

            # Send update to dashboard in case a dashboard is started
            self._send_update()

    def _register_progress_bar(self) -> None:
        """
        Register this progress bar to the dashboard
        """
        if self.progress_bar_id is None and DASHBOARD_STARTED_EVENT is not None and DASHBOARD_STARTED_EVENT.is_set():

            # Import only when needed
            self.dashboard_dict, self.dashboard_details_dict, dashboard_tqdm_lock = get_manager_client_dicts()

            # Register new progress bar
            dashboard_tqdm_lock.acquire()
            self.progress_bar_id = len(self.dashboard_dict.keys()) + 1
            self.dashboard_details_dict.update([(self.progress_bar_id, self.function_details)])
            self._send_update()
            dashboard_tqdm_lock.release()

    def _send_update(self, failed: bool = False) -> None:
        """
        Adds a progress bar update to the shared dict so the dashboard process can use it, only when a dashboard has
        started

        :param failed: whether or not the operation failed or not
        """
        if self.progress_bar_id is not None:
            self.dashboard_dict.update([(self.progress_bar_id, self._get_progress_bar_update_dict(failed))])

    def _get_progress_bar_update_dict(self, failed: bool) -> Dict[str, Any]:
        """
        Obtain update dictionary with all the information needed for displaying on the dashboard

        :param failed: whether or not the operation failed or not
        :return: update dictionary
        """
        # Save some variables first so we can use them consistently with the same value
        n = self.progress_bar.n
        total = self.progress_bar.total
        avg_time = self.progress_bar.avg_time
        now = datetime.now()
        remaining_time = ((total - n) * avg_time) if avg_time else None

        return {"id": self.progress_bar_id,
                "success": not failed,
                "n": n,
                "total": total,
                "percentage": n / total,
                "duration": str(now - self.start_t).rsplit('.', 1)[0],
                "remaining": (str(timedelta(seconds=remaining_time)).rsplit('.', 1)[0]
                              if remaining_time is not None else '-'),
                "started_raw": self.start_t,
                "started": self.start_t.strftime(DATETIME_FORMAT),
                "finished_raw": now + timedelta(seconds=remaining_time) if remaining_time is not None else None,
                "finished": ((now + timedelta(seconds=remaining_time)).strftime(DATETIME_FORMAT)
                             if remaining_time is not None else '-')}
