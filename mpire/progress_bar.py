import threading
import traceback
from datetime import datetime, timedelta
from threading import Event, Thread
from typing import Any, Dict, Optional, Type
import warnings

from tqdm import TqdmExperimentalWarning, tqdm as tqdm_type

from mpire.comms import WorkerComms, POISON_PILL
from mpire.exception import remove_highlighting
from mpire.insights import WorkerInsights
from mpire.params import WorkerMapParams, WorkerPoolParams
from mpire.signal import DisableKeyboardInterruptSignal
from mpire.tqdm_utils import get_tqdm, TqdmManager
from mpire.utils import format_seconds

# If a user has not installed the dashboard dependencies than the imports below will fail
try:
    from mpire.dashboard.dashboard import DASHBOARD_STARTED_EVENT
    from mpire.dashboard.utils import get_function_details
    from mpire.dashboard.manager import get_manager_client_dicts
except ImportError:
    DASHBOARD_STARTED_EVENT = None

    def get_function_details(_):
        pass

    def get_manager_client_dicts():
        raise NotImplementedError

DATETIME_FORMAT = "%Y-%m-%d, %H:%M:%S"


class ProgressBarHandler:

    def __init__(self, pool_params: WorkerPoolParams, map_params: WorkerMapParams, show_progress_bar: bool,
                 progress_bar_options: Dict[str, Any], progress_bar_style: Optional[str], worker_comms: WorkerComms,
                 worker_insights: WorkerInsights) -> None:
        """
        :param pool_params: WorkerPool parameters
        :param map_params: Map parameters
        :param show_progress_bar: When ``True`` will display a progress bar
        :param progress_bar_options: Dictionary containing keyword arguments to pass to the ``tqdm`` progress bar. See
         ``tqdm.tqdm()`` for details.
        :param progress_bar_style: The progress bar style to use
        :param worker_comms: Worker communication objects (queues, locks, events, ...)
        :param worker_insights: WorkerInsights object which stores the worker insights
        """
        self.show_progress_bar = show_progress_bar
        self.progress_bar_options = progress_bar_options
        self.progress_bar_style = progress_bar_style
        self.worker_comms = worker_comms
        self.worker_insights = worker_insights
        if show_progress_bar and DASHBOARD_STARTED_EVENT is not None and DASHBOARD_STARTED_EVENT.is_set():
            self.function_details = get_function_details(map_params.func)
            self.function_details['n_jobs'] = pool_params.n_jobs
        else:
            self.function_details = None

        self.thread = None
        self.thread_started = Event()
        self.progress_bar_id = None
        self.total = None
        self.total_updated = Event()
        self.exception_traceback_str = None
        self.exception_traceback_str_set_condition = threading.Condition(lock=threading.Lock())
        self.dashboard_dict = None
        self.dashboard_details_dict = None
        self.start_t = None

    def __enter__(self) -> 'ProgressBarHandler':
        """
        Enables the use of the ``with`` statement. Starts a new progress handler thread if a progress bar should be
        shown

        :return: self
        """
        if self.show_progress_bar:

            # Disable the interrupt signal. We let the thread die gracefully
            with DisableKeyboardInterruptSignal():
                self.thread = Thread(target=self._progress_bar_handler)
                self.thread.start()
                self.thread_started.wait()

        return self

    def __exit__(self, exc_type: Type, *_) -> None:
        """
        Enables the use of the ``with`` statement. Terminates the progress handler thread if there is one
        """
        if self.show_progress_bar and self.thread.is_alive():

            # If this exit is called with an exception, then we assume an external kill signal was received (this is,
            # for example, necessary in nested pools when an error occurs)
            if exc_type is not None:
                self.worker_comms.signal_kill_signal_received()

            # Signal shutdown and close the handling thread
            if not self.worker_comms.exception_thrown():
                self.worker_comms.signal_progress_bar_shutdown()
            self.thread.join()

    def _progress_bar_handler(self) -> None:
        """
        Keeps track of the progress made by the workers and updates the progress bar accordingly
        """
        # Obtain the progress bar tqdm class
        tqdm = get_tqdm(self.progress_bar_style)

        # Connect to the tqdm manager
        tqdm_manager = TqdmManager()
        tqdm_lock, tqdm_position_register = tqdm_manager.get_connection_details()
        tqdm.set_lock(tqdm_lock)
        tqdm.set_main_progress_bar(
            tqdm_position_register.register_progress_bar_position(self.progress_bar_options["position"])
        )

        # Create progress bar and register the start time. Ignore the experimental warning for rich progress bars
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", TqdmExperimentalWarning)
            tqdm.monitor_interval = False
            progress_bar = tqdm(**self.progress_bar_options)
        self.start_t = datetime.fromtimestamp(progress_bar.start_t)

        # Notify that the main process can continue working. We set it after the progress bar has been created, instead
        # of right after this thread has started, for a better user experience
        self.thread_started.set()

        # Register progress bar to dashboard in case a dashboard is started
        self._register_progress_bar(progress_bar)

        while True:
            # Wait for a job to finish
            tasks_completed = self.worker_comms.get_tasks_completed_progress_bar()

            # If we received a poison pill, we should quit right away. We do force a final refresh of the progress bar
            # to show the latest status
            if tasks_completed is POISON_PILL:
                # Check if we got a poison pill because there was an error. If so, we obtain the exception information
                # and send it to the dashboard, if available.)
                if self.worker_comms.exception_thrown() or self.worker_comms.kill_signal_received():
                    progress_bar.set_description('Exception occurred, terminating ... ')
                    if self.worker_comms.exception_thrown():
                        # Wait for exception traceback str to be set
                        with self.exception_traceback_str_set_condition:
                            if self.exception_traceback_str is None:
                                self.exception_traceback_str_set_condition.wait()
                        self._send_dashboard_update(progress_bar, failed=True,
                                                    traceback_str=self.exception_traceback_str)
                    elif self.worker_comms.kill_signal_received():
                        self._send_dashboard_update(progress_bar, failed=True, traceback_str='Kill signal received')

                # Final update of the progress bar
                progress_bar.final_refresh(tqdm_position_register.get_highest_progress_bar_position())
                break

            # Check if the total has been updated. It could be that we didn't know the total number of tasks at the
            # beginning, but we do now.
            if self.total_updated.is_set():
                progress_bar.update_total(self.total)
                self._send_dashboard_update(progress_bar)
                self.total_updated.clear()

            # Check if there's an actual update
            if tasks_completed > 0 and tasks_completed == progress_bar.n:
                continue

            # Update progress bar
            progress_bar.update(tasks_completed - progress_bar.n)
            if progress_bar.n == progress_bar.total:
                self.worker_comms.signal_progress_bar_complete()
                self.worker_comms.wait_until_progress_bar_is_complete()
                self._send_dashboard_update(progress_bar)

            # Send update to dashboard in case a dashboard is started, but only when tqdm updated its view as well. This
            # will make the dashboard a lot more responsive
            if progress_bar.n == progress_bar.last_print_n:
                self._send_dashboard_update(progress_bar)

    def _register_progress_bar(self, progress_bar: tqdm_type) -> None:
        """
        Register this progress bar to the dashboard

        :param progress_bar: tqdm progress bar instance
        """
        if self.progress_bar_id is None and DASHBOARD_STARTED_EVENT is not None and DASHBOARD_STARTED_EVENT.is_set():
            # Connect to manager server
            self.dashboard_dict, self.dashboard_details_dict, dashboard_tqdm_lock = get_manager_client_dicts()

            # Register new progress bar
            dashboard_tqdm_lock.acquire()
            self.progress_bar_id = len(self.dashboard_dict.keys()) + 1
            self.dashboard_details_dict.update([(self.progress_bar_id, self.function_details)])
            self._send_dashboard_update(progress_bar)
            dashboard_tqdm_lock.release()

    def _send_dashboard_update(self, progress_bar: tqdm_type, failed: bool = False,
                               traceback_str: Optional[str] = None) -> None:
        """
        Adds a progress bar update to the shared dict so the dashboard process can use it, only when a dashboard has
        started

        :param progress_bar: tqdm progress bar instance
        :param failed: Whether or not the operation failed or not
        :param traceback_str: Traceback string, if an exception was raised
        """
        if self.progress_bar_id is not None:
            self.dashboard_dict.update([(self.progress_bar_id,
                                         self._get_progress_bar_update_dict(progress_bar, failed, traceback_str))])

    def _get_progress_bar_update_dict(self, progress_bar: tqdm_type, failed: bool,
                                      traceback_str: Optional[str] = None) -> Dict[str, Any]:
        """
        Obtain update dictionary with all the information needed for displaying on the dashboard

        :param progress_bar: tqdm progress bar instance
        :param failed: Whether or not the operation failed or not
        :param traceback_str: Traceback string, if an exception was raised
        :return: Update dictionary
        """
        # Save some variables first so we can use them consistently with the same value
        details = progress_bar.format_dict
        n = details["n"]
        total = details["total"]
        now = datetime.now()
        rate = details["rate"] if details["rate"] else n / details["elapsed"] if details["elapsed"] else None
        remaining_time = (total - n) / rate if total and rate else None

        return {"id": self.progress_bar_id,
                "success": not failed,
                "n": n,
                "total": total,
                "percentage": n / total if total else None,
                "duration": str(now - self.start_t).rsplit('.', 1)[0],
                "remaining": format_seconds(remaining_time, False),
                "started_raw": self.start_t,
                "started": self.start_t.strftime(DATETIME_FORMAT),
                "finished_raw": now + timedelta(seconds=remaining_time) if remaining_time is not None else None,
                "finished": ((now + timedelta(seconds=remaining_time)).strftime(DATETIME_FORMAT)
                             if remaining_time is not None else ''),
                "traceback": traceback_str.strip() if traceback_str is not None else None,
                "insights": self.worker_insights.get_insights()}

    def set_new_total(self, total: int) -> None:
        """
        Set a new total for the progress bar

        :param total: New total
        """
        self.total = total
        self.total_updated.set()

    def set_exception(self, traceback_err: Exception) -> None:
        """
        Set the exception traceback string and notify the progress bar handler that it's ready

        :param traceback_err: Traceback error
        """
        if traceback_err.__cause__ is not None:
            traceback_str = "".join(traceback.format_tb(traceback_err.__traceback__))
        else:
            traceback_str = str(traceback_err)

        with self.exception_traceback_str_set_condition:
            self.exception_traceback_str = remove_highlighting(traceback_str)
            self.exception_traceback_str_set_condition.notify()
