from multiprocessing import JoinableQueue, Process
from typing import Any, Optional

from tqdm import tqdm

from mpire.signal import DisableKeyboardInterruptSignal


class ProgressBarHandler:

    def __init__(self, progress_bar: Optional[tqdm], task_completed_queue: JoinableQueue) -> None:
        """
        :param progress_bar: tqdm progress bar or None when no progress bar should be shown
        """
        self.progress_bar = progress_bar
        self.task_completed_queue = task_completed_queue
        self.process = None

    def __enter__(self) -> 'ProgressBarHandler':
        """
        Enables the use of the ``with`` statement. Starts a new progress handler process if a progress bar was provided

        :return: self
        """
        if self.progress_bar is not None:

            # Disable the interrupt signal. We let the process die gracefully
            with DisableKeyboardInterruptSignal():

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
        while True:
            # Wait for a job to finish
            task_completed = self.task_completed_queue.get(block=True)

            # If we received None, we should quit right away
            if task_completed is None:
                self.task_completed_queue.task_done()
                break

            # Update progress bar. Note that we also update a separate counter which is used to check if the progress
            # bar is completed. I realize that tqdm has a public variable `n` which should keep track of the current
            # progress, but for some reason that variable doesn't work here, it equals the `total` variable all the
            # time.
            self.progress_bar.update(1)
            self.task_completed_queue.task_done()

        # Force a final refresh
        self.progress_bar.refresh()
