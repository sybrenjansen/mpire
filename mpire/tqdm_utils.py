import logging
import warnings
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from multiprocessing import Lock as mp_Lock
from multiprocessing.synchronize import Lock as LockType
from typing import Optional, Tuple, Type

from tqdm import TqdmExperimentalWarning, tqdm as tqdm_std
from tqdm.notebook import tqdm as tqdm_notebook
try:
    from tqdm.rich import tqdm as tqdm_rich
    RICH_AVAILABLE = True
except ImportError:
    tqdm_rich = None
    RICH_AVAILABLE = False

from mpire.context import mp_dill
from mpire.signal import DisableKeyboardInterruptSignal
from mpire.utils import create_sync_manager

PROGRESS_BAR_DEFAULT_STYLE = 'std'
TqdmConnectionDetails = Tuple[LockType, "TqdmPositionRegister"]

logger = logging.getLogger(__name__)


class TqdmMpire:
    """ Abstract class for tqdm classes that are used in mpire"""

    main_progress_bar = False

    @classmethod
    def set_main_progress_bar(cls, main: bool) -> None:
        """
        Marks this progress bar as the main progress bar

        :param main: Whether this progress bar is the main progress bar
        """
        cls.main_progress_bar = main

    def update(self, n: int = 1) -> None:
        """
        Update the progress bar. Forces a final refresh when the progress bar is finished.

        :param n: Number of steps to update the progress bar with
        """
        super().update(n)
        if self.n == self.total:
            self.final_refresh()

    def update_total(self, total: int) -> None:
        """
        Update the total number of steps of the progress bar. Forces a refresh to show the new total.

        :param total: Total number of steps
        """
        self.total = total
        self.refresh()

    def final_refresh(self, highest_progress_bar_position: Optional[int] = None) -> None:
        """
        Final refresh of the progress bar. This function is called when the progress bar is finished. It should
        perform a final refresh of the progress bar and close it.

        :param highest_progress_bar_position: Highest progress bar position in case of multiple progress bars
        """
        self.refresh()
        self.close()

    @classmethod
    def check_options(cls, options: dict) -> None:
        """
        Check whether the options passed to the tqdm class are valid. This function should raise an exception when the
        options are invalid.

        :param options: Options passed to the tqdm class
        """
        with redirect_stderr(StringIO()), redirect_stdout(StringIO()):
            cls(**options)


class TqdmMpireStd(TqdmMpire, tqdm_std):
    """ A tqdm class that shows a standard progress bar. """

    def final_refresh(self, highest_progress_bar_position: Optional[int] = None) -> None:
        """
        Final refresh of the progress bar. This function is called when the progress bar is finished. It should
        perform a final refresh.

        When we're using a standard progress bar and this is the main progress bar, we add as many newlines as the
        highest progress bar position, such that new output is added after the progress bars.

        :param highest_progress_bar_position: Highest progress bar position in case of multiple progress bars
        """
        self.refresh()
        self.disable = True
        if self.main_progress_bar and highest_progress_bar_position is not None:
            self.fp.write('\n' * (highest_progress_bar_position + 1))


if RICH_AVAILABLE:
    class TqdmMpireRich(TqdmMpire, tqdm_rich):
        """ A tqdm class that shows a rich progress bar. """

        @classmethod
        def check_options(cls, options: dict) -> None:
            """
            Check whether the options passed to the tqdm class are valid. This function should raise an exception when the
            options are invalid.

            For rich progress bars we disable the progress bar, because we don't want to show the progress bar in the
            terminal. For some reason, redirecting stdout/stderr makes the rich progress bar not work properly afterwards.

            :param options: Options passed to the tqdm class
            """
            options = options.copy()
            if "options" not in options:
                options["options"] = {"disable": True}
            else:
                options["options"]["disable"] = True
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", TqdmExperimentalWarning)
                cls(**options)

        def display(self, *args, **kwargs) -> None:
            """
            Display the progress bar and force a refresh of the widget. The refresh is needed to show the final update.
            """
            super().display(*args, **kwargs)
            self._prog.refresh()

else:
    class TqdmMpireRich(TqdmMpire):
        
        def __init__(self, *args, **kwargs) -> None:
            raise ImportError("rich is not installed. Please install rich to use rich progress bars.")


class TqdmMpireNotebook(TqdmMpire, tqdm_notebook):
    """ A tqdm class that shows a GUI widget in notebooks. """

    def __init__(self, *args, **kwargs) -> None:
        """
        In case we're running tqdm in a notebook we need to apply a dirty hack to get progress bars working.
        Solution adapted from https://github.com/tqdm/tqdm/issues/485#issuecomment-473338308
        """
        if not self.main_progress_bar:
            print(' ', end='', flush=True)
        super().__init__(*args, **kwargs)

    def update_total(self, total: int) -> None:
        """
        Update the total number of steps of the progress bar. Forces a refresh to show the new total.

        In a notebook we also need to update the max value of the progress bar widget.

        :param total: Total number of steps
        """
        self.container.children[1].max = total
        return super().update_total(total)

    @classmethod
    def check_options(cls, options: dict) -> None:
        """
        Check whether the options passed to the tqdm class are valid. This function should raise an exception when the
        options are invalid.

        For notebook progress bars we set display to false, because redirecting stdout/stderr doesn't work for notebook
        widgets.

        :param options: Options passed to the tqdm class
        """
        options = options.copy()
        options["display"] = False
        cls(**options)


class TqdmMpireDashboardOnly(TqdmMpire, tqdm_std):
    """
    A tqdm class that gives no output, but will still update the internal progress-bar attributes that the
    dashboard relies on.
    """

    def __init__(self, *args, **kwargs) -> None:
        """ Set the file to a StringIO object so that no output is given """
        kwargs["file"] = StringIO()
        super().__init__(*args, **kwargs)

    def display(self, *args, **kwargs) -> None:
        """ Don't display anything """
        pass


def get_tqdm(progress_bar_style: Optional[str]) -> Type[TqdmMpire]:
    """
    Get the tqdm class to use based on the progress bar style

    :param progress_bar_style: The progress bar style to use. Can be one of ``None``, ``std``, or ``notebook``
    :return: A tuple containing the tqdm class to use and a boolean indicating whether the progress bar is a notebook
        widget
    """
    if progress_bar_style is None:
        progress_bar_style = PROGRESS_BAR_DEFAULT_STYLE
    if progress_bar_style == 'std':
        return TqdmMpireStd
    elif progress_bar_style == 'rich':
        return TqdmMpireRich
    elif progress_bar_style == 'notebook':
        return TqdmMpireNotebook
    elif progress_bar_style == 'dashboard':
        return TqdmMpireDashboardOnly
    else:
        raise ValueError(f'Invalid progress bar style: {progress_bar_style}. '
                         f'Use either None (=default), "std", or "notebook"')


class TqdmPositionRegister:

    """
    Class that keeps track of all the registered progress bar positions. Needed to properly display multiple tqdm
    progress bars
    """

    def __init__(self, use_dill: bool) -> None:
        """
        :param use_dill: Whether dill is used as serialization library
        """
        self.lock = mp_dill.Lock() if use_dill else mp_Lock()
        self.highest_position = None

    def register_progress_bar_position(self, position: int) -> bool:
        """
        Register new progress bar position. Returns True when it's the first one to register

        :param position: Progress bar position
        :return: Whether this progress bar is the first one to register
        """
        with self.lock:
            first_one = self.highest_position is None
            if self.highest_position is None or position > self.highest_position:
                self.highest_position = position

        return first_one

    def get_highest_progress_bar_position(self) -> Optional[int]:
        """
        Obtain the highest registered progress bar position

        :return: Highest progress bar position
        """
        with self.lock:
            return self.highest_position

    def reset_progress_bar_positions(self) -> None:
        """
        Reset the registered progress bar positions
        """
        with self.lock:
            self.highest_position = None


class TqdmManager:
    
    """Tqdm manager wrapper for syncing multiple progress bars, independent of process start method used."""

    MANAGER = None
    LOCK = None
    POSITION_REGISTER = None

    @classmethod
    def start_manager(cls, use_dill: bool) -> bool:
        """
        Sets up and starts the tqdm manager

        :param use_dill: Whether dill is used as serialization library
        :return: Whether the manager was started
        """
        # Don't do anything when there's already a tqdm manager that has started
        if cls.LOCK is not None:
            return False

        logger.debug("Starting TQDM manager")

        # Create manager
        with DisableKeyboardInterruptSignal():
            cls.MANAGER = create_sync_manager(use_dill)
            cls.MANAGER.register('TqdmPositionRegister', TqdmPositionRegister)
            cls.MANAGER.start()
            cls.LOCK = cls.MANAGER.Lock()
            cls.POSITION_REGISTER = cls.MANAGER.TqdmPositionRegister(use_dill)

        return True

    @classmethod
    def stop_manager(cls) -> None:
        """
        Stops the tqdm manager
        """
        cls.MANAGER.shutdown()
        cls.MANAGER = None
        cls.LOCK = None
        cls.POSITION_REGISTER = None

    @classmethod
    def get_connection_details(cls) -> TqdmConnectionDetails:
        """
        Obtains the connection details of the tqdm manager. These details are needed to be passed on to child process
        when the start method is either forkserver or spawn.

        :return: TQDM lock and position register
        """
        return cls.LOCK, cls.POSITION_REGISTER

    @classmethod
    def set_connection_details(cls, tqdm_connection_details: TqdmConnectionDetails) -> None:
        """
        Sets the tqdm connection details.

        :param tqdm_connection_details: TQDM lock and position register
        """
        cls.LOCK, cls.POSITION_REGISTER = tqdm_connection_details
