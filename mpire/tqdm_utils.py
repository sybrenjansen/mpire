import logging
from multiprocessing import Lock
from multiprocessing.managers import SyncManager
import os
from typing import Optional, Tuple, Type, Union

from tqdm import tqdm as tqdm_std
from tqdm.notebook import tqdm as tqdm_notebook

from mpire.signal import DisableKeyboardInterruptSignal

PROGRESS_BAR_DEFAULT_STYLE = 'std'
TqdmConnectionDetails = Tuple[Union[str, bytes, None], bytes, bool]

logger = logging.getLogger(__name__)


def get_tqdm(progress_bar_style: Optional[str]) -> Tuple[Type[tqdm_std], bool]:
    """
    Get the tqdm class to use based on the progress bar style

    :param progress_bar_style: The progress bar style to use. Can be one of ``None``, ``std``, or ``notebook``
    :return: A tuple containing the tqdm class to use and a boolean indicating whether the progress bar is a notebook
        widget
    """
    if progress_bar_style is None:
        progress_bar_style = PROGRESS_BAR_DEFAULT_STYLE
    if progress_bar_style == 'std':
        return tqdm_std, False
    elif progress_bar_style == 'notebook':
        return tqdm_notebook, True
    elif progress_bar_style == 'dashboard':
        return TqdmDashboardOnly, False
    else:
        raise ValueError(f'Invalid progress bar style: {progress_bar_style}. '
                         f'Use either None (=default), "std", or "notebook"')


class TqdmDashboardOnly(tqdm_std):

    """
    A tqdm class that gives no output, but will still update the internal progress-bar attributes that the
    dashboard relies on.
    """

    def display(self, *args, **kwargs) -> None:
        pass


class TqdmPositionRegister:

    """
    Class that keeps track of all the registered progress bar positions. Needed to properly display multiple tqdm
    progress bars
    """

    def __init__(self) -> None:
        self.lock = Lock()
        self.highest_position = None

    def register_progress_bar_position(self, position) -> bool:
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


class TqdmLock:

    """
    Small wrapper around a multiprocessing.Lock proxy object returned by a Manager. These proxies only expose public
    functions, so we wrap such a proxy and expose the private/public functions needed for a lock to work.
    """

    def __init__(self, lock_proxy: Lock) -> None:
        self.lock_proxy = lock_proxy

    def acquire(self, *args, **kwargs) -> None:
        self.lock_proxy.acquire(*args, **kwargs)

    def release(self) -> None:
        self.lock_proxy.release()

    def __enter__(self) -> None:
        self.lock_proxy.acquire()

    def __exit__(self, *_) -> None:
        self.lock_proxy.release()


class TqdmManager:

    LOCK = Lock()
    POSITION_REGISTER = TqdmPositionRegister()

    MANAGER = None
    MANAGER_HOST: Union[str, bytes, None] = None
    MANAGER_AUTHKEY = b""
    MANAGER_STARTED = False

    def __init__(self) -> None:
        """
        Tqdm manager wrapper for syncing multiple progress bars, independent of process start method used.
        """
        # Connect to existing manager, if it exists
        if self.MANAGER_STARTED:
            self.connect_to_manager()

    @classmethod
    def start_manager(cls) -> bool:
        """
        Sets up and starts the tqdm manager

        :return: Whether the manager was started
        """
        # Don't do anything when there's already a connected tqdm manager
        if cls.MANAGER_STARTED:
            return False

        logger.debug("Starting TQDM manager")

        # Create manager
        with DisableKeyboardInterruptSignal():
            cls.MANAGER = SyncManager(authkey=os.urandom(24))
            cls.MANAGER.register('get_tqdm_lock', cls._get_tqdm_lock)
            cls.MANAGER.register('get_tqdm_position_register', cls._get_tqdm_position_register)
            cls.MANAGER.start()

        # Set host and authkey so other processes know where to connect to
        cls.MANAGER_HOST = cls.MANAGER.address
        cls.MANAGER_AUTHKEY = bytes(cls.MANAGER._authkey)
        cls.MANAGER_STARTED = True

        return True

    def connect_to_manager(self) -> None:
        """
        Connect to the tqdm manager
        """
        self.MANAGER = SyncManager(address=self.MANAGER_HOST, authkey=self.MANAGER_AUTHKEY)
        self.MANAGER.register('get_tqdm_lock')
        self.MANAGER.register('get_tqdm_position_register')
        self.MANAGER.connect()

    @classmethod
    def stop_manager(cls) -> None:
        """
        Stops the tqdm manager
        """
        cls.MANAGER.shutdown()
        cls.MANAGER = None
        cls.MANAGER_HOST = None
        cls.MANAGER_AUTHKEY = b""
        cls.MANAGER_STARTED = False

    @staticmethod
    def _get_tqdm_lock() -> Lock:
        """
        This function needs to be static, because a manager doesn't pass arguments to registered functions

        :return: Lock object for tqdm
        """
        return TqdmManager.LOCK

    @staticmethod
    def _get_tqdm_position_register() -> TqdmPositionRegister:
        """
        This function needs to be static, because a manager doesn't pass arguments to registered functions

        :return: tqdm position register
        """
        return TqdmManager.POSITION_REGISTER

    def get_lock_and_position_register(self) -> Tuple[TqdmLock, TqdmPositionRegister]:
        """
        Obtain synchronized tqdm lock and positions register

        :return: Synchronized tqdm lock and tqdm position register
        """
        return TqdmLock(self.MANAGER.get_tqdm_lock()), self.MANAGER.get_tqdm_position_register()

    @classmethod
    def get_connection_details(cls) -> TqdmConnectionDetails:
        """
        Obtains the connection details of the tqdm manager. These details are needed to be passed on to child process
        when the start method is either forkserver or spawn.

        :return: TQDM manager host, authkey, and whether a manager is started/connected
        """
        return cls.MANAGER_HOST, cls.MANAGER_AUTHKEY, cls.MANAGER_STARTED

    @classmethod
    def set_connection_details(cls, tqdm_connection_details: TqdmConnectionDetails) -> None:
        """
        Sets the tqdm connection details and connects to an existing tqdm manager if needed.

        :param tqdm_connection_details: TQDM manager host, and whether a manager is started/connected
        """
        tqdm_manager_host, tqdm_manager_authkey, tqdm_manager_started = tqdm_connection_details
        if not cls.MANAGER_STARTED:
            cls.MANAGER_HOST = tqdm_manager_host
            cls.MANAGER_AUTHKEY = tqdm_manager_authkey
            cls.MANAGER_STARTED = tqdm_manager_started
