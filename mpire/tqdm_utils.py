import logging
from ctypes import c_char
from multiprocessing import Array, Event, Lock
from multiprocessing.managers import SyncManager
from typing import Optional, Tuple

from mpire.signal import DisableKeyboardInterruptSignal

TqdmConnectionDetails = Tuple[Optional[bytes], bool]

logger = logging.getLogger(__name__)


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
    MANAGER_HOST = Array(c_char, 10000, lock=True)
    MANAGER_STARTED = Event()

    def __init__(self) -> None:
        """
        Tqdm manager wrapper for syncing multiple progress bars, independent of process start method used.
        """
        # Connect to existing manager, if it exists
        if self.MANAGER_STARTED.is_set():
            self.connect_to_manager()

    @classmethod
    def start_manager(cls) -> bool:
        """
        Sets up and starts the tqdm manager

        :return: Whether the manager was started
        """
        # Don't do anything when there's already a connected tqdm manager
        if cls.MANAGER_STARTED.is_set():
            return False

        logger.debug("Starting TQDM manager")

        # Create manager
        with DisableKeyboardInterruptSignal():
            cls.MANAGER = SyncManager(authkey=b'mpire_tqdm')
            cls.MANAGER.register('get_tqdm_lock', cls._get_tqdm_lock)
            cls.MANAGER.register('get_tqdm_position_register', cls._get_tqdm_position_register)
            cls.MANAGER.start()
        cls.MANAGER_STARTED.set()

        # Set host so other processes know where to connect to. On some systems and Python versions address is a bytes
        # object, on others it's a string. On some it's also prefixed by a null byte which needs to be removed (null
        # byte doesn't work with Array).
        address = cls.MANAGER.address
        if isinstance(address, str):
            address = address.encode()
        if address[0] == 0:
            address = address[1:]
        cls.MANAGER_HOST.value = address

        return True

    def connect_to_manager(self) -> None:
        """
        Connect to the tqdm manager
        """
        # Connect to a server. On some systems and Python versions the address is prefixed by a null byte (which was
        # stripped when setting the host value, due to restrictions in Array). Address needs to be a string.
        address = self.MANAGER_HOST.value.decode()
        try:
            self.MANAGER = SyncManager(address=address, authkey=b'mpire_tqdm')
            self.MANAGER.register('get_tqdm_lock')
            self.MANAGER.register('get_tqdm_position_register')
            self.MANAGER.connect()
        except FileNotFoundError:
            address = f"\x00{address}"
            self.MANAGER = SyncManager(address=address, authkey=b'mpire_tqdm')
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
        cls.MANAGER_HOST.value = b''
        cls.MANAGER_STARTED.clear()

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

        :return: TQDM manager host and whether a manager is started/connected
        """
        return cls.MANAGER_HOST.value, cls.MANAGER_STARTED.is_set()

    @classmethod
    def set_connection_details(cls, tqdm_connection_details: TqdmConnectionDetails) -> None:
        """
        Sets the tqdm connection details and connects to an existing tqdm manager if needed.

        :param tqdm_connection_details: TQDM manager host, and whether a manager is started/connected
        """
        tqdm_manager_host, tqdm_manager_started = tqdm_connection_details
        if not cls.MANAGER_STARTED.is_set():
            cls.MANAGER_HOST.value = tqdm_manager_host
            if tqdm_manager_started:
                cls.MANAGER_STARTED.set()
