import socket
from ctypes import c_char
from multiprocessing import Array, Lock, Value
from multiprocessing.managers import BaseProxy, SyncManager
from typing import Dict, Tuple

# Dict for tqdm progress bar updates
DASHBOARD_TQDM_DICT = {}

# Dict for tqdm progress bar details (function called etc.)
DASHBOARD_TQDM_DETAILS_DICT = {}

# Lock for registering new progress bars
DASHBOARD_TQDM_LOCK = Lock()

# Array which tells which host and a value which tells which port to use for connecting to a manager
DASHBOARD_MANAGER_HOST = Array(c_char, 10000, lock=True)
DASHBOARD_MANAGER_PORT = Value('i', lock=True)


def get_dashboard_tqdm_dict() -> Dict:
    """
    :return: Dashboard tqdm dict which should be used in a SyncManager context
    """
    return DASHBOARD_TQDM_DICT


def get_dashboard_tqdm_details_dict() -> Dict:
    """
    :return: Dashboard tqdm details dict which should be used in a SyncManager context
    """
    return DASHBOARD_TQDM_DETAILS_DICT


def get_dashboard_tqdm_lock() -> Lock:
    """
    :return: Dashboard tqdm lock which should be used in a SyncManager context
    """
    return DASHBOARD_TQDM_LOCK


def start_manager_server() -> SyncManager:
    """
    Start a SyncManager

    :return: SyncManager
    """
    for port_nr in reversed(range(8080, 8100)):
        try:
            # If a port is already occupied the SyncManager process will spit out EOFError and OSError messages. The
            # former can be catched, but the latter will still show up. So we first check if a port is available
            # manually
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(('', port_nr))
            s.close()

            # Create manager
            sm = SyncManager(address=('', port_nr), authkey=b'mpire_dashboard')
            sm.register('get_dashboard_tqdm_dict', get_dashboard_tqdm_dict)
            sm.register('get_dashboard_tqdm_details_dict', get_dashboard_tqdm_details_dict)
            sm.register('get_dashboard_tqdm_lock', get_dashboard_tqdm_lock)
            sm.start()

            # Set host and port number so other processes know where to connect to
            DASHBOARD_MANAGER_HOST.value = b''
            DASHBOARD_MANAGER_PORT.value = port_nr

            return sm

        except OSError:
            # Port is occupied, ignore it and try another
            pass

    raise OSError("All ports (8080-8099) are in use")


def get_manager_client_dicts() -> Tuple[BaseProxy, BaseProxy, BaseProxy]:
    """
    Connect to a SyncManager and obtain the synchronized tqdm dashboard dicts

    :return: Synchronized tqdm dict, tqdm details dict, tqdm lock
    """
    # Connect to a server
    sm = SyncManager(address=(DASHBOARD_MANAGER_HOST.value.decode(), DASHBOARD_MANAGER_PORT.value),
                     authkey=b'mpire_dashboard')
    sm.register('get_dashboard_tqdm_dict', get_dashboard_tqdm_dict)
    sm.register('get_dashboard_tqdm_details_dict', get_dashboard_tqdm_details_dict)
    sm.register('get_dashboard_tqdm_lock', get_dashboard_tqdm_lock)
    sm.connect()

    return sm.get_dashboard_tqdm_dict(), sm.get_dashboard_tqdm_details_dict(), sm.get_dashboard_tqdm_lock()
