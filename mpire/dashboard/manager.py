import socket
from multiprocessing import Lock, Value
from multiprocessing.managers import BaseProxy, SyncManager
from typing import Dict, Tuple

# Dict for tqdm progress bar updates
DASHBOARD_TQDM_DICT = {}

# Dict for tqdm progress bar details (function called etc.)
DASHBOARD_TQDM_DETAILS_DICT = {}

# Lock for registering new progress bars
DASHBOARD_TQDM_LOCK = Lock()

# Value which tells which port to use for connecting to a manager
DASHBOARD_MANAGER_PORT = Value('i', lock=True)


def get_dashboard_tqdm_dict() -> Dict:
    """
    :return: dashboard tqdm dict which should be used in a SyncManager context
    """
    return DASHBOARD_TQDM_DICT


def get_dashboard_tqdm_details_dict() -> Dict:
    """
    :return: dashboard tqdm details dict which should be used in a SyncManager context
    """
    return DASHBOARD_TQDM_DETAILS_DICT


def get_dashboard_tqdm_lock() -> Lock:
    """
    :return: dashboard tqdm lock which should be used in a SyncManager context
    """
    return DASHBOARD_TQDM_LOCK


def start_manager_server() -> SyncManager:
    """
    Start a SyncManager

    :return: SyncManager
    """
    for port_nr in range(50000, 50100):
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

            # Set port number so other processes know which port to use
            DASHBOARD_MANAGER_PORT.value = port_nr

            return sm

        except OSError:
            # Port is occupied, ignore it and try another
            pass


def get_manager_client_dicts() -> Tuple[BaseProxy, BaseProxy, BaseProxy]:
    """
    Connect to a SyncManager and obtain the synchronized tqdm dashboard dicts

    :return: synchronized tqdm dict, tqdm details dict, tqdm lock
    """
    # Connect to a server
    sm = SyncManager(address=('', DASHBOARD_MANAGER_PORT.value), authkey=b'mpire_dashboard')
    sm.register('get_dashboard_tqdm_dict', get_dashboard_tqdm_dict)
    sm.register('get_dashboard_tqdm_details_dict', get_dashboard_tqdm_details_dict)
    sm.register('get_dashboard_tqdm_lock', get_dashboard_tqdm_lock)
    sm.connect()

    return sm.get_dashboard_tqdm_dict(), sm.get_dashboard_tqdm_details_dict(), sm.get_dashboard_tqdm_lock()
