from multiprocessing import Lock
from multiprocessing.synchronize import Lock as LockType
from multiprocessing.managers import BaseProxy
from typing import Dict, Optional, Tuple

from mpire.dashboard.connection_classes import DashboardManager, DashboardManagerConnectionDetails
from mpire.signal import ignore_keyboard_interrupt
    

# Dict for tqdm progress bar updates
DASHBOARD_TQDM_DICT = None

# Dict for tqdm progress bar details (function called etc.)
DASHBOARD_TQDM_DETAILS_DICT = None

# Lock for registering new progress bars
DASHBOARD_TQDM_LOCK = None

# Connection details for connecting to a manager
DASHBOARD_MANAGER_CONNECTION_DETAILS = DashboardManagerConnectionDetails()


def get_dashboard_tqdm_dict() -> Dict:
    """
    :return: Dashboard tqdm dict which should be used in a DashboardManager context
    """
    global DASHBOARD_TQDM_DICT
    if DASHBOARD_TQDM_DICT is None:
        DASHBOARD_TQDM_DICT = {}
    return DASHBOARD_TQDM_DICT


def get_dashboard_tqdm_details_dict() -> Dict:
    """
    :return: Dashboard tqdm details dict which should be used in a DashboardManager context
    """
    global DASHBOARD_TQDM_DETAILS_DICT
    if DASHBOARD_TQDM_DETAILS_DICT is None:
        DASHBOARD_TQDM_DETAILS_DICT = {}
    return DASHBOARD_TQDM_DETAILS_DICT


def get_dashboard_tqdm_lock() -> LockType:
    """
    :return: Dashboard tqdm lock which should be used in a DashboardManager context
    """
    global DASHBOARD_TQDM_LOCK
    if DASHBOARD_TQDM_LOCK is None:
        DASHBOARD_TQDM_LOCK = Lock()
    return DASHBOARD_TQDM_LOCK


def start_manager_server(manager_port_nr: int) -> DashboardManager:
    """
    Start a SyncManager

    :param manager_port_nr: Port number to use for the manager
    :return: SyncManager and hostname
    """
    global DASHBOARD_TQDM_DICT, DASHBOARD_TQDM_DETAILS_DICT, DASHBOARD_TQDM_LOCK, \
        DASHBOARD_MANAGER_HOST, DASHBOARD_MANAGER_PORT
    
    DashboardManager.register('get_dashboard_tqdm_dict', get_dashboard_tqdm_dict)
    DashboardManager.register('get_dashboard_tqdm_details_dict', get_dashboard_tqdm_details_dict)
    DashboardManager.register('get_dashboard_tqdm_lock', get_dashboard_tqdm_lock)
    
    # Create manager
    dm = DashboardManager(address=("127.0.0.1", manager_port_nr), authkey=b'mpire_dashboard')
    dm.start(ignore_keyboard_interrupt)
    DASHBOARD_TQDM_DICT = dm.get_dashboard_tqdm_dict()
    DASHBOARD_TQDM_DETAILS_DICT = dm.get_dashboard_tqdm_details_dict()
    DASHBOARD_TQDM_LOCK = dm.get_dashboard_tqdm_lock()

    # Set host and port number so other processes know where to connect to
    DASHBOARD_MANAGER_CONNECTION_DETAILS.host = "127.0.0.1"
    DASHBOARD_MANAGER_CONNECTION_DETAILS.port = manager_port_nr

    return dm


def shutdown_manager_server(manager: Optional[DashboardManager]) -> None:
    """
    Shutdown a DashboardManager
    
    :param manager: DashboardManager to shutdown
    """
    global DASHBOARD_TQDM_DICT, DASHBOARD_TQDM_DETAILS_DICT, DASHBOARD_TQDM_LOCK
    if manager is not None:
        manager.shutdown()
    DASHBOARD_TQDM_DICT = None
    DASHBOARD_TQDM_DETAILS_DICT = None
    DASHBOARD_TQDM_LOCK = None
    DASHBOARD_MANAGER_CONNECTION_DETAILS.clear()


def get_manager_client_dicts() -> Tuple[BaseProxy, BaseProxy, BaseProxy]:
    """
    Connect to a DashboardManager and obtain the synchronized tqdm dashboard dicts

    :return: DashboardManager tqdm dict, tqdm details dict, tqdm lock
    """
    global DASHBOARD_TQDM_DICT, DASHBOARD_TQDM_DETAILS_DICT, DASHBOARD_TQDM_LOCK
    
    # If we're already connected to a manager, return the dicts directly
    if DASHBOARD_TQDM_DICT is not None:
        return DASHBOARD_TQDM_DICT, DASHBOARD_TQDM_DETAILS_DICT, DASHBOARD_TQDM_LOCK
    
    # Connect to a server
    DashboardManager.register('get_dashboard_tqdm_dict', get_dashboard_tqdm_dict)
    DashboardManager.register('get_dashboard_tqdm_details_dict', get_dashboard_tqdm_details_dict)
    DashboardManager.register('get_dashboard_tqdm_lock', get_dashboard_tqdm_lock)
    dm = DashboardManager(
        address=(DASHBOARD_MANAGER_CONNECTION_DETAILS.host, DASHBOARD_MANAGER_CONNECTION_DETAILS.port), 
        authkey=b'mpire_dashboard'
    )
    dm.connect()

    DASHBOARD_TQDM_DICT = dm.get_dashboard_tqdm_dict()
    DASHBOARD_TQDM_DETAILS_DICT = dm.get_dashboard_tqdm_details_dict()
    DASHBOARD_TQDM_LOCK = dm.get_dashboard_tqdm_lock()
    return DASHBOARD_TQDM_DICT, DASHBOARD_TQDM_DETAILS_DICT, DASHBOARD_TQDM_LOCK
