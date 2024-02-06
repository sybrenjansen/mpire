from typing import Optional, Tuple

from mpire.dashboard.connection_classes import DashboardManagerConnectionDetails, DashboardStartedEvent

# If a user has not installed the dashboard dependencies than the imports below will fail
try:
    from mpire.dashboard import connect_to_dashboard
    from mpire.dashboard.dashboard import DASHBOARD_STARTED_EVENT
    from mpire.dashboard.manager import DASHBOARD_MANAGER_CONNECTION_DETAILS
except (ImportError, ModuleNotFoundError):
    DASHBOARD_MANAGER_CONNECTION_DETAILS = DashboardManagerConnectionDetails()
    DASHBOARD_STARTED_EVENT = DashboardStartedEvent()

    def connect_to_dashboard(*_):
        pass

DashboardConnectionDetails = Tuple[Optional[str], Optional[int], bool]


def get_dashboard_connection_details() -> DashboardConnectionDetails:
    """
    Obtains the connection details of a dasbhoard. These details are needed to be passed on to child process when the
    start method is either forkserver or spawn.

    :return: Dashboard manager host, port_nr and whether a dashboard is started/connected
    """
    return (DASHBOARD_MANAGER_CONNECTION_DETAILS.host, DASHBOARD_MANAGER_CONNECTION_DETAILS.port, 
            DASHBOARD_STARTED_EVENT.is_set())


def set_dashboard_connection(dashboard_connection_details: DashboardConnectionDetails,
                             auto_connect: bool = True) -> None:
    """
    Sets the dashboard connection details and connects to an existing dashboard if needed.

    :param dashboard_connection_details:  Dashboard manager host, port_nr and whether a dashboard is started/connected
    :param auto_connect: Whether to automatically connect to a server when the dashboard_started event is set
    """
    global DASHBOARD_MANAGER_CONNECTION_DETAILS
    
    dashboard_manager_host, dashboard_manager_port_nr, dashboard_started = dashboard_connection_details
    if (dashboard_manager_host is not None and dashboard_manager_port_nr is not None and
            not DASHBOARD_STARTED_EVENT.is_set()):
        if dashboard_started and auto_connect:
            connect_to_dashboard(dashboard_manager_port_nr, dashboard_manager_host)
        else:
            DASHBOARD_MANAGER_CONNECTION_DETAILS.host = dashboard_manager_host
            DASHBOARD_MANAGER_CONNECTION_DETAILS.port = dashboard_manager_port_nr
            if dashboard_started:
                DASHBOARD_STARTED_EVENT.set()
