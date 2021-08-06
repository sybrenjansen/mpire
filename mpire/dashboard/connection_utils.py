from typing import Optional, Tuple

# If a user has not installed the dashboard dependencies than the imports below will fail
try:
    from mpire.dashboard import connect_to_dashboard
    from mpire.dashboard.dashboard import DASHBOARD_STARTED_EVENT
    from mpire.dashboard.manager import DASHBOARD_MANAGER_HOST, DASHBOARD_MANAGER_PORT
except (ImportError, ModuleNotFoundError):
    DASHBOARD_MANAGER_HOST = None
    DASHBOARD_MANAGER_PORT = None
    DASHBOARD_STARTED_EVENT = None

    def connect_to_dashboard(*_):
        pass

DashboardConnectionDetails = Tuple[Optional[bytes], Optional[int], bool]


def get_dashboard_connection_details() -> DashboardConnectionDetails:
    """
    Obtains the connection details of a dasbhoard. These details are needed to be passed on to child process when the
    start method is either forkserver or spawn.

    :return: Dashboard manager host, port_nr and whether a dashboard is started/connected
    """
    return (DASHBOARD_MANAGER_HOST.value if DASHBOARD_MANAGER_HOST is not None else None,
            DASHBOARD_MANAGER_PORT.value if DASHBOARD_MANAGER_PORT is not None else None,
            DASHBOARD_STARTED_EVENT.is_set() if DASHBOARD_STARTED_EVENT is not None else False)


def set_dashboard_connection(dashboard_connection_details: DashboardConnectionDetails,
                             auto_connect: bool = True) -> None:
    """
    Sets the dashboard connection details and connects to an existing dashboard if needed.

    :param dashboard_connection_details:  Dashboard manager host, port_nr and whether a dashboard is started/connected
    :param auto_connect: Whether to automatically connect to a sever when the dashboard_started event is set
    """
    dashboard_manager_host, dashboard_manager_port_nr, dashboard_started = dashboard_connection_details
    if (dashboard_manager_host is not None and dashboard_manager_port_nr is not None and
            not DASHBOARD_STARTED_EVENT.is_set()):
        if dashboard_started and auto_connect:
            connect_to_dashboard(dashboard_manager_port_nr, dashboard_manager_host)
        else:
            DASHBOARD_MANAGER_HOST.value = dashboard_manager_host
            DASHBOARD_MANAGER_PORT.value = dashboard_manager_port_nr
            if dashboard_started:
                DASHBOARD_STARTED_EVENT.set()
