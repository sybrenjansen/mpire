try:
    from mpire.dashboard.dashboard import connect_to_dashboard, shutdown_dashboard, start_dashboard
    from mpire.dashboard.utils import get_stacklevel, set_stacklevel
except (ImportError, ModuleNotFoundError):
    def _not_installed(*_, **__):
        raise NotImplementedError("Install the dashboard dependencies to enable the dashboard")
    
    connect_to_dashboard = shutdown_dashboard = start_dashboard = _not_installed
    get_stacklevel = set_stacklevel = _not_installed
