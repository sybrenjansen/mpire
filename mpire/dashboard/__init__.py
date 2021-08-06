try:
    from mpire.dashboard.dashboard import connect_to_dashboard, start_dashboard
except (ImportError, ModuleNotFoundError):
    def connect_to_dashboard(*_, **__):
        raise NotImplementedError("Install the dashboard dependencies to enable the dashboard")

    def start_dashboard(*_, **__):
        raise NotImplementedError("Install the dashboard dependencies to enable the dashboard")
