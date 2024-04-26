import atexit
import getpass
try:
    from importlib.resources import files as resource
except ImportError:
    # Python < 3.9 compatibility
    from importlib_resources import files as resource
import logging
import os
import signal
import socket
from datetime import datetime
from multiprocessing import Event, Process
from multiprocessing.managers import BaseProxy
from typing import Dict, Optional, Sequence, Tuple, Union

from flask import Flask, jsonify, render_template, request
from markupsafe import escape
from werkzeug.serving import make_server

from mpire.dashboard.connection_classes import DashboardStartedEvent
from mpire.dashboard.manager import (DASHBOARD_MANAGER_CONNECTION_DETAILS,
                                     get_manager_client_dicts, shutdown_manager_server, start_manager_server)
from mpire.dashboard.utils import get_two_available_ports

logger = logging.getLogger(__name__)
logger_werkzeug = logging.getLogger('werkzeug')
logger_werkzeug.setLevel(logging.ERROR)
app = Flask(__name__)
_server_process = None
with open(resource('mpire.dashboard') / 'templates' / 'progress_bar.html', 'r') as fp:
    _progress_bar_html = fp.read()

_DASHBOARD_MANAGER = None
_DASHBOARD_TQDM_DICT = None
_DASHBOARD_TQDM_DETAILS_DICT = None
DASHBOARD_STARTED_EVENT = DashboardStartedEvent()


@app.route('/')
def index() -> str:
    """
    Obtain the index HTML

    :return: HTML
    """
    # Obtain user. This can fail when the current uid refers to a non-existing user, which can happen when running in a
    # container as a non-root user. See https://github.com/sybrenjansen/mpire/issues/128.
    try:
        user = getpass.getuser()
    except KeyError:
        user = "n/a"
    return render_template('index.html', username=user, hostname=socket.gethostname(),
                           manager_host=DASHBOARD_MANAGER_CONNECTION_DETAILS.host or 'localhost',
                           manager_port_nr=DASHBOARD_MANAGER_CONNECTION_DETAILS.port)


@app.route('/_progress_bar_update')
def progress_bar_update() -> str:
    """
    Obtain progress bar updates (should be called through AJAX)

    :return: JSON string containing progress bar updates
    """
    # As we get updates only when the progress bar is updated we need to fix the 'duration' and 'time remaining' parts
    # (time never stops)
    now = datetime.now()
    result = []
    for pb_id in sorted(_DASHBOARD_TQDM_DICT.keys()):
        progress = _DASHBOARD_TQDM_DICT.get(pb_id)
        if progress['total'] is None:
            progress['total'] = '?'
        if progress['success'] and progress['n'] != progress['total']:
            progress['duration'] = str(now - progress['started_raw']).rsplit('.', 1)[0]
            progress['remaining'] = (str(progress['finished_raw'] - now).rsplit('.', 1)[0]
                                     if progress['finished_raw'] is not None and progress['finished_raw'] > now
                                     else '-')
        result.append(progress)

    return jsonify(result=result)


@app.route('/_progress_bar_new')
def progress_bar_new() -> str:
    """
    Obtain a piece of HTML for a new progress bar (should be called through AJAX)

    :return: JSON string containing new progress bar HTML
    """
    pb_id = int(request.args['pb_id'])
    has_insights = request.args['has_insights'] == 'true'

    # Obtain progress bar details. Only show the user@host part if it doesn't equal the user@host of this process
    # (in case someone connected to this dashboard from another machine or user)
    progress_bar_details = _DASHBOARD_TQDM_DETAILS_DICT.get(pb_id)
    if progress_bar_details['user'] == f'{getpass.getuser()}@{socket.gethostname()}':
        progress_bar_details['user'] = ''
    else:
        progress_bar_details['user'] = '{}:'.format(progress_bar_details['user'])

    # Create table for worker insights
    insights_workers = []
    if has_insights:
        for worker_id in range(progress_bar_details['n_jobs']):
            insights_workers.append(f"<tr><td>{worker_id}</td>"
                                    f"<td id='pb_{pb_id}_insights_worker_{worker_id}_tasks_completed'></td>"
                                    f"<td id='pb_{pb_id}_insights_worker_{worker_id}_start_up_time'></td>"
                                    f"<td id='pb_{pb_id}_insights_worker_{worker_id}_init_time'></td>"
                                    f"<td id='pb_{pb_id}_insights_worker_{worker_id}_waiting_time'></td>"
                                    f"<td id='pb_{pb_id}_insights_worker_{worker_id}_working_time'></td>"
                                    f"<td id='pb_{pb_id}_insights_worker_{worker_id}_exit_time'></td>"
                                    f"</tr>")
    insights_workers = "\n".join(insights_workers)

    return jsonify(result=_progress_bar_html.format(id=pb_id, insights_workers=insights_workers,
                                                    has_insights='block' if has_insights else 'none',
                                                    **{k: escape(v) for k, v in progress_bar_details.items()}))


def start_dashboard(port_range: Sequence = range(8080, 8100)) -> Dict[str, Union[int, str]]:
    """
    Starts a new MPIRE dashboard

    :param port_range: Port range to try.
    :return: A dictionary containing the dashboard port number and manager host and port number being used
    """
    global _server_process, _DASHBOARD_MANAGER
        
    if not DASHBOARD_STARTED_EVENT.is_set():

        DASHBOARD_STARTED_EVENT.init()
        
        dashboard_port_nr, manager_port_nr = get_two_available_ports(port_range)

        # Set up manager server
        _DASHBOARD_MANAGER = start_manager_server(manager_port_nr)

        # Start flask server
        logging.getLogger('werkzeug').setLevel(logging.WARN)
        _server_process = Process(target=_run, args=(DASHBOARD_STARTED_EVENT, dashboard_port_nr, 
                                                        get_manager_client_dicts()),
                                    daemon=True, name='dashboard-process')
        _server_process.start()
        DASHBOARD_STARTED_EVENT.wait()

        # Return connect information
        return {'dashboard_port_nr': dashboard_port_nr,
                'manager_host': DASHBOARD_MANAGER_CONNECTION_DETAILS.host or socket.gethostname(),
                'manager_port_nr': DASHBOARD_MANAGER_CONNECTION_DETAILS.port}

    else:
        raise RuntimeError("You already have a running dashboard")


@atexit.register
def shutdown_dashboard() -> None:
    """ Shuts down the dashboard """
    if DASHBOARD_STARTED_EVENT.is_set():
        global _server_process, _DASHBOARD_MANAGER, _DASHBOARD_TQDM_DICT, _DASHBOARD_TQDM_DETAILS_DICT
        if _server_process is not None:
            # Send SIGINT to the server process, which is the only way to stop it without causing semaphore leaks
            os.kill(_server_process.pid, signal.SIGINT)
            _server_process.join()
        shutdown_manager_server(_DASHBOARD_MANAGER)
        _DASHBOARD_MANAGER = None
        _DASHBOARD_TQDM_DICT = None
        _DASHBOARD_TQDM_DETAILS_DICT = None
        DASHBOARD_STARTED_EVENT.reset()
        

def connect_to_dashboard(manager_port_nr: int, manager_host: Optional[Union[bytes, str]] = None) -> None:
    """
    Connects to an existing MPIRE dashboard

    :param manager_port_nr: Port to use when connecting to a manager
    :param manager_host: Host to use when connecting to a manager. If ``None`` it will use localhost
    """
    global _DASHBOARD_MANAGER, DASHBOARD_MANAGER_CONNECTION_DETAILS

    if DASHBOARD_STARTED_EVENT.is_set():
        raise RuntimeError("You're already connected to a running dashboard")

    # Set connection variables so we can connect to the right manager
    manager_host = manager_host or "127.0.0.1"
    DASHBOARD_MANAGER_CONNECTION_DETAILS.host = manager_host
    DASHBOARD_MANAGER_CONNECTION_DETAILS.port = manager_port_nr

    # Try to connect
    try:
        get_manager_client_dicts()
    except ConnectionRefusedError:
        raise ConnectionRefusedError("Could not connect to dashboard manager at "
                                     f"{manager_host.decode()}:{manager_port_nr}")

    DASHBOARD_STARTED_EVENT.set()


def _run(started: Event, dashboard_port_nr: int, manager_client_dicts: Tuple[BaseProxy, BaseProxy, BaseProxy]) -> None:
    """
    Starts a dashboard server

    :param started: Event that signals the dashboard server has started
    :param manager_host: Dashboard manager host
    :param manager_port_nr: Dashboard manager port number
    :param dashboard_port_nr: Dashboard port number
    """
    global _DASHBOARD_TQDM_DICT, _DASHBOARD_TQDM_DETAILS_DICT
    _DASHBOARD_TQDM_DICT, _DASHBOARD_TQDM_DETAILS_DICT, _ = manager_client_dicts

    # Start server
    server = make_server('0.0.0.0', dashboard_port_nr, app)
    started.set()
    logger.info(f"Server started on 0.0.0.0:{dashboard_port_nr}")
    server.serve_forever()
