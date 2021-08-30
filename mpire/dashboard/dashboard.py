import atexit
import errno
import getpass
import logging
import socket
from datetime import datetime
from multiprocessing import Event, Process, Value
from pkg_resources import resource_string
from typing import Dict, Optional, Sequence, Union

from flask import escape, Flask, jsonify, render_template, request
from werkzeug.serving import make_server

from mpire.signal import DisableKeyboardInterruptSignal, ignore_keyboard_interrupt
from mpire.dashboard.manager import (DASHBOARD_MANAGER_HOST, DASHBOARD_MANAGER_PORT,
                                     get_manager_client_dicts, start_manager_server)

logger = logging.getLogger(__name__)
logger_werkzeug = logging.getLogger('werkzeug')
logger_werkzeug.setLevel(logging.ERROR)
app = Flask(__name__)
_server = None
_progress_bar_html = resource_string(__name__, 'templates/progress_bar.html').decode('utf-8')

_DASHBOARD_MANAGER = None
_DASHBOARD_TQDM_DICT = None
_DASHBOARD_TQDM_DETAILS_DICT = None
DASHBOARD_STARTED_EVENT = Event()


@app.route('/')
def index() -> str:
    """
    Obtain the index HTML

    :return: HTML
    """
    return render_template('index.html', username=getpass.getuser(), hostname=socket.gethostname(),
                           manager_host=DASHBOARD_MANAGER_HOST.value.decode() or 'localhost',
                           manager_port_nr=DASHBOARD_MANAGER_PORT.value)


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
    if progress_bar_details['user'] == '{}@{}'.format(getpass.getuser(), socket.gethostname()):
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
    :return: A dictionary containing the dashboard port number and manager host and port_nr being used
    """
    global _DASHBOARD_MANAGER, _DASHBOARD_TQDM_DICT, _DASHBOARD_TQDM_DETAILS_DICT

    if not DASHBOARD_STARTED_EVENT.is_set():

        # Prevent signal from propagating to child process
        with DisableKeyboardInterruptSignal():

            # Set up manager server
            _DASHBOARD_MANAGER = start_manager_server(port_range)

            # Start flask server
            logging.getLogger('werkzeug').setLevel(logging.WARN)
            dashboard_port_nr = Value('i', 0, lock=False)
            Process(target=_run, args=(DASHBOARD_STARTED_EVENT, DASHBOARD_MANAGER_HOST.value,
                                       DASHBOARD_MANAGER_PORT.value, dashboard_port_nr, port_range),
                    daemon=True, name='dashboard-process').start()
            DASHBOARD_STARTED_EVENT.wait()

            # Return connect information
            return {'dashboard_port_nr': dashboard_port_nr.value,
                    'manager_host': DASHBOARD_MANAGER_HOST.value.decode() or socket.gethostname(),
                    'manager_port_nr': DASHBOARD_MANAGER_PORT.value}

    else:
        raise RuntimeError("You already have a running dashboard")


def connect_to_dashboard(manager_port_nr: int, manager_host: Optional[Union[bytes, str]] = None) -> None:
    """
    Connects to an existing MPIRE dashboard

    :param manager_port_nr: Port to use when connecting to a manager
    :param manager_host: Host to use when connecting to a manager. If ``None`` it will use localhost
    """
    global _DASHBOARD_MANAGER, _DASHBOARD_TQDM_DICT, _DASHBOARD_TQDM_DETAILS_DICT

    if not DASHBOARD_STARTED_EVENT.is_set():
        # Set connection variables so we can connect to the right manager
        manager_host = manager_host or "127.0.0.1"
        if isinstance(manager_host, str):
            manager_host = manager_host.encode()
        DASHBOARD_MANAGER_HOST.value = manager_host
        DASHBOARD_MANAGER_PORT.value = manager_port_nr
        DASHBOARD_STARTED_EVENT.set()
    else:
        raise RuntimeError("You're already connected to a running dashboard")


def _run(started: Event, manager_host: str, manager_port_nr: int, dashboard_port_nr: Value,
         port_range: Sequence) -> None:
    """
    Starts a dashboard server

    :param started: Event that signals the dashboard server has started
    :param manager_host: Dashboard manager host
    :param manager_port_nr: Dashboard manager port number
    :param dashboard_port_nr: Value object for storing the dashboad port number that is used
    :param port_range: Port range to try.
    """
    ignore_keyboard_interrupt()  # For Windows compatibility

    # Set dashboard connection details. This is needed when spawn is the default start method
    DASHBOARD_MANAGER_HOST.value = manager_host
    DASHBOARD_MANAGER_PORT.value = manager_port_nr

    # Connect to manager from this process
    global _DASHBOARD_TQDM_DICT, _DASHBOARD_TQDM_DETAILS_DICT, _server
    _DASHBOARD_TQDM_DICT, _DASHBOARD_TQDM_DETAILS_DICT, _ = get_manager_client_dicts()

    # Try different ports, until a free one is found
    for port in port_range:
        try:
            _server = make_server('0.0.0.0', port, app)
            dashboard_port_nr.value = port
            started.set()
            logger.info("Server started on 0.0.0.0:%d", port)
            _server.serve_forever()
            break
        except OSError as exc:
            if exc.errno != errno.EADDRINUSE:
                raise exc

    if not _server:
        raise OSError(f"Dashboard server: All ports are in use: {port_range}")


@atexit.register
def stop() -> None:
    """
    Called when the program exits, will shutdown the server
    """
    global _server
    if _server:
        try:
            _server.shutdown()
        except Exception:
            pass
