import atexit
import errno
import getpass
import logging
import socket
from datetime import datetime
from multiprocessing import Event, Process, Value
from pkg_resources import resource_string

from flask import escape, Flask, jsonify, render_template, request
from werkzeug.serving import make_server

from mpire.signal import DisableSignal
from mpire.dashboard.manager import get_manager_client_dicts, start_manager_server

logger = logging.getLogger(__name__)
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
    return render_template('index.html', username=getpass.getuser(), hostname=socket.gethostname())


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
    return jsonify(result=_progress_bar_html.format(
        id=request.args['pb_id'],
        **{k: escape(v) for k, v in _DASHBOARD_TQDM_DETAILS_DICT.get(int(request.args['pb_id'])).items()}
    ))


def start_dashboard() -> int:
    """
    Starts an MPIRE dashboard

    :return: port number that is used
    """
    global _DASHBOARD_MANAGER

    if not DASHBOARD_STARTED_EVENT.is_set():

        # Prevent signal from propagating to child process
        with DisableSignal():

            # Set up manager server
            _DASHBOARD_MANAGER = start_manager_server()

            # Start flask server
            logging.getLogger('werkzeug').setLevel(logging.WARN)
            port_nr = Value('i', 0, lock=False)
            Process(target=_run, args=(DASHBOARD_STARTED_EVENT, port_nr), daemon=True, name='dashboard-thread').start()
            DASHBOARD_STARTED_EVENT.wait()
            return port_nr.value
    else:
        raise RuntimeError("You already have a running dashboard")


def _run(started: Event, port_nr: Value) -> None:
    """
    Starts a dashboard server

    :param started: Event that signals the dashboard server has started
    """
    # Connect to manager from this process
    global _DASHBOARD_TQDM_DICT, _DASHBOARD_TQDM_DETAILS_DICT
    _DASHBOARD_TQDM_DICT, _DASHBOARD_TQDM_DETAILS_DICT, _ = get_manager_client_dicts()

    # Try different ports, until a free one is found
    for port in range(8080, 8200):
        try:
            global _server
            _server = make_server('0.0.0.0', port, app)
            port_nr.value = port
            started.set()
            logger.info("Server started on 0.0.0.0:%d", port)
            _server.serve_forever()
            break
        except OSError as exc:
            if exc.errno != errno.EADDRINUSE:
                raise exc


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
