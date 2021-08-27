import multiprocessing as mp
try:
    import multiprocess as mp_dill
except ImportError:
    mp_dill = None
import platform
import threading

# Check if fork is available as start method. It's not available on Windows machines
try:
    mp.get_context('fork')
    FORK_AVAILABLE = True
except ValueError:
    FORK_AVAILABLE = False

# Check if we're running on Windows
RUNNING_WINDOWS = platform.system() == "Windows"


# Threading context so we can use threading as backend as well
class ThreadingContext:

    Barrier = threading.Barrier
    Event = threading.Event
    Lock = threading.Lock
    Thread = threading.Thread

    # threading doesn't have Array and JoinableQueue, so we take it from multiprocessing. Both are thread-safe. We need
    # the Process class for the MPIRE insights SyncManager instance.
    Array = mp.Array
    JoinableQueue = mp.JoinableQueue
    Process = mp.Process
    Value = mp.Value


MP_CONTEXTS = {'mp': {'fork': mp.get_context('fork') if FORK_AVAILABLE else None,
                      'forkserver': mp.get_context('forkserver') if FORK_AVAILABLE else None,
                      'spawn': mp.get_context('spawn')},
               'threading': ThreadingContext}
if mp_dill is not None:
    MP_CONTEXTS['mp_dill'] = {'fork': mp_dill.get_context('fork') if FORK_AVAILABLE else None,
                              'forkserver': mp_dill.get_context('forkserver') if FORK_AVAILABLE else None,
                              'spawn': mp_dill.get_context('spawn')}

DEFAULT_START_METHOD = 'fork' if FORK_AVAILABLE else 'spawn'
