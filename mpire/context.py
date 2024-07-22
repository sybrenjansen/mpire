import multiprocessing as mp
from multiprocessing.context import BaseContext
import platform
from queue import Full
import threading

try:
    import faster_fifo
except ImportError:
    faster_fifo = None

try:
    import multiprocess as mp_dill
    import multiprocess.managers  # Needed in utils.py
except ImportError:
    mp_dill = None

# Check if fork is available as start method. It's not available on Windows machines
try:
    mp.get_context('fork')
    FORK_AVAILABLE = True
except ValueError:
    FORK_AVAILABLE = False

# Check if we're running on Windows or MacOS
RUNNING_WINDOWS = platform.system() == "Windows"
RUNNING_MACOS = platform.system() == "Darwin"


# Threading context so we can use threading as backend as well
class ThreadingContext:

    Barrier = threading.Barrier
    Condition = threading.Condition
    Event = threading.Event
    Lock = threading.Lock
    RLock = threading.RLock
    Thread = threading.Thread

    # threading doesn't have Array and JoinableQueue, so we take it from multiprocessing. Both are thread-safe. We need
    # the Process class for the MPIRE insights SyncManager instance.
    Array = mp.Array
    JoinableQueue = mp.JoinableQueue
    Process = mp.Process
    Value = mp.Value
    

if faster_fifo is not None:
    
    class FasterFifoJoinableQueue(faster_fifo.Queue):

        def __init__(self, maxsize=0, *, ctx):
            faster_fifo.Queue.__init__(self, maxsize, ctx=ctx)
            self._unfinished_tasks = ctx.Semaphore(0)
            self._cond = ctx.Condition()

        def __getstate__(self):
            return faster_fifo.Queue.__getstate__(self) + (self._cond, self._unfinished_tasks)

        def __setstate__(self, state):
            faster_fifo.Queue.__setstate__(self, state[:-2])
            self._cond, self._unfinished_tasks = state[-2:]

        def put(self, obj, block=True, timeout=None):
            if self._closed:
                raise ValueError(f"Queue {self!r} is closed")
            if not self._sem.acquire(block, timeout):
                raise Full

            with self._notempty, self._cond:
                if self._thread is None:
                    self._start_thread()
                self._buffer.append(obj)
                self._unfinished_tasks.release()
                self._notempty.notify()

        def task_done(self):
            with self._cond:
                if not self._unfinished_tasks.acquire(False):
                    raise ValueError('task_done() called too many times')
                if self._unfinished_tasks._semlock._is_zero():
                    self._cond.notify_all()

        def join(self):
            with self._cond:
                if not self._unfinished_tasks._semlock._is_zero():
                    self._cond.wait()
        
    
    if FORK_AVAILABLE:
        class FasterFifoForkContext(BaseContext):
            _name = 'faster_fifo_fork'
            Process = mp.get_context('fork').Process

            def Queue(self):
                '''Returns a queue object'''
                return faster_fifo.Queue(max_size_bytes=1000000)

            def JoinableQueue(self):
                '''Returns a queue object'''
                return FasterFifoJoinableQueue(max_size_bytes=1000000, ctx=self.get_context())


MP_CONTEXTS = {'mp': {'fork': mp.get_context('fork') if FORK_AVAILABLE else None,
                      'forkserver': mp.get_context('forkserver') if FORK_AVAILABLE else None,
                      'spawn': mp.get_context('spawn')},
               'threading': ThreadingContext}
if mp_dill is not None:
    MP_CONTEXTS['mp_dill'] = {'fork': mp_dill.get_context('fork') if FORK_AVAILABLE else None,
                              'forkserver': mp_dill.get_context('forkserver') if FORK_AVAILABLE else None,
                              'spawn': mp_dill.get_context('spawn')}
if faster_fifo is not None:
    MP_CONTEXTS['mp_faster_fifo'] = {'fork': FasterFifoForkContext() if FORK_AVAILABLE else None}

DEFAULT_START_METHOD = 'fork' if FORK_AVAILABLE else 'spawn'
