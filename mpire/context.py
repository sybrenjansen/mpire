import multiprocessing as mp
try:
    import multiprocess as mp_dill
except ImportError:
    mp_dill = None
import threading


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


MP_CONTEXTS = {'mp': {'fork': mp.get_context('fork'),
                      'forkserver': mp.get_context('forkserver'),
                      'spawn': mp.get_context('spawn')},
               'threading': ThreadingContext}
if mp_dill is not None:
    MP_CONTEXTS['mp_dill'] = {'fork': mp_dill.get_context('fork'),
                              'forkserver': mp_dill.get_context('forkserver'),
                              'spawn': mp_dill.get_context('spawn')}
