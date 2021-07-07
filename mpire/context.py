import multiprocess as mp
import threading


# Threading context so we can use threading as backend as well
class ThreadingContext:

    Event = threading.Event
    Lock = threading.Lock
    Thread = threading.Thread

    # threading doesn't have Array and JoinableQueue, so we take it from multiprocessing. Both are thread-safe. We need
    # the Process class for the MPIRE insights SyncManager instance.
    Array = mp.Array
    JoinableQueue = mp.JoinableQueue
    Process = mp.Process


MP_CONTEXTS = {'fork': mp.get_context('fork'),
               'forkserver': mp.get_context('forkserver'),
               'spawn': mp.get_context('spawn'),
               'threading': ThreadingContext}
