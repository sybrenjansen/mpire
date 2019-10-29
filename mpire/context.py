import threading

# If multiprocess is installed we want to use that as it has more capabilities than regular multiprocessing (e.g.,
# pickling lambdas en functions located in __main__)
try:
    import multiprocess as mp
except ImportError:
    import multiprocessing as mp


# Threading context so we can use threading as backend as well
class ThreadingContext:

    Event = threading.Event
    Lock = threading.Lock
    Thread = threading.Thread

    # threading doesn't have Array and JoinableQueue, so we take it from multiprocessing. Both are thread-safe
    Array = mp.Array
    JoinableQueue = mp.JoinableQueue


MP_CONTEXTS = {'fork': mp.get_context('fork'),
               'forkserver': mp.get_context('forkserver'),
               'spawn': mp.get_context('spawn'),
               'threading': ThreadingContext}
