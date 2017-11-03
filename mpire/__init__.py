try:
    from .mpire import cpu_count, Worker, WorkerPool
except ImportError:
    pass

__version__ = '0.6.0'
