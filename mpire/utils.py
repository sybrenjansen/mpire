import heapq
import itertools
import math
import os
from datetime import datetime, timedelta
from multiprocessing import Array, cpu_count
from typing import Callable, Generator, Iterable, List, Optional, Tuple, Union

try:
    import numpy as np
    NUMPY_INSTALLED = True
except ImportError:
    np = None
    NUMPY_INSTALLED = False

from mpire.context import RUNNING_WINDOWS

# Needed for setting CPU affinity
if RUNNING_WINDOWS:
    try:
        import win32api
        import win32con
        import win32process
        WIN32API_AVAILABLE = True
        WIN32API_ERROR = None
    except ImportError as e:
        WIN32API_AVAILABLE = False
        WIN32API_ERROR = e
        WIN32API_ERROR.msg += " If you're using Conda, you can run `conda install pywin32` to install the missing " \
                              "module."


def set_cpu_affinity(pid: int, mask: List[int]) -> None:
    """
    Sets the CPU affinity for a given process.

    On Windows-based systems with more than 64 processors, I'm not sure if this will work. See
    https://docs.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-setprocessaffinitymask#parameters.

    :param pid: Process ID
    :param mask: List of CPU IDs
    """
    if RUNNING_WINDOWS:
        # On Conda systems simply install pywin32 doesn't always work. In those cases, you need to run
        # `conda install pywin32`.
        if not WIN32API_AVAILABLE:
            raise WIN32API_ERROR

        # Convert mask to something Windows understands
        windows_mask = 0
        for cpu_id in mask:
            windows_mask ^= 2 ** cpu_id

        # Get handle and set affinity
        handle = win32api.OpenProcess(win32con.PROCESS_ALL_ACCESS, True, pid)
        win32process.SetProcessAffinityMask(handle, windows_mask)
    else:
        os.sched_setaffinity(pid, mask)


def chunk_tasks(iterable_of_args: Iterable, iterable_len: Optional[int] = None,
                chunk_size: Optional[Union[int, float]] = None, n_splits: Optional[int] = None) \
        -> Generator[Iterable, None, None]:
    """
    Chunks tasks such that individual workers will receive chunks of tasks rather than individual ones, which can
    speed up processing drastically.

    :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker, which
        passes it to the function
    :param iterable_len: Number of tasks available in ``iterable_of_args``. Only needed when ``iterable_of_args`` is a
        generator
    :param chunk_size: Number of simultaneous tasks to give to a worker. If ``None``, will use ``n_splits`` to determine
        the chunk size
    :param n_splits: Number of splits to use when ``chunk_size`` is ``None``
    :return: Generator of chunked task arguments
    """
    if chunk_size is None and n_splits is None:
        raise ValueError("chunk_size and n_splits cannot both be None")

    # Determine chunk size
    if chunk_size is None:
        # Get number of tasks
        if iterable_len is not None:
            n_tasks = iterable_len
        elif hasattr(iterable_of_args, '__len__'):
            n_tasks = len(iterable_of_args)
        else:
            raise ValueError('Failed to obtain length of iterable when chunk size is None. Remedy: either provide an '
                             'iterable with a len() function or specify iterable_len in the function call')

        # Determine chunk size
        chunk_size = n_tasks / n_splits

    # Chunk tasks
    args_iter = iter(iterable_of_args)
    current_chunk_size = chunk_size
    n_elements_returned = 0
    while True:
        # Use numpy slicing if available. We use max(1, ...) to always at least get one element
        if NUMPY_INSTALLED and isinstance(iterable_of_args, np.ndarray):
            chunk = iterable_of_args[n_elements_returned:n_elements_returned + max(1, math.ceil(current_chunk_size))]
        else:
            chunk = tuple(itertools.islice(args_iter, max(1, math.ceil(current_chunk_size))))

        # If we ran out of input, we stop
        if len(chunk) == 0:
            return

        # If the iterable has more elements than the given iterable length, we stop
        if iterable_len is not None and n_elements_returned + len(chunk) > iterable_len:
            chunk = chunk[:iterable_len - n_elements_returned]
            if chunk:
                yield chunk
            return

        yield chunk
        current_chunk_size = (current_chunk_size + chunk_size) - math.ceil(current_chunk_size)
        n_elements_returned += len(chunk)


def apply_numpy_chunking(iterable_of_args: Iterable, iterable_len: Optional[int] = None,
                         chunk_size: Optional[int] = None, n_splits: Optional[int] = None,
                         n_jobs: Optional[int] = None) -> Tuple[Iterable, int, int, None]:
    """
    If we're dealing with numpy arrays, chunk them using numpy slicing and return changed map parameters

    :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker, which
        passes it to the function
    :param iterable_len: When chunk_size is set to ``None`` it needs to know the number of tasks. This can either be
        provided by implementing the ``__len__`` function on the iterable object, or by specifying the number of tasks
    :param chunk_size: Number of simultaneous tasks to give to a worker. If ``None``, will generate ``n_jobs * 4``
        number of chunks
    :param n_splits: Number of splits to use when ``chunk_size`` is ``None``
    :param n_jobs: Number of workers to spawn. If ``None``, will use ``cpu_count()``.
    :return: Chunked ``iterable_of_args`` with updated ``iterable_len``, ``chunk_size`` and ``n_splits``
    """
    if iterable_len is not None:
        iterable_of_args = iterable_of_args[:iterable_len]
    iterable_len = get_n_chunks(iterable_of_args, iterable_len, chunk_size, n_splits, n_jobs)
    iterable_of_args = make_single_arguments(chunk_tasks(iterable_of_args, len(iterable_of_args), chunk_size,
                                                         n_splits or (n_jobs * 4 if n_jobs is not None else None)))
    chunk_size = 1
    n_splits = None

    return iterable_of_args, iterable_len, chunk_size, n_splits


def get_n_chunks(iterable_of_args: Iterable, iterable_len: Optional[int] = None, chunk_size: Optional[int] = None,
                 n_splits: Optional[int] = None, n_jobs: Optional[int] = None) -> int:
    """
    Get number of chunks

    :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker, which
        passes it to the function
    :param iterable_len: Number of tasks available in ``iterable_of_args``. Only needed when ``iterable_of_args`` is a
        generator
    :param chunk_size: Number of simultaneous tasks to give to a worker. If ``None``, will use ``n_splits`` to determine
        the chunk size
    :param n_splits: Number of splits to use when ``chunk_size`` is ``None``
    :param n_jobs: Number of workers to spawn. If ``None``, will use ``cpu_count()``
    :return: Number of chunks that will be created by the chunker
    """
    # Get number of tasks
    if iterable_len is not None:
        n_tasks = min(iterable_len, len(iterable_of_args)) if hasattr(iterable_of_args, '__len__') else iterable_len
    elif hasattr(iterable_of_args, '__len__'):
        n_tasks = len(iterable_of_args)
    else:
        raise ValueError('Failed to obtain length of iterable. Remedy: either provide an iterable with a len() '
                         'function or specify iterable_len in the function call')

    # Determine chunk size
    if chunk_size is None:
        chunk_size = n_tasks / (n_splits or (n_jobs or cpu_count()) * 4)

    return min(n_tasks, math.ceil(n_tasks / chunk_size))


def make_single_arguments(iterable_of_args: Iterable, generator: bool = True) -> Union[List, Generator]:
    """
    Converts an iterable of single arguments to an iterable of single argument tuples

    :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker, which
        passes it to the function
    :param generator: Whether or not to return a generator, otherwise a materialized list will be returned
    :return: Iterable of single argument tuples
    """
    gen = ((arg,) for arg in iterable_of_args)
    return gen if generator else list(gen)


def format_seconds(seconds: Optional[Union[int, float]], with_milliseconds: bool) -> str:
    """
    Format seconds to a string, without milliseconds

    :param seconds: Number of seconds
    :param with_milliseconds: Whether to display milliseconds as well
    :return: String formatted time
    """
    if seconds is None:
        return ''

    # Format to hours:minutes:seconds.milliseconds. Only the first 3 digits of the milliseconds is shown
    duration = str(timedelta(seconds=seconds)).rsplit('.', 1)
    if with_milliseconds and len(duration) > 1:
        duration = f'{duration[0]}.{duration[1][:3]}'
    else:
        duration = duration[0]

    return duration


class TimeIt:

    """ Simple class that provides a context manager for keeping track of task duration and adds the total number
     of seconds in a designated output array """

    def __init__(self, cum_time_array: Optional[Array], array_idx: int, max_time_array: Optional[Array] = None,
                 format_args_func: Optional[Callable] = None) -> None:
        """
        :param cum_time_array: Optional array to store cumulative time in
        :param array_idx: Index of cum_time_array to store the time value to
        :param max_time_array: Optional array to store maximum time duration in. Note that the array_idx doesn't apply
            to this array. The entire array is used for heapq
        :param format_args_func: Optional function which should return the formatted args corresponding to the function
            called within this context manager
        """
        self.cum_time_array = cum_time_array
        self.array_idx = array_idx
        self.max_time_array = max_time_array
        self.format_args_func = format_args_func
        self.start_dt = None

    def __enter__(self) -> None:
        self.start_dt = datetime.now()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        duration = (datetime.now() - self.start_dt).total_seconds()
        if self.cum_time_array is not None:
            self.cum_time_array[self.array_idx] += duration
        if self.max_time_array is not None and duration > self.max_time_array[0][0]:
            heapq.heappushpop(self.max_time_array,
                              (duration, self.format_args_func() if self.format_args_func is not None else None))
