import itertools
import math

import numpy as np


def chunk_tasks(iterable_of_args, iterable_len=None, chunk_size=None, n_splits=None):
    """
    Chunks tasks such that individual workers will receive chunks of tasks rather than individual ones, which can
    speed up processing drastically.

    :param iterable_of_args: An iterable containing tuples of arguments to pass to a worker, which passes it to the
        function pointer
    :param iterable_len: Int. Number of tasks available in ``iterable_of_args``. Only needed when ``iterable_of_args``
        is a generator.
    :param chunk_size: Int or ``None``. Number of simultaneous tasks to give to a worker. If ``None``, will use
        ``n_splits`` to determine the chunk size
    :param n_splits: Int or ``None``. Number of splits to use when ``chunk_size`` is ``None``.
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
    numpy_row_offset = 0
    args_iter = iter(iterable_of_args)
    current_chunk_size = chunk_size
    n_elements_returned = 0
    while True:
        # Use numpy slicing if available. We use max(1, ...) to always at least get one element
        if isinstance(iterable_of_args, np.ndarray):
            chunk = iterable_of_args[numpy_row_offset:numpy_row_offset + max(1, math.ceil(current_chunk_size))]
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
        numpy_row_offset += len(chunk)
        n_elements_returned += len(chunk)


def make_single_arguments(iterable_of_args, generator=True):
    """
    Converts an iterable of single arguments to an iterable of single argument tuples

    :param iterable_of_args: An iterable containing tuples of arguments to pass to a worker, which passes it to the
        function pointer
    :param generator: Whether or not to return a generator, otherwise a materialized list will be returned
    :return: iterable of single argument tuples
    """
    gen = ((arg,) for arg in iterable_of_args)
    return gen if generator else list(gen)
