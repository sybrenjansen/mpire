.. _Task chunking:

Task chunking
=============

.. contents:: Contents
    :depth: 2
    :local:

By default, MPIRE chunks the given tasks in to ``64 * n_jobs`` chunks. Each worker is given one chunk of tasks at a time
before returning its results. This usually makes processing faster when you have rather small tasks (computation wise)
and results are pickled/unpickled when they are send to a worker or main process. Chunking the tasks and results ensures
that each process has to pickle/unpickle less often.

However, to determine the number of tasks in the argument list the iterable should implement the ``__len__`` method,
which is available in default containers like ``list`` or ``tuple``, but isn't available in most generator objects
(the ``range`` object is one of the exceptions). To allow working with generators each ``map`` function has the option
to pass the iterable length:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        # 1. This will issue a warning and sets the chunk size to 1
        results = pool.map(square, ((x,) for x in range(1000)))

        # 2. This will issue a warning as well and sets the chunk size to 1
        results = pool.map(square, ((x,) for x in range(1000)), n_splits=4)

        # 3. Square the numbers using a generator using a specific number of splits
        results = pool.map(square, ((x,) for x in range(1000)), iterable_len=1000, n_splits=4)

        # 4. Square the numbers using a generator using automatic chunking
        results = pool.map(square, ((x,) for x in range(1000)), iterable_len=1000)

        # 5. Square the numbers using a generator using a fixed chunk size
        results = pool.map(square, ((x,) for x in range(1000)), chunk_size=4)

In the first two examples the function call will issue a warning because MPIRE doesn't know how large the chunks should
be as the total number of tasks is unknown, therefore it will fall back to a chunk size of 1. The third example should
work as expected where 4 chunks are used. The fourth example uses 256 chunks (the default 64 times the number of
workers). The last example uses a fixed chunk size of four, so MPIRE doesn't need to know the iterable length.

You can also call the chunk function manually:

.. code-block:: python

    from mpire.utils import chunk_tasks

    # Convert to list because chunk_tasks returns a generator
    print(list(chunk_tasks(range(10), n_splits=3)))
    print(list(chunk_tasks(range(10), chunk_size=2.5)))
    print(list(chunk_tasks((x for x in range(10)), iterable_len=10, n_splits=6)))

will output:

.. code-block:: python

    [(0, 1, 2, 3), (4, 5, 6), (7, 8, 9)]
    [(0, 1, 2), (3, 4), (5, 6, 7), (8, 9)]
    [(0, 1), (2, 3), (4,), (5, 6), (7, 8), (9,)]
