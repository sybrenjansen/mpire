Setup
=====

This section describes the different ways of creating a :obj:`mpire.WorkerPool` object.

.. contents:: Contents
    :depth: 2
    :local:

Starting a WorkerPool
---------------------

The :obj:`mpire.WorkerPool` class controls a pool of worker processes similarly to a ``multiprocessing.Pool``. It
contains all the ``map`` like functions (with the addition of :meth:`mpire.WorkerPool.map_unordered`), but currently
lacks the ``apply`` and ``apply_async`` functions (if you wish to add it, feel free to do so).

An :obj:`mpire.WorkerPool` can be started in two different ways. The first and recommended way to do so is using a
context manager:

.. code-block:: python

    from mpire import WorkerPool

    # Start a pool of 4 workers
    with WorkerPool(n_jobs=4) as pool:
        # Do some processing here
        pass

The ``with`` statement takes care of properly joining/terminating the spawned worker processes after the block has
ended.

The other way is to do it manually:

.. code-block:: python

    # Start a pool of 4 workers
    pool = WorkerPool(n_jobs=4)

    # Do some processing here
    pass

    # Clean up pool (this will block until all processing has completed)
    pool.stop_and_join()

    # In the case you want to kill the processes even though they are still busy
    pool.terminate()

When using ``n_jobs=None`` MPIRE will spawn as many processes as there are CPUs on your system. Specifying more jobs
than you have CPUs is, of course, possible as well.

.. warning::

    The results queue should be drained first before joining the workers, otherwise you can get a deadlock. If you want
    to join either way, use :meth:`mpire.WorkerPool.terminate`. For more information, see the warnings in the Python
    docs here_.

.. _here: https://docs.python.org/3.4/library/multiprocessing.html#pipes-and-queues


Nested WorkerPools
------------------

Normally, the :obj:`mpire.WorkerPool` class spawns daemon child processes who are not able to create child processes
themselves, so nested pools are not allowed. However, there's an option to create normal child processes, instead of
daemon, to allow for nested structures:

.. code-block:: python

    def job(...)
        with WorkerPool(n_jobs=4) as p:
            # Do some work
            results = p.map(...)

    with WorkerPool(n_jobs=4, daemon=True) as pool:
        # This will raise an AssertionError telling you daemon processes can't start child processes
        pool.map(job, ...)

    with WorkerPool(n_jobs=4, daemon=False) as pool:
        # This will work just fine
        pool.map(job, ...)

Do make sure all your non-daemon processes are terminated correctly. If a nested child process is interrupted, for
example when the user triggers a ``KeyboardInterrupt``, the process will remain active and will have to be terminated
manually.


CPU pinning
-----------

If desired you can pin the child processes of :obj:`mpire.WorkerPool` to specific CPUs by using the ``cpu_ids``
parameter in the constructor:

.. code-block:: python

    # Pin the two child processes to CPUs 2 and 3
    with WorkerPool(n_jobs=2, cpu_ids=[2, 3]) as pool:
        ...

    # Pin the child processes to CPUs 40-59
    with WorkerPool(n_jobs=20, cpu_ids=list(range(40, 60))) as pool:
        ...

    # All child processes have to share a single core:
    with WorkerPool(n_jobs=4, cpu_ids=[0]):
        ...

    # All child processes have to share multiple cores, namely 4-7:
    with WorkerPool(n_jobs=4, cpu_ids=[[4, 5, 6, 7]]):
        ...

    # Each child process can use two distinctive cores:
    with WorkerPool(n_jobs=4, cpu_ids=[[0, 1], [2, 3], [4, 5], [6, 7]]):
        ...

CPU IDs have to be positive integers, not exceeding the number of CPUs available (which can be retrieved by using
``mpire.cpu_count()``). Use ``None`` to disable CPU pinning (which is the default).
