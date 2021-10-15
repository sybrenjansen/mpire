Starting a WorkerPool
=====================

.. contents:: Contents
    :depth: 2
    :local:

The :obj:`mpire.WorkerPool` class controls a pool of worker processes similarly to a ``multiprocessing.Pool``. It
contains all the ``map`` like functions (with the addition of :meth:`mpire.WorkerPool.map_unordered`), but currently
lacks the ``apply`` and ``apply_async`` functions.

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

    # Only needed when keep_alive=True:
    # Clean up pool (this will block until all processing has completed)
    pool.stop_and_join()

    # In the case you want to kill the processes, even though they are still busy
    pool.terminate()

When using ``n_jobs=None`` MPIRE will spawn as many processes as there are CPUs on your system. Specifying more jobs
than you have CPUs is, of course, possible as well.

.. warning::

    In the manual approach, the results queue should be drained before joining the workers, otherwise you can get a
    deadlock. If you want to join either way, use :meth:`mpire.WorkerPool.terminate`. For more information, see the
    warnings in the Python docs here_.

.. _here: https://docs.python.org/3/library/multiprocessing.html#pipes-and-queues


Nested WorkerPools
------------------

By default, the :obj:`mpire.WorkerPool` class spawns daemon child processes who are not able to create child processes
themselves, so nested pools are not allowed. There's an option to create non-daemon child processes to allow for nested
structures:

.. code-block:: python

    def job(...)
        with WorkerPool(n_jobs=4) as p:
            # Do some work
            results = p.map(...)

    with WorkerPool(n_jobs=4, daemon=True) as pool:
        # This will raise an AssertionError telling you daemon processes
        # can't start child processes
        pool.map(job, ...)

    with WorkerPool(n_jobs=4, daemon=False) as pool:
        # This will work just fine
        pool.map(job, ...)

.. note::

    Nested pools aren't supported when using threading.

.. note::

    Due to a strange bug in Python, using ``forkserver`` as start method in a nested pool is not allowed when the
    outer pool is using ``fork``, as the forkserver will not have been started there. For it to work your outer pool
    will have to have either ``spawn`` or ``forkserver`` as start method.

.. warning::

    Nested pools aren't production ready. Error handling and keyboard interrupts when using nested pools can, on some
    rare occassions (~1% of the time), still cause deadlocks. Use at your own risk.

    When a function is guaranteed to finish successfully, using nested pools is absolutely fine.
