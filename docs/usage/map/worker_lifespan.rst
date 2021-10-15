Worker lifespan
===============

Occasionally, workers that process multiple, memory intensive tasks do not release their used up memory properly, which
results in memory usage building up. This is not a bug in MPIRE, but a consequence of Python's poor garbage collection.
To avoid this type of problem you can set the worker lifespan: the number of tasks after which a worker should restart.

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        results = pool.map(task, range(100), worker_lifespan=1, chunk_size=1)

In this example each worker is restarted after finishing a single task.

.. note::

    When the worker lifespan has been reached, a worker will finish the current chunk of tasks before restarting. I.e.,
    based on the ``chunk_size`` a worker could end up completing more tasks than is allowed by the worker lifespan.
