.. _workerID:

Accessing the worker ID
=======================

.. contents:: Contents
    :depth: 2
    :local:

Each worker in MPIRE is given an integer ID to distinguish them. Worker #1 will have ID ``0``, #2 will have ID ``1``,
etc. Sometimes it can be useful to have access to this ID.

By default, the worker ID is not passed on. You can enable/disable this by setting the ``pass_worker_id`` flag:

.. code-block:: python

    def task(worker_id, x):
        pass

    with WorkerPool(n_jobs=4, pass_worker_id=True) as pool:
        pool.map(task, range(10))

.. important::

    The worker ID will always be the first argument passed on to the provided function.

Instead of passing the flag to the :obj:`mpire.WorkerPool` constructor you can also make use of
:meth:`mpire.WorkerPool.pass_on_worker_id`:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        pool.pass_on_worker_id()
        pool.map(task, range(10))

Elaborate example
-----------------

Here's a more elaborate example of using the worker ID together with a shared array, where each worker can only access
the element corresponding to its worker ID, making the use of locking unnecessary:

.. code-block:: python

    def square_sum(worker_id, shared_objects, x):
        # Even though the shared objects is a single container, we 'unpack' it anyway
        results_container = shared_objects

        # Square and sum
        results_container[worker_id] += x * x

    # Use a shared array of size equal to the number of jobs to store the results
    results_container = Array('f', 4, lock=False)

    with WorkerPool(n_jobs=4, shared_objects=results_container, pass_worker_id=True) as pool:
        # Square the results and store them in the results container
        pool.map_unordered(square_sum, range(100))
