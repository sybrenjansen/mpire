Order tasks
===========

.. contents:: Contents
    :depth: 2
    :local:

In some settings it can be useful to supply the tasks to workers in order. This means worker 0 will get task 0, worker
1 will get task 1, etc. After each worker got a task, we start with worker 0 again instead of picking the worker that
has most recently completed a task.

When the chunk size is larger than 1, the tasks are distributed to the workers in order, but in chunks. I.e., when
``chunk_size=3`` tasks 0, 1, and 2 will be assigned to worker 0, tasks 3, 4, and 5 to worker 1, and so on.

When ``keep_alive`` is set to ``True`` and the second ``map`` call is made, MPIRE resets the worker order and starts at
worker 0 again.

You can enable/disable this by setting the ``order_tasks`` flag:

.. code-block:: python

    def task(x):
        pass

    with WorkerPool(n_jobs=4, order_tasks=True) as pool:
        pool.map(task, range(10))

Instead of passing the flag to the :obj:`mpire.WorkerPool` constructor you can also make use of
:meth:`mpire.WorkerPool.set_order_tasks`:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        pool.set_order_tasks()
        pool.map(task, range(10))
