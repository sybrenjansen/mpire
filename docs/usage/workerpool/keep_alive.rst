.. _keep_alive:

Keep alive
==========

.. contents:: Contents
    :depth: 2
    :local:

By default, workers are restarted on each ``map`` call. This is done to clean up resources as quickly as possible when
the work is done.

Workers can be kept alive in between consecutive map calls using the ``keep_alive`` flag. This is useful when your
workers have a long startup time and you need to call one of the map functions multiple times.

.. code-block:: python

    def foo(x):
        pass

    with WorkerPool(n_jobs=4, keep_alive=True) as pool:
        pool.map(task, range(100))
        pool.map(task, range(100))  # Workers are reused here

Instead of passing the flag to the :obj:`mpire.WorkerPool` constructor you can also make use of
:meth:`mpire.WorkerPool.set_keep_alive`:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        pool.map(task, range(100))
        pool.map(task, range(100))  # Workers are restarted
        pool.set_keep_alive()
        pool.map(task, range(100))  # Workers are reused here

Caveats
-------

Changing some WorkerPool init parameters do require a restart. These include ``pass_worker_id``, ``shared_objects``, and
``use_worker_state``.

Keeping workers alive works even when the function to be called or any other parameter passed on to the ``map`` function
changes.

However, when you're changing either the ``worker_init`` and/or ``worker_exit`` function while ``keep_alive`` is
enabled, you need to be aware this can have undesired side-effects. ``worker_init`` functions are only executed when a
worker is started and ``worker_exit`` functions when a worker is terminated. When ``keep_alive`` is enabled, workers
aren't restarted in between consecutive ``map`` calls, so those functions are not called.

.. code-block:: python

    def init_func_1(): pass
    def exit_func_1(): pass

    def init_func_2(): pass
    def init_func_2(): pass

    with WorkerPool(n_jobs=4, keep_alive=True) as pool:
        pool.map(task, range(100), worker_init=init_func_1, worker_exit=exit_func_1)
        pool.map(task, range(100), worker_init=init_func_2, worker_exit=exit_func_2)

In the above example ``init_func_1`` is called for each worker when the workers are started. After the first ``map``
call ``exit_func_1`` is not called because workers are kept alive. During the second ``map`` call ``init_func_2`` isn't
called as well, because the workers are still alive. When exiting the context manager the workers are shut down and
``exit_func_2`` is called.

It gets even trickier when you also enable ``worker_lifespan``. In this scenario during the first ``map`` call a worker
could've reached its maximum lifespan and is forced to restart, while others haven't. The exit function of the worker to
be restarted is called (i.e., ``exit_func_1``). When calling ``map`` for the second time and the exit function is
changed, the other workers will execute the new exit function when they need to be restarted (i.e., ``exit_func_2``).
