.. _apply-family:

Apply family
============

.. contents:: Contents
    :depth: 2
    :local:

:obj:`mpire.WorkerPool` implements two ``apply`` functions, which are very similar to the ones in the
:mod:`multiprocessing` module:

:meth:`mpire.WorkerPool.apply`
    Apply a function to a single task. This is a blocking call.
:meth:`mpire.WorkerPool.apply_async`
    A variant of the above, but which is non-blocking. This returns an :obj:`mpire.async_result.AsyncResult` object.

``apply``
---------

The ``apply`` function is a blocking call, which means that it will not return until the task is completed. If you want
to run multiple different tasks in parallel, you should use the ``apply_async`` function instead. If you require
to run the same function for many tasks in parallel, use the ``map`` functions instead.

The ``apply`` function takes a function, positional arguments, and keyword arguments, similar to how
:mod:`multiprocessing` does it.

.. code-block:: python

    def task(a, b, c, d):
        return a + b + c + d

    with WorkerPool(n_jobs=1) as pool:
        result = pool.apply(task, args=(1, 2), kwargs={'d': 4, 'c': 3})
        print(result)


``apply_async``
---------------

The ``apply_async`` function is a non-blocking call, which means that it will return immediately. It returns an
:obj:`mpire.async_result.AsyncResult` object, which can be used to get the result of the task at a later moment in time.

The ``apply_async`` function takes the same parameters as the ``apply`` function.

.. code-block:: python

    def task(a, b):
        return a + b

    with WorkerPool(n_jobs=4) as pool:
        async_results = [pool.apply_async(task, args=(i, i)) for i in range(10)]
        results = [async_result.get() for async_result in async_results]

Obtaining the results should happen while the pool is still running! E.g., the following will deadlock:

.. code-block::

    with WorkerPool(n_jobs=4) as pool:
        async_results = [pool.apply_async(task, args=(i, i)) for i in range(10)]

    # Will wait forever
    results = [async_result.get() for async_result in async_results]

You can, however, make use of the :meth:`mpire.WorkerPool.stop_and_join()` function to stop the workers and join the
pool. This will make sure that all tasks are completed before the pool exits.

.. code-block::

    with WorkerPool(n_jobs=4) as pool:
        async_results = [pool.apply_async(task, args=(i, i)) for i in range(10)]
        pool.stop_and_join()

    # Will not deadlock
    results = [async_result.get() for async_result in async_results]

AsyncResult
-----------

The :obj:`mpire.async_result.AsyncResult` object has the following convenient methods:

.. code-block:: python

    with WorkerPool(n_jobs=1) as pool:
        async_result = pool.apply_async(task, args=(1, 1))

        # Check if the task is completed
        is_completed = async_result.ready()

        # Wait until the task is completed, or until the timeout is reached.
        async_result.wait(timeout=10)

        # Get the result of the task. This will block until the task is completed,
        # or until the timeout is reached.
        result = async_result.get(timeout=None)

        # Check if the task was successful (i.e., did not raise an exception).
        # This will raise an exception if the task is not completed yet.
        is_successful = async_result.successful()

Callbacks
---------

Each ``apply`` function has a ``callback`` and ``error_callback`` argument. These are functions which are called when
the task is finished. The ``callback`` function is called with the result of the task when the task was completed
successfully, and the ``error_callback`` is called with the exception when the task failed.

.. code-block:: python

    def task(a):
        return a + 1

    def callback(result):
        print("Task completed successfully with result:", result)

    def error_callback(exception):
        print("Task failed with exception:", exception)

    with WorkerPool(n_jobs=1) as pool:
        pool.apply(task, 42, callback=callback, error_callback=error_callback)


Worker init and exit
--------------------

As with the ``map`` family of functions, the ``apply`` family of functions also has ``worker_init`` and ``worker_exit``
arguments. These are functions which are called when a worker is started and stopped, respectively. See
:ref:`worker_init_exit` for more information on these functions.

.. code-block:: python

    def worker_init():
        print("Worker started")

    def worker_exit():
        print("Worker stopped")

    with WorkerPool(n_jobs=5) as pool:
        pool.apply(task, 42, worker_init=worker_init, worker_exit=worker_exit)

There's a caveat though. When the first ``apply`` or ``apply_async`` function is executed, the entire pool of workers
is started. This means that in the above example all five workers are started, while only one was needed. This also
means that the ``worker_init`` function is set for all those workers at once. This means you cannot have a different
``worker_init`` function for each apply task. A second, different ``worker_init`` function will simply be ignored.

Similarly, the ``worker_exit`` function can only be set once as well. Additionally, exit functions are only called when
a worker exits, which in this case translates to when the pool exits. This means that if you call ``apply`` or
``apply_async`` multiple times, the ``worker_exit`` function is only called once at the end. Use
:meth:`mpire.WorkerPool.stop_and_join()` to stop the workers, which will cause the ``worker_exit`` function to be
triggered for each worker.


Timeouts
--------

The ``apply`` family of functions also has ``task_timeout``, ``worker_init_timeout`` and ``worker_exit_timeout``
arguments. These are timeouts for the task, the ``worker_init`` function and the ``worker_exit`` function, respectively.
They work completely the same as for the ``map`` functions. See :ref:`timeouts` for more information.
