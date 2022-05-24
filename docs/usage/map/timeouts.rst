.. _timeouts:

Timeouts
========

Timeouts can be set separately for the target, ``worker_init`` and ``worker_exit`` functions. When a timeout has been
set and reached, it will throw a ``TimeoutError``:

.. code-block:: python

    # Will raise TimeoutError, provided that the target function takes longer
    # than half a second to complete
    with WorkerPool(n_jobs=5) as pool:
        pool.map(time_consuming_function, range(10), task_timeout=0.5)

    # Will raise TimeoutError, provided that the worker_init function takes longer
    # than 3 seconds to complete or the worker_exit function takes longer than
    # 150.5 seconds to complete
    with WorkerPool(n_jobs=5) as pool:
        pool.map(time_consuming_function, range(10), worker_init=init, worker_exit=exit_,
                 worker_init_timeout=3.0, worker_exit_timeout=150.5)

Use ``None`` (=default) to disable timeouts.

``imap`` and ``imap_unordered``
-------------------------------

When you're using one of the lazy map functions (e.g., ``imap`` or ``imap_unordered``) then an exception will only be
raised when the function is actually running. E.g. when executing:

.. code-block:: python

    with WorkerPool(n_jobs=5) as pool:
        results = pool.imap(time_consuming_function, range(10), task_timeout=0.5)

this will never raise. This is because ``imap`` and ``imap_unordered`` return a generator object, which stops executing
until it gets the trigger to go beyond the ``yield`` statement. When iterating through the results, it will raise as
expected:

.. code-block:: python

    with WorkerPool(n_jobs=5) as pool:
        results = pool.imap(time_consuming_function, range(10), task_timeout=0.5)
        for result in results:
            ...

Threading
---------

When using ``threading`` as start method MPIRE won't be able to interrupt certain functions, like ``time.sleep``.