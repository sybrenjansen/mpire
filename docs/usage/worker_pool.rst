WorkerPool
==========

This section describes the different ways of creating and setting up a :obj:`mpire.WorkerPool` instance.

.. contents:: Contents
    :depth: 2
    :local:

Starting a WorkerPool
---------------------

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

    In the manual approach, the results queue should be drained first before joining the workers, otherwise you can get
    a deadlock. If you want to join either way, use :meth:`mpire.WorkerPool.terminate`. For more information, see the
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


CPU pinning
-----------

You can pin the child processes of :obj:`mpire.WorkerPool` to specific CPUs by using the ``cpu_ids`` parameter in the
constructor:

.. code-block:: python

    # Pin the two child processes to CPUs 2 and 3
    with WorkerPool(n_jobs=2, cpu_ids=[2, 3]) as pool:
        ...

    # Pin the child processes to CPUs 40-59
    with WorkerPool(n_jobs=20, cpu_ids=list(range(40, 60))) as pool:
        ...

    # All child processes have to share a single core:
    with WorkerPool(n_jobs=4, cpu_ids=[0]) as pool:
        ...

    # All child processes have to share multiple cores, namely 4-7:
    with WorkerPool(n_jobs=4, cpu_ids=[[4, 5, 6, 7]]) as pool:
        ...

    # Each child process can use two distinctive cores:
    with WorkerPool(n_jobs=4, cpu_ids=[[0, 1], [2, 3], [4, 5], [6, 7]]) as pool:
        ...

CPU IDs have to be positive integers, not exceeding the number of CPUs available (which can be retrieved by using
``mpire.cpu_count()``). Use ``None`` to disable CPU pinning (which is the default).

.. note::

    Pinning processes to CPU IDs doesn't work when using threading.


.. _workerID:


Accessing the worker ID
-----------------------

Each worker in MPIRE is given an integer ID to distinguish them. Worker #1 will have ID ``0``, #2 will have ID ``1``,
etc. Sometimes it can be useful to have access to this ID. For example, when you have a shared array of which the size
equals the number of workers and you want worker #1 only to access the first element, and worker #2 only to access the
second element, and so on.

By default, the worker ID is not passed on. You can enable/disable this by setting the ``pass_worker_id`` flag:

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

.. important::

    The worker ID will always be the first argument passed on to the provided function pointer.

Instead of passing the flag to the :obj:`mpire.WorkerPool` constructor you can also make use of
:meth:`mpire.WorkerPool.pass_on_worker_id`:

.. code-block:: python

    with WorkerPool(n_jobs=4, shared_objects=results_container) as pool:
        pool.pass_on_worker_id()
        pool.map_unordered(square_sum, range(100))


.. _shared_objects:

Shared objects
--------------

MPIRE allows you to provide shared objects to the workers in a similar way as is possible with the
``multiprocessing.Process`` class. For the start methods ``fork`` and ``threading`` these shared objects are treated as
``copy-on-write``, which means they are only copied once changes are made to them. Otherwise they share the same memory
address. This is convenient if you want to let workers access a large dataset that wouldn't fit in memory when copied
multiple times. For the start methods ``spawn`` and ``forkserver`` the shared objects are copied only once for each
worker, in contrast to copying it for each task which is done when using a regular ``multiprocessing.Pool``.

.. code-block:: python

    def task(dataset, x):
        # Do something with this copy-on-write dataset
        ...

    def main():
        dataset = ... # Load big dataset
        with WorkerPool(n_jobs=4, shared_objects=dataset, start_method='fork') as pool:
            ... = pool.map(task, range(100))

Apart from sharing regular Python objects between workers, you can also share multiprocessing synchronization
primitives such as ``multiprocessing.Lock`` using this method. Objects like these require to be shared through
inheritance, which is exactly how shared objects in MPIRE are passed on.

When copy-on-write is not available for you, you can also use shared objects to share a ``multiprocessing.Array``,
``multiprocessing.Value``, or another object with ``multiprocessing.Manager``. You can then store results in the same
object from multiple processes. However, you should keep the amount of synchronization to a minimum when the resources
are protected with a lock, or disable locking if your situation allows it as is shown here:

.. code-block:: python

    from multiprocessing import Array

    def square_add_and_modulo_with_index(shared_objects, idx, x):
        # Unpack results containers
        square_results_container, add_results_container = shared_objects

        # Square, add and modulo
        square_results_container[idx] = x * x
        add_results_container[idx] = x + x
        return x % 2

    def main():
        # Use a shared array of size 100 and type float to store the results
        square_results_container = Array('f', 100, lock=False)
        add_results_container = Array('f', 100, lock=False)
        shared_objects = square_results_container, add_results_container
        with WorkerPool(n_jobs=4, shared_objects=shared_objects) as pool:

            # Square, add and modulo the results and store them in the results containers
            modulo_results = pool.map(square_add_and_modulo_with_index,
                                      enumerate(range(100)), iterable_len=100)

Multiple objects can be provided by placing them, for example, in a tuple container as is shown above.

.. important::

    Shared objects are passed on as the second argument, after the worker ID (when enabled), to the provided function
    pointer.

In the example above we create two results containers, one for squaring and for adding the given value, and disable
locking for both. Additionally, we also return a value, even though we use shared objects for storing results. We can
safely disable locking here as each task writes to a different index in the array, so no race conditions can occur.
Disabling locking is, of course, a lot faster than enabling it

Instead of passing the shared objects to the :obj:`mpire.WorkerPool` constructor you can also use the
:meth:`mpire.WorkerPool.set_shared_objects` function:

.. code-block:: python

    def square_with_index(shared_objects, idx, x):
        results_container = shared_objects
        results_container[idx] = x * x

    results_container = Array('f', 100, lock=False)

    with WorkerPool(n_jobs=4) as pool:
        pool.set_shared_objects(results_container)
        pool.map_unordered(square_with_index, enumerate(range(100)),
                           iterable_len=100)

Shared objects have to be specified before the workers are started. Workers are started once the first ``map`` call is
executed. I.e., when ``keep_alive=True`` and the workers are reused, changing the shared objects between two consecutive
``map`` calls won't work.

.. _worker_state:

Worker state
------------

If you want to let each worker have its own state you can use the ``use_worker_state`` flag. The worker state can be
combined with the ``worker_init`` and ``worker_exit`` parameters of each ``map`` function, leading to some really useful
capabilities:

.. code-block:: python

    import numpy as np
    import pickle

    def load_big_model(worker_state):
        # Load a model which takes up a lot of memory
        with open('./a_really_big_model.p3', 'rb') as f:
            worker_state['model'] = pickle.load(f)

    def model_predict(worker_state, x):
        # Predict
        return worker_state['model'].predict(x)

    with WorkerPool(n_jobs=4, use_worker_state=True) as pool:
        # Let the model predict
        data = np.array([[...]])
        results = pool.map(model_predict, data, worker_init=load_big_model)

.. important::

    The worker state is passed on as the third argument, after the worker ID and shared objects (when enabled), to the
    provided function pointer.

More information about the ``worker_init`` and ``worker_exit`` parameters can be found at :ref:`worker_init_exit`.

Instead of passing the flag to the :obj:`mpire.WorkerPool` constructor you can also make use of
:meth:`mpire.WorkerPool.set_use_worker_state`:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        pool.set_use_worker_state()
        results = pool.map(model_predict, data, worker_init=load_big_model)

.. _start_methods:

Process start method
--------------------

The ``multiprocessing`` package allows you to start processes using a few different methods: ``'fork'``, ``'spawn'`` or
``'forkserver'``. Threading is also available by using ``'threading'``. For detailed information on the multiprocessing
contexts, please refer to the multiprocessing documentation_ and caveats_ section. In short:

- ``'fork'`` (the default on Unix based systems) copies the parent process such that the child process is effectively
  identical. This includes copying everything currently in memory. This is sometimes useful, but other times useless or
  even a serious bottleneck.
- ``'spawn'`` (the default on Windows) starts a fresh python interpreter where only those resources necessary are
  inherited.
- ``'forkserver'`` first starts a server process (using spawn). Whenever a new process is needed the parent process
  requests the server to fork a new process.
- ``'threading'`` starts child threads.

Be aware that global variables (constants are fine) might have a different value than you might expect. You also have to
import packages within the called function:

.. code-block:: python

    import os

    def failing_job(folder, filename):
        return os.path.join(folder, filename)

    # This will fail because 'os' is not copied to the child processes
    with WorkerPool(n_jobs=2, start_method='spawn') as pool:
        pool.map(failing_job, [('folder', '0.p3'), ('folder', '1.p3')])

.. code-block:: python

    def working_job(folder, filename):
        import os
        return os.path.join(folder, filename)

    # This will work
    with WorkerPool(n_jobs=2, start_method='spawn') as pool:
        pool.map(working_job, [('folder', '0.p3'), ('folder', '1.p3')])

A lot of effort has been put into making the progress bar, dashboard, and nested pools (with multiple progress bars)
work well with ``spawn`` and ``forkserver``. So, everything should work fine.

Keep alive
----------

Workers can be kept alive in between consecutive map calls using the ``keep_alive`` flag. This is useful when your
workers have a long startup time and you need to call one of the map functions multiple times. When either the function
to execute or the ``worker_lifespan`` parameter changes MPIRE will ignore the flag as it needs to restart the workers
anyway.

Building further on the worker state example:

.. code-block:: python

    import numpy as np
    import pickle

    def load_big_model():
        # Load a model which takes up a lot of memory
        with open('./a_really_big_model.p3', 'rb') as f:
            return pickle.load(f)

    def model_predict(worker_state, x):
        # Load model
        if 'model' not in worker_state:
            worker_state['model'] = load_big_model()

        # Predict
        return worker_state['model'].predict(x)

    with WorkerPool(n_jobs=4, use_worker_state=True, keep_alive=True) as pool:
        # Let the model predict
        data = np.array([[...]])
        results = pool.map(model_predict, data)

        # Do something with the results
        ...

        # Let the model predict some more. In this call the workers are reused,
        # which means the big model doesn't need to be loaded again
        data = np.array([[...]])
        results = pool.map(model_predict, data)

        # Workers are restarted in this case because the function changed
        pool.map(square_sum, range(100))

Instead of passing the flag to the :obj:`mpire.WorkerPool` constructor you can also make use of
:meth:`mpire.WorkerPool.set_keep_alive`:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        pool.set_keep_alive()
        pool.map_unordered(square_sum, range(100))


.. _use_dill:

Dill
----

For some functions or tasks it can be useful to not rely on pickle, but on some more powerful serialization backends
like dill_. ``dill`` isn't installed by default. See :ref:`dilldep` for more information on installing the dependencies.

For all benefits of ``dill``, please refer to the `dill documentation`_.

Once the dependencies have been installed, you can enable it using the ``use_dill`` flag:

.. code-block:: python

    with WorkerPool(n_jobs=4, use_dill=True) as pool:
        ...

.. note::

    When using ``dill`` it can potentially slow down processing. This is the cost of having a more reliable and
    powerful serialization backend.

.. _documentation: https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods
.. _caveats: https://docs.python.org/3/library/multiprocessing.html#the-spawn-and-forkserver-start-methods
.. _dill: https://pypi.org/project/dill/
.. _dill documentation: https://github.com/uqfoundation/dill
