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


Shared objects
--------------

MPIRE allows you to provide shared objects to the workers in a similar way as is possible with the
``multiprocessing.Process`` class. These shared objects are treated as ``copy-on-write``, they are only copied once
changes are made to them, otherwise they share the same memory address. This is convenient if you want to let workers
access a large dataset that wouldn't fit in memory when copied multiple times. When shared objects are copied they are
only copied once for each worker, in contrast to copying it for each task which is done when using a regular
``multiprocessing.Pool``.

By using a ``multiprocessing.Array``, ``multiprocessing.Value``, or another object with ``multiprocessing.Manager`` you
could even store results in the same object from multiple processes. However, be aware of the possible locking behavior
that comes with it. However, in some cases you can safely disable locking, as is shown here:

.. code-block:: python

    from multiprocessing import Array

    def square_with_index(shared_objects, idx, x):
        # Even though the shared objects is a single container, we 'unpack' it
        # (only to be consistent with the function below)
        results_container = shared_objects

        # Square
        results_container[idx] = x * x

    def square_add_and_modulo_with_index(shared_objects, idx, x):
        # Unpack results containers
        square_results_container, add_results_container = shared_objects

        # Square, add and modulo
        square_results_container[idx] = x * x
        add_results_container[idx] = x + x
        return x % 2


    # 1. Use a shared array of size 100 and type float to store the results
    results_container = Array('f', 100, lock=False)
    with WorkerPool(n_jobs=4, shared_objects=results_container) as pool:

        # Square the results and store them in the results container
        pool.map_unordered(square_with_index, enumerate(range(100)),
                           iterable_len=100)

    # 2, Use a shared array of size 100 and type float to store the results
    square_results_container = Array('f', 100, lock=False)
    add_results_container = Array('f', 100, lock=False)
    with WorkerPool(n_jobs=4,
                    shared_objects=(square_results_container, add_results_container)) as pool:

        # Square, add and modulo the results and store them in the results containers
        modulo_results = pool.map(square_add_and_modulo_with_index,
                                  enumerate(range(100)), iterable_len=100)

Multiple objects can be provided by placing them, for example, in a tuple container as is done in example two above.
When providing shared objects the provided function pointer in the map functions should receive the shared objects as
its first argument (or the second argument when the worker ID is passed on as well, see :ref:`workerID`).

In the first example (marked ``#1``) we create a results container and disable locking. We can safely disable locking
here as each task writes to a different index in the array, so no race conditions can occur. Disabling locking is, of
course, a lot faster than enabling it.

In the second example we create two different results containers, one for squaring and for adding the given value.
Additionally, we also return a value, even though we use shared objects for storing results.

Instead of passing the shared objects to the :obj:`mpire.WorkerPool` constructor you can also use the
:meth:`mpire.WorkerPool.set_shared_objects` function:

.. code-block:: python

    results_container = Array('f', 100, lock=False)

    with WorkerPool(n_jobs=4) as pool:
        pool.set_shared_objects(results_container)
        pool.map_unordered(square_with_index, enumerate(range(100)),
                           iterable_len=100)


Worker state
------------

If you want to let each worker have its own state you can use the ``use_worker_state`` flag:

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

    with WorkerPool(n_jobs=4, use_worker_state=True) as pool:
        # Let the model predict
        data = np.array([[...]])
        results = pool.map(model_predict, data)

Instead of passing the flag to the :obj:`mpire.WorkerPool` constructor you can also make use of
:meth:`mpire.WorkerPool.set_use_worker_state`:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        pool.set_use_worker_state()
        results = pool.map(model_predict, data)

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

The worker ID will always be the first passed on argument to the provided function pointer.

Instead of passing the flag to the :obj:`mpire.WorkerPool` constructor you can also make use of
:meth:`mpire.WorkerPool.pass_on_worker_id`:

.. code-block:: python

    with WorkerPool(n_jobs=4, shared_objects=results_container) as pool:
        pool.pass_on_worker_id()
        pool.map_unordered(square_sum, range(100))


Process start method
--------------------

The ``multiprocessing`` package allows you to start processes using a few different methods: ``'fork'``, ``'spawn'`` or
``'forkserver'``. Threading is also available by using ``'threading'``. For detailed information on the multiprocessing
contexts, please refer to the multiprocessing documentation_ and caveats_ section. In short:

- ``'fork'`` (the default) copies the parent process such that the child process is effectively identical. This
  includes copying everything currently in memory. This is sometimes useful, but other times useless or even a serious
  bottleneck.
- ``'spawn'`` starts a fresh python interpreter where only those resources necessary are inherited.
- ``'forkserver'`` first starts a server process. Whenever a new process is needed the parent process requests the
  server to fork a new process.
- ``'threading'`` starts child threads.

The ``'spawn'`` and ``'forkserver'`` methods have some caveats_. All resources needed for running the child process
should be picklable. This can sometimes be a hassle when you heavily rely on lambdas or are trying to run MPIRE in an
interactive shell. To remedy most of these problems MPIRE can use dill_ as a replacement for pickle. Simply install the
required :ref:`dependencies <dilldep>` and you're good to go.

Additionally, global variables (constants are fine) might have a different value than you might expect. You also have to
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


.. _documentation: https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods
.. _caveats: https://docs.python.org/3/library/multiprocessing.html#the-spawn-and-forkserver-start-methods
.. _dill: https://pypi.org/project/dill/
