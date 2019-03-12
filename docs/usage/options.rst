WorkerPool options
==================

This section describes the different options of a :obj:`mpire.WorkerPool` object.

.. contents:: Contents
    :depth: 2
    :local:

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

    with WorkerPool(n_jobs=4) as pool:
        # 1. Use a shared array of size 100 and type float to store the results
        results_container = Array('f', 100, lock=False)
        pool.set_shared_objects(results_container)

        # Square the results and store them in the results container
        pool.map_unordered(square_with_index, enumerate(range(100)),
                           iterable_len=100)

        # 2, Use a shared array of size 100 and type float to store the results
        square_results_container = Array('f', 100, lock=False)
        add_results_container = Array('f', 100, lock=False)
        pool.set_shared_objects((square_results_container, add_results_container))

        # Square, add and modulo the results and store them in the results containers
        modulo_results = pool.map(square_add_and_modulo_with_index,
                                  enumerate(range(100)), iterable_len=100)

We use the :meth:`mpire.WorkerPool.set_shared_objects` function to let MPIRE know we want to pass shared objects to all
the workers. Instead of using the designated function we could also pass in the shared objects in the
:obj:`mpire.WorkerPool` constructor:

.. code-block:: python

    results_container = Array('f', 100, lock=False)

    with WorkerPool(n_jobs=4, shared_objects=results_container) as pool:
        pool.map_unordered(square_with_index, enumerate(range(100)),
                           iterable_len=100)

Multiple objects can be provided by placing them, for example, in a tuple container as is done in example two. When
providing shared objects the provided function pointer in the map functions should receive the shared objects as its
first argument (or the second argument when the worker ID is passed on as well, see :ref:`workerID`).

In the first example (marked ``#1``) we create a results container and disable locking. We can safely disable locking
here as each task writes to a different index in the array, so no race conditions can occur. Disabling locking is, of
course, a lot faster than enabling it.

In the second example we create two different results containers, one for squaring and for adding the given value.
Additionally, we also return a value, even though we use shared objects for storing results. Note that we have to
restart the workers in this example.


Worker state
------------

If you want to let each worker have its own state you can enable this using the
:meth:`mpire.WorkerPool.set_use_worker_state` function:

.. code-block:: python

    import numpy as np
    import pickle

    def load_big_model():
        # Load a model which takes up a lot of memory
        with open('./big_ass_model.p3', 'rb') as f:
            return pickle.load(f)

    def model_predict(worker_state, x):
        # Load model
        if 'model' not in worker_state:
            worker_state['model'] = load_big_model()

        # Predict
        return worker_state['model'].predict(x)

    with WorkerPool(n_jobs=4) as pool:
        # Use worker state
        pool.set_use_worker_state(True)

        # Let the model predict
        data = np.array([[...]])
        results = pool.map(model_predict, data)

Instead of using the designated function we could also pass in this flag in the :obj:`mpire.WorkerPool` constructor:

.. code-block:: python

    with WorkerPool(n_jobs=4, use_worker_state=True) as pool:
        # Let the model predict
        data = np.array([[...]])
        results = pool.map(model_predict, data)

.. _workerID:


Accessing the worker ID
-----------------------

Each worker in MPIRE is given an integer ID to distinguish them. Worker #1 will have ID ``0``, #2 will have ID ``1``,
etc. Sometimes it can be useful to have access to this ID. For example, when you have a shared array of which the size
equals the number of workers and you want worker #1 only to access the first element, and worker #2 only to access the
second element, and so on.

By default, the worker ID is not passed on. You can enable/disable this using the
:meth:`mpire.WorkerPool.pass_on_worker_id` function:

.. code-block:: python

    def square_sum(worker_id, shared_objects, x):
        # Even though the shared objects is a single container, we 'unpack' it anyway
        results_container = shared_objects

        # Square and sum
        results_container[worker_id] += x * x

    with WorkerPool(n_jobs=4) as pool:
        # Use a shared array of size equal to the number of jobs to store the results
        results_container = Array('f', 4, lock=False)
        pool.set_shared_objects(results_container)

        # Let MPIRE know that we want to pass on the worker ID
        pool.pass_on_worker_id(True)

        # Square the results and store them in the results container
        pool.map_unordered(square_sum, range(100))

The worker ID will always be the first passed on argument to the provided function pointer.

Instead of using the designated function we could also pass in the worker ID flag in the :obj:`mpire.WorkerPool`
constructor:

.. code-block:: python

    results_container = Array('f', 4, lock=False)

    with WorkerPool(n_jobs=4, shared_objects=results_container, pass_worker_id=True) as pool:
        # Square the results and store them in the results container
        pool.map_unordered(square_sum, range(100))


Process start method
--------------------

The ``multiprocessing`` package allows you to start processes using a few different methods: ``'fork'``, ``'spawn'`` or
``'forkserver'``. For detailed information, please refer to the multiprocessing documentation_ and caveats_ section. In
short:

- ``'fork'`` (the default) copies the parent process such that the child process is effectively identical. This
  includes copying everything currently in memory. This is sometimes useful, but other times useless or even a serious
  bottleneck.
- ``'spawn'`` starts a fresh python interpreter where only those resources necessary are inherited.
- ``'forkserver'`` first starts a server process. Whenever a new process is needed the parent process requests the
  server to fork a new process.

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


Unit tests
~~~~~~~~~~

When using the ``'spawn'`` or ``'forkserver'`` method you'll probably run in to one or two issues when running
unittests. One problem that might occur is that your unittests will restart whenever the piece of code containing such
a start method is called, leading to very funky terminal output. To remedy this problem make sure your ``setup`` call in
``setup.py`` is surrounded by an ``if __name__ == '__main__':`` clause:

.. code-block:: python

    from setuptools import setup

    if __name__ == '__main__':

        # Call setup and install any dependencies you have inside the if-clause
        setup(...)

See the 'Safe importing of main module' section at caveats_.

The second problem you might encounter is that the semaphore tracker of multiprocessing will complain when you run
individual (or a selection of) unittests using ``python setup.py test -s tests.some_test``. At the end of the tests you
will see errors like:

.. code-block:: python

    Traceback (most recent call last):
      File ".../site-packages/multiprocess/semaphore_tracker.py", line 132, in main
        cache.remove(name)
    KeyError: b'/mp-d3i13qd5'
    .../site-packages/multiprocess/semaphore_tracker.py:146: UserWarning: semaphore_tracker: There appear to be 58
                                                             leaked semaphores to clean up at shutdown
      len(cache))
    .../site-packages/multiprocess/semaphore_tracker.py:158: UserWarning: semaphore_tracker: '/mp-f45dt4d6': [Errno 2]
                                                             No such file or directory
      warnings.warn('semaphore_tracker: %r: %s' % (name, e))
    ...

Your unittests will still succeed and run OK. Unfortunately, I've not found a remedy to this problem using
``python setup.py test`` yet. What you can use instead is something like the following:

.. code-block:: python

    python -m unittest tests.some_test

This will work just fine. See the unittest_ documentation for more information.

.. _documentation: https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods
.. _caveats: https://docs.python.org/3/library/multiprocessing.html#the-spawn-and-forkserver-start-methods
.. _dill: https://pypi.org/project/dill/
.. _unittest: https://docs.python.org/3.4/library/unittest.html#command-line-interface
