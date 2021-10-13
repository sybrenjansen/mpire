Getting started
===============

Suppose you have a time consuming function that receives some input and returns its results. This could look like the
following:

.. code-block:: python

    import time

    def time_consuming_function(x):
        time.sleep(1)  # Simulate that this function takes long to complete
        return ...

    results = [time_consuming_function(x) for x in range(10)]

Running this function takes about 10 seconds to complete.

Functions like these are known as `embarrassingly parallel`_ problems, functions that require little to no effort to
turn into a parallel task. Parallelizing a simple function as this can be as easy as importing ``multiprocessing`` and
using the ``multiprocessing.Pool`` class:

.. _embarrassingly parallel: https://en.wikipedia.org/wiki/Embarrassingly_parallel

.. code-block:: python

    from multiprocessing import Pool

    with Pool(processes=5) as pool:
        results = pool.map(time_consuming_function, range(10))

We configured to have 5 workers, so we can handle 5 tasks in parallel. As a result, this function will complete in about
2 seconds.

MPIRE can be used almost as a drop-in replacement to ``multiprocessing``. We use the :obj:`mpire.WorkerPool` class and
call one of the available ``map`` functions:

.. code-block:: python

    from mpire import WorkerPool

    with WorkerPool(n_jobs=5) as pool:
        results = pool.map(time_consuming_function, range(10))

Similarly, this will complete in about 2 seconds. The differences in code are small: there's no need to learn a
completely new multiprocessing syntax, if you're used to vanilla ``multiprocessing``. The additional available
functionality, though, is what sets MPIRE apart.

Progress bar
------------

Suppose we want to know the status of the current task: how many tasks are completed, how long before the work is ready?
It's as simple as setting the ``progress_bar`` parameter to ``True``:

.. code-block:: python

    with WorkerPool(n_jobs=5) as pool:
        results = pool.map(time_consuming_function, range(10), progress_bar=True)

And it will output a nicely formatted tqdm_ progress bar. In case you're running your code inside a notebook it will
automatically switch to a widget.

MPIRE also offers a dashboard, for which you need to install additional :ref:`dependencies <dashboarddep>`. See
:ref:`Dashboard` for more information.

.. _tqdm: https://tqdm.github.io/


Shared objects
--------------

If you have one or more objects that you want to share between all workers you can make use of the copy-on-write
``shared_objects`` option of MPIRE. MPIRE will pass on these objects only once for each worker without
copying/serialization. Only when the object is altered in the worker function it will start copying it for that worker.

.. note::

    Copy-on-write is not available on Windows, as it requires the start method ``fork``.

.. code-block:: python

    def time_consuming_function(some_object, x):
        time.sleep(1)  # Simulate that this function takes long to complete
        return ...

    def main():
        some_object = ...
        with WorkerPool(n_jobs=5, shared_objects=some_object, start_method='fork') as pool:
            results = pool.map(time_consuming_function, range(10), progress_bar=True)

See :ref:`shared_objects` for more details.

Worker initialization
---------------------

Need to initialize each worker before starting the work? Have a look at the ``worker_state`` and ``worker_init``
functionality:

.. code-block:: python

    def init(worker_state):
        # Load a big dataset or model and store it in a worker specific worker_state
        worker_state['dataset'] = ...
        worker_state['model'] = ...

    def task(worker_state, idx):
        # Let the model predict a specific instance of the dataset
        return worker_state['model'].predict(worker_state['dataset'][idx])

    with WorkerPool(n_jobs=5, use_worker_state=True) as pool:
        results = pool.map(task, range(10), worker_init=init)

Similarly, you can use the ``worker_exit`` parameter to let MPIRE call a function whenever a worker terminates. You can
even let this exit function return results, which can be obtained later on. See the :ref:`worker_init_exit` section for
more information.


Worker insights
---------------

When you're multiprocessing setup isn't performing as you want it to and you have no clue what's causing it, there's the
worker insights functionality. This will give you some insight in your setup, but it will not profile the function
you're running (there are other libraries for that). Instead, it profiles the worker start up time, waiting time and
working time. When worker init and exit functions are provided it will time those as well.

Perhaps you're sending a lot of data over the task queue, which makes the waiting time go up. Whatever the case, you
can enable and grab the insights using the ``enable_insights`` flag and :meth:`mpire.WorkerPool.get_insights` function,
respectively:

.. code-block:: python

    with WorkerPool(n_jobs=5) as pool:
        results = pool.map(time_consuming_function, range(10), enable_insights=True)
        insights = pool.get_insights()

See :ref:`worker insights` for a more detailed example and expected output.
