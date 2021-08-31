MPIRE (MultiProcessing Is Really Easy)
======================================

|Build status| |Docs status| |Pypi status| |Python versions|

.. |Build status| image:: https://github.com/Slimmer-AI/mpire/workflows/Build/badge.svg?branch=master
.. |Docs status| image:: https://github.com/Slimmer-AI/mpire/workflows/Docs/badge.svg?branch=master
.. |Pypi status| image:: https://img.shields.io/pypi/v/mpire
.. |Python versions| image:: https://img.shields.io/pypi/pyversions/mpire

``MPIRE``, short for MultiProcessing Is Really Easy, is a Python package for multiprocessing, but faster and more
user-friendly than the default multiprocessing package. It combines the convenient map like functions of
``multiprocessing.Pool`` with the benefits of using copy-on-write shared objects of ``multiprocessing.Process``,
together with easy-to-use worker state, worker insights, and progress bar functionality.

Full documentation is available at https://slimmer-ai.github.io/mpire/.

Features
--------

- Faster execution than other multiprocessing libraries. See benchmarks_.
- Intuitive, Pythonic syntax
- Multiprocessing with ``map``/``map_unordered``/``imap``/``imap_unordered`` functions
- Easy use of copy-on-write shared objects with a pool of workers (copy-on-write is only available for start method
  ``fork``)
- Each worker can have its own state and with convenient worker init and exit functionality this state can be easily
  manipulated (e.g., to load a memory-intensive model only once for each worker without the need of sending it through a
  queue)
- Progress bar support using tqdm_
- Progress dashboard support
- Worker insights to provide insight into your multiprocessing efficiency
- Graceful and user-friendly exception handling
- Automatic task chunking for all available map functions to speed up processing of small task queues (including numpy
  arrays)
- Adjustable maximum number of active tasks to avoid memory problems
- Automatic restarting of workers after a specified number of tasks to reduce memory footprint
- Nested pool of workers are allowed when setting the ``daemon`` option
- Child processes can be pinned to specific or a range of CPUs
- Optionally utilizes dill_ as serialization backend through multiprocess_, enabling parallelizing more exotic objects,
  lambdas, and functions in iPython and Jupyter notebooks.

MPIRE has been tested on both Linux and Windows. There are a few minor known caveats for Windows users, which can be
found here_.

.. _benchmarks: https://towardsdatascience.com/mpire-for-python-multiprocessing-is-really-easy-d2ae7999a3e9
.. _multiprocess: https://github.com/uqfoundation/multiprocess
.. _dill: https://pypi.org/project/dill/
.. _tqdm: https://tqdm.github.io/
.. _here: https://slimmer-ai.github.io/mpire/troubleshooting.html#windows


Installation
------------

Through pip (PyPi):

.. code-block:: bash

    pip install mpire

From source:

.. code-block:: bash

    python setup.py install


Getting started
---------------

Suppose you have a time consuming function that receives some input and returns its results. Simple functions like these
are known as `embarrassingly parallel`_ problems, functions that require little to no effort to turn into a parallel
task. Parallelizing a simple function as this can be as easy as importing ``multiprocessing`` and using the
``multiprocessing.Pool`` class:

.. _embarrassingly parallel: https://en.wikipedia.org/wiki/Embarrassingly_parallel

.. code-block:: python

    import time
    from multiprocessing import Pool

    def time_consuming_function(x):
        time.sleep(1)  # Simulate that this function takes long to complete
        return ...

    with Pool(processes=5) as pool:
        results = pool.map(time_consuming_function, range(10))

MPIRE can be used almost as a drop-in replacement to ``multiprocessing``. We use the ``mpire.WorkerPool`` class and
call one of the available ``map`` functions:

.. code-block:: python

    from mpire import WorkerPool

    with WorkerPool(n_jobs=5) as pool:
        results = pool.map(time_consuming_function, range(10))

The differences in code are small: there's no need to learn a completely new multiprocessing syntax, if you're used to
vanilla ``multiprocessing``. The additional available functionality, though, is what sets MPIRE apart.

Progress bar
~~~~~~~~~~~~

Suppose we want to know the status of the current task: how many tasks are completed, how long before the work is ready?
It's as simple as setting the ``progress_bar`` parameter to ``True``:

.. code-block:: python

    with WorkerPool(n_jobs=5) as pool:
        results = pool.map(time_consuming_function, range(10), progress_bar=True)

And it will output a nicely formatted tqdm_ progress bar. In case you're running your code inside a notebook it will
automatically switch to a widget.

MPIRE also offers a dashboard, for which you need to install additional dependencies_. See Dashboard_ for more
information.

.. _dependencies: https://slimmer-ai.github.io/mpire/install.html#dashboard
.. _Dashboard: https://slimmer-ai.github.io/mpire/usage/dashboard.html


Shared objects
~~~~~~~~~~~~~~

Note: Copy-on-write shared objects is only available for start method ``fork``. For ``threading`` the objects are shared
as-is. For other start methods the shared objects are copied once for each worker.

If you have one or more objects that you want to share between all workers you can make use of the copy-on-write
``shared_objects`` option of MPIRE.  MPIRE will pass on these objects only once for each worker without
copying/serialization. Only when you alter the object in the worker function it will start copying it for that worker.

.. code-block:: python

    def time_consuming_function(some_object, x):
        time.sleep(1)  # Simulate that this function takes long to complete
        return ...

    def main():
        some_object = ...
        with WorkerPool(n_jobs=5, shared_objects=some_object) as pool:
            results = pool.map(time_consuming_function, range(10), progress_bar=True)

See shared_objects_ for more details.

.. _shared_objects: https://slimmer-ai.github.io/mpire/usage/worker_pool.html#shared-objects

Worker initialization
~~~~~~~~~~~~~~~~~~~~~

Workers can be initialized using the ``worker_init`` feature. Together with ``worker_state`` you can load a model, or
set up a database connection, etc.:

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

Similarly, you can use the ``worker_exit`` feature to let MPIRE call a function whenever a worker terminates. You can
even let this exit function return results, which can be obtained later on. See the `worker_init and worker_exit`_
section for more information.

.. _worker_init and worker_exit: https://slimmer-ai.github.io/mpire/usage/map.html#worker-init-and-exit


Worker insights
~~~~~~~~~~~~~~~

When you're multiprocessing setup isn't performing as you want it to and you have no clue what's causing it, there's the
worker insights functionality. This will give you insight in your setup, but it will not profile the function you're
running (there are other libraries for that). Instead, it profiles the worker start up time, waiting time and
working time. When worker init and exit functions are provided it will time those as well.

Perhaps you're sending a lot of data over the task queue, which makes the waiting time go up. Whatever the case, you
can enable and grab the insights using the ``enable_insights`` flag and ``mpire.WorkerPool.get_insights`` function,
respectively:

.. code-block:: python

    with WorkerPool(n_jobs=5) as pool:
        results = pool.map(time_consuming_function, range(10), enable_insights=True)
        insights = pool.get_insights()

See `worker insights`_ for a more detailed example and expected output.

.. _worker insights: https://slimmer-ai.github.io/mpire/usage/map.html#worker-insights


Benchmarks
----------

MPIRE has been benchmarked on three different benchmarks: numerical computation, stateful computation, and expensive
initialization. More details on these benchmarks can be found in this `blog post`_. All code for these benchmarks can
be found in this project_.

The following graph shows the average normalized results of all three benchmarks. Results for individual benchmarks
can be found in the `blog post`_. The benchmarks were run on a Linux machine with 20 cores, with disabled hyperthreading
and 200GB of RAM. For each task, experiments were run with different numbers of processes/workers and results were
averaged over 5 runs.

.. image:: images/benchmarks_averaged.png
    :width: 600px
    :alt: Average normalized bechmark results

.. _blog post: https://towardsdatascience.com/mpire-for-python-multiprocessing-is-really-easy-d2ae7999a3e9
.. _project: https://github.com/sybrenjansen/multiprocessing_benchmarks



Documentation
-------------

See the full documentation at https://slimmer-ai.github.io/mpire/ for information on all the other features of MPIRE.

If you want to build the documentation yourself, please install the documentation dependencies by executing:

.. code-block:: bash

    pip install mpire[docs]

or 

.. code-block:: bash

    pip install .[docs]


Documentation can then be build by executing:

.. code-block:: bash

    python setup.py build_docs

Documentation can also be build from the ``docs`` folder directly. In that case ``MPIRE`` should be installed and
available in your current working environment. Then execute:

.. code-block:: bash

    make html

in the ``docs`` folder.
