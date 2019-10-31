Map family
==========

This section describes the different ways of interacting with a :obj:`mpire.WorkerPool` object.

.. contents:: Contents
    :depth: 2
    :local:

map family of functions
-----------------------

:obj:`mpire.WorkerPool` implements four types of parallel ``map`` functions, being:

- :meth:`mpire.WorkerPool.map`: Blocks until results are ready, results are ordered in the same way as the provided
  arguments
- :meth:`mpire.WorkerPool.map_unordered`: The same as :meth:`mpire.WorkerPool.map`, but results are ordered by task
  completion time. Usually faster than :meth:`mpire.WorkerPool.map`.
- :meth:`mpire.WorkerPool.imap`: Lazy version of :meth:`mpire.WorkerPool.map`, returns a generator. The generator will
  give results back whenever new results are ready. Results are ordered in the same way as the provided arguments.
- :meth:`mpire.WorkerPool.imap_unordered`: The same as :meth:`mpire.WorkerPool.imap`, but results are ordered by task
  completion time. Usually faster than :meth:`mpire.WorkerPool.imap`.

When using a single worker the unordered versions are equivalent to their ordered counterpart.

.. important::

    Each ``map`` function should receive a function pointer and an iterable of arguments, where the elements of the
    iterable are expected to be iterables that are unpacked as arguments. If the elements are not iterables, the single
    value is simply passed on as the only argument. However, if a single value is a dictionary the (key, value) pairs
    will be unpacked with the ``**``-operator.

A few examples:

.. code-block:: python

    def square(x):
        return x * x

    def multiply(x, y):
        return x * y

    with WorkerPool(n_jobs=4) as pool:
        # 1. Square the numbers, results should be: [0, 1, 4, 9, 16, 25, ...]
        results = pool.map(square, range(100))

    with WorkerPool(n_jobs=4) as pool:
        # 2. Square the numbers, results should be: [0, 1, 4, 9, 16, 25, ...]
        # Note: you'll probably don't want to execute this, it will take a long time ...
        results = pool.map(square, range(int(1e30)), iterable_len=int(1e30), chunk_size=1)

    with WorkerPool(n_jobs=4) as pool:
        # 3. Multiply the numbers, results should be [0, 101, 204, 309, 416, ...]
        for result in pool.imap(multiply, zip(range(100), range(100, 200)), iterable_len=100):
            # Do something with this result
            print(result)

    with WorkerPool(n_jobs=4) as pool:
        # 4. Multiply the numbers, results should be [0, 101, ...]
        for result in pool.imap(multiply, [{'x': 0, 'y': 100}, {'y': 101, 'x': 1}, ...]):
            # Do something with this result
            print(result)

The first example should work as expected, the numbers are simply squared. MPIRE knows how many tasks there are because
a ``range`` object implements the ``__len__`` method (see section below).

In the second example the ``1e30`` number is too large for Python: try calling ``len(range(int(1e30)))``, this will
throw an ``OverflowError`` (I know ...). Therefore, we must use the ``iterable_len`` parameter to let MPIRE know how
large the tasks list is. We also have to specify a chunk size here as the chunk size should be lower than
``sys.maxsize``.

The third example shows an example of using multiple function arguments. Also note that we use ``imap`` in the third
example, which allows us to process the results whenever they come available, not having to wait for all results to be
ready.

The final example shows the use of an iterable of dictionaries. The (key, value) pairs are unpacked with the
``**``-operator, as you would expect. So it doesn't matter in what order the keys are stored. This should work for
``collection.OrderedDict`` objects as well.

If you want to pass those dictionaries in example 4 as a whole to the following function, for example:

.. code-block:: python

    def multiply_dict(d):
        return d['x'] * d['y']

you would have to convert the list of dictionaries to a list of single argument tuples, where each argument is a
dictionary:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        # Multiply the numbers, results should be [0, 101, ...]
        for result in pool.imap(multiply_dict, [({'x': 0, 'y': 100},), ({'y': 101, 'x': 1},), ...]):
            # Do something with this result
            print(result)

There is, however, a utility function that does this transformation for you:

.. code-block:: python

    from mpire.utils import make_single_arguments

    with WorkerPool(n_jobs=4) as pool:
        # Multiply the numbers, results should be [0, 101, ...]
        for result in pool.imap(multiply_dict, make_single_arguments([{'x': 0, 'y': 100},
                                                                      {'y': 101, 'x': 1}, ...],
                                                                     generator=False)):
            # Do something with this result
            print(result)

:meth:`mpire.utils.make_single_arguments` expects an iterable of arguments and converts them to tuples accordingly. The
second argument of this function specifies if you want the function to return a generator or a materialized list. If we
would like to return a generator we would need to pass on the iterable length as well.

Task chunking
-------------

By default, MPIRE chunks the given tasks in to four times the number of jobs chunks. Each worker is given one chunk of
tasks at a time before returning its results. This usually makes processing faster when you have rather small tasks
(computation wise) and results are pickled/unpickled when they are send to a worker or main process. Chunking the tasks
and results ensures that each process has to pickle/unpickle less often.

However, to determine the number of tasks in the argument list the iterable should implement the ``__len__`` method,
which is available in default containers like ``list`` or ``tuple``, but isn't available in most generator objects
(the ``range`` object is one of the exceptions). To allow working with generators each ``map`` function has the option
to pass the iterable length:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        # 1. This will issue a warning and sets the chunk size to 1
        results = pool.map(square, ((x,) for x in range(100)))

        # 2. This will issue a warning as well and sets the chunk size to 1
        results = pool.map(square, ((x,) for x in range(100)), n_splits=4)

        # 3. Square the numbers using a generator using a specific number of splits
        results = pool.map(square, ((x,) for x in range(100)), iterable_len=100, n_splits=4)

        # 4. Square the numbers using a generator using automatic chunking
        results = pool.map(square, ((x,) for x in range(100)), iterable_len=100)

        # 5. Square the numbers using a generator using a fixed chunk size
        results = pool.map(square, ((x,) for x in range(100)), chunk_size=4)

In the first two examples the function call will fail because MPIRE doesn't know how large the chunks should be as the
total number of tasks is unknown, therefore it will fall back to a chunk size of 1. The third example should work as
expected where 4 chunks are used. The fourth example uses 16 chunks (the default four times the number of workers). The
last example uses a fixed chunk size of four, so MPIRE doesn't need to know the iterable length.

You can also call the chunk function manually:

.. code-block:: python

    from mpire.utils import chunk_tasks

    # Convert to list because chunk_tasks returns a generator
    print(list(chunk_tasks(range(10), n_splits=3)))
    print(list(chunk_tasks(range(10), chunk_size=2.5)))
    print(list(chunk_tasks((x for x in range(10)), iterable_len=10, n_splits=6)))

will output:

.. code-block:: python

    [(0, 1, 2, 3), (4, 5, 6), (7, 8, 9)]
    [(0, 1, 2), (3, 4), (5, 6, 7), (8, 9)]
    [(0, 1), (2, 3), (4,), (5, 6), (7, 8), (9,)]


Numpy arrays
------------

Chunking
~~~~~~~~

Numpy arrays are treated a little bit differently when passed on to the ``map`` functions. Usually MPIRE uses
``itertools.islice`` for chunking, which depends on the ``__iter__`` special function of the container object. But
applying that to numpy arrays would yield:

.. code-block:: python

    import numpy as np

    # Create random array
    arr = np.random.rand(10, 3)

    # Chunk the array using default chunking
    arr_iter = iter(arr)
    chunk_size = 3
    while True:
        chunk = list(itertools.islice(arr_iter, chunk_size))
        if chunk:
            yield chunk
        else:
            break

with output:

.. code-block:: python

    [array([0.68438994, 0.9701514 , 0.40083965]), array([0.88428556, 0.2083905 , 0.61490443]),
     array([0.89249174, 0.39902235, 0.70762541])]
    [array([0.18850964, 0.1022777 , 0.41539432]), array([0.07327858, 0.18608165, 0.75862301]),
     array([0.69215651, 0.4211941 , 0.31029439])]
    [array([0.82571272, 0.72257819, 0.86079131]), array([0.91285817, 0.49398461, 0.27863929]),
     array([0.146981  , 0.84671211, 0.30122806])]
    [array([0.11783283, 0.12585031, 0.39864368])]

In other words, each row of the array is now in its own array and each one of them is given to the target function
individually. Instead, MPIRE will chunk them in to something more reasonable using numpy slicing instead:

.. code-block:: python

    from mpire.utils import chunk_tasks

    for chunk in chunk_tasks(arr, chunk_size=chunk_size):
        print(repr(chunk))

Output:

.. code-block:: python

    array([[0.68438994, 0.9701514 , 0.40083965],
           [0.88428556, 0.2083905 , 0.61490443],
           [0.89249174, 0.39902235, 0.70762541]])
    array([[0.18850964, 0.1022777 , 0.41539432],
           [0.07327858, 0.18608165, 0.75862301],
           [0.69215651, 0.4211941 , 0.31029439]])
    array([[0.82571272, 0.72257819, 0.86079131],
           [0.91285817, 0.49398461, 0.27863929],
           [0.146981  , 0.84671211, 0.30122806]])
    array([[0.11783283, 0.12585031, 0.39864368]])

Each chunk is now a single numpy array containing as many rows as the chunk size, except for the last chunk as there
aren't enough rows left.

Return value
~~~~~~~~~~~~

When the user defined function returns numpy arrays and you're applying the :meth:`mpire.WorkerPool.map` function MPIRE
will concatenate the resulting numpy arrays to a single array by default. For example:

.. code-block:: python

    def add_five(x):
        return x + 5

    with WorkerPool(n_jobs=4) as pool:
        results = pool.map(add_five, arr, chunk_size=chunk_size)

will return:

.. code-block:: python

    array([[5.68438994, 5.9701514 , 5.40083965],
           [5.88428556, 5.2083905 , 5.61490443],
           [5.89249174, 5.39902235, 5.70762541],
           [5.18850964, 5.1022777 , 5.41539432],
           [5.07327858, 5.18608165, 5.75862301],
           [5.69215651, 5.4211941 , 5.31029439],
           [5.82571272, 5.72257819, 5.86079131],
           [5.91285817, 5.49398461, 5.27863929],
           [5.146981  , 5.84671211, 5.30122806],
           [5.11783283, 5.12585031, 5.39864368]])

This behavior can be cancelled by using the ``concatenate_numpy_output`` flag:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        results = pool.map(add_five, arr, chunk_size=chunk_size, concatenate_numpy_output=False)

This will return individual arrays:

.. code-block:: python

    [array([[5.68438994, 5.9701514 , 5.40083965],
            [5.88428556, 5.2083905 , 5.61490443],
            [5.89249174, 5.39902235, 5.70762541]]),
     array([[5.18850964, 5.1022777 , 5.41539432],
            [5.07327858, 5.18608165, 5.75862301],
            [5.69215651, 5.4211941 , 5.31029439]]),
     array([[5.82571272, 5.72257819, 5.86079131],
            [5.91285817, 5.49398461, 5.27863929],
            [5.146981  , 5.84671211, 5.30122806]]),
     array([[5.11783283, 5.12585031, 5.39864368]])]


Maximum number of active tasks
------------------------------

When you have tasks that take up a lot of memory you can limit the number of jobs or limit the number of active tasks
(i.e., the number of tasks currently being available to the workers, tasks that are in the queue ready to be processed).
The first option is the most obvious one to save memory when the processes themselves use up much memory. The second is
convenient when the argument list takes up too much memory. For example, suppose you want to kick off an enormous amount
of jobs (let's say a billion) of which the arguments take up 1 KB per task (e.g., large strings), then that task queue
would take up ~1 TB of memory!

In such cases, a good rule of thumb would be to have twice the amount of active tasks than there are jobs. This means
that when all workers complete their task at the same time each would directly be able to continue with another task.
When workers take on their new tasks the generator of tasks is iterated to the point that again there would be twice the
amount of active tasks.

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        # Square the numbers using a generator
        results = pool.map(square, range(int(1e300)), iterable_len=int(1e300),
                           chunk_size=int(1e5), max_tasks_active=2*4)


Worker lifespan
---------------

Occasionally, workers that process multiple, memory intensive tasks do not release their used up memory properly, which
results in memory usage building up. This is not a bug in MPIRE, but a consequence of Python's poor garbage collection.
To avoid this type of problem you can set the worker lifespan: the number of tasks (well, actually the number of chunks
of tasks) after which a worker should restart.

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        # Square the numbers using a generator
        results = pool.map(square, range(100), worker_lifespan=1)

In this example each worker is restarted after finishing a single chunk of tasks.


Progress bar
------------

Progress bar support is added through the tqdm_ package (installed by default when installing MPIRE). The most easy way
to include a progress bar is by enabling the ``progress_bar`` flag in any of the ``map`` functions:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        pool.map(square, range(100), progress_bar=True)

This will display a basic ``tqdm`` progress bar displaying the time elapsed and remaining, number of tasks completed
(including a percentage value) and the speed (i.e., number of tasks completed per time unit).

When inside a Jupyter/IPython notebook, the progress bar will change automatically to a native Jupyter widget.

.. note::

    The Jupyter ``tqdm`` widget requires the Javascript widget to run, which might not be enabled by default. You will
    notice a ``Widget Javascript not detected`` error message in your notebook if so. To remedy this, enable the widget
    by executing ``jupyter nbextension enable --py --sys-prefix widgetsnbextension`` in your terminal before starting
    the notebook.

.. note::

    Please keep in mind that to show real-time progress information MPIRE starts an additional child process, which
    could consume a bit of the available compute power of your machine.


Multiple progress bars with nested WorkerPools
----------------------------------------------

In MPIRE you can easily print a progress bar on a different position on the terminal using the ``progress_bar_position``
parameter in the map functions, which facilitates the use of multiple progress bars. Here's an example of using multiple
progress bars using nested WorkerPools:

.. code-block:: python

    from mpire import tqdm

    def dispatcher(worker_id, X):
        with WorkerPool(n_jobs=4) as nested_pool:
            return nested_pool.map(square, X, progress_bar=True, progress_bar_position=worker_id + 1)

    def main():
        with WorkerPool(n_jobs=4, daemon=False, pass_worker_id=True) as pool:
            pool.map(dispatcher, ((range(x, x + 100),) for x in range(100)), iterable_len=100,
                     n_splits=4, progress_bar=True)

    main()

We use ``worker_id + 1`` here because the worker IDs start at zero, and we reserve position 0 for the progress bar of
the main WorkerPool (which is the default).

.. note::

    Unfortunately, multiple ``tqdm`` progress bars from child processes don't play that nicely within a Jupyter/IPython
    notebook session. It'll work but you'll get some additional new lines in your output and it could be that your main
    progress bar won't update as you would expect. Note that you can always use the MPIRE dashboard.


.. _tqdm: https://pypi.python.org/pypi/tqdm
.. _`tqdm documentation`: https://pypi.python.org/pypi/tqdm#documentation