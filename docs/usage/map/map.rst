map family of functions
=======================

.. contents:: Contents
    :depth: 2
    :local:

:obj:`mpire.WorkerPool` implements four types of parallel ``map`` functions, being:

:meth:`mpire.WorkerPool.map`
    Blocks until results are ready, results are ordered in the same way as the provided arguments.
:meth:`mpire.WorkerPool.map_unordered`
    The same as :meth:`mpire.WorkerPool.map`, but results are ordered by task completion time. Usually faster than
    :meth:`mpire.WorkerPool.map`.
:meth:`mpire.WorkerPool.imap`
    Lazy version of :meth:`mpire.WorkerPool.map`, returns a generator. The generator will give results back whenever new
    results are ready. Results are ordered in the same way as the provided arguments.
:meth:`mpire.WorkerPool.imap_unordered`
    The same as :meth:`mpire.WorkerPool.imap`, but results are ordered by task completion time. Usually faster than
    :meth:`mpire.WorkerPool.imap`.

When using a single worker the unordered versions are equivalent to their ordered counterparts.

Iterable of arguments
---------------------

Each ``map`` function should receive a function and an iterable of arguments, where the elements of the iterable can
be single values or iterables that are unpacked as arguments. If an element is a dictionary, the ``(key, value)`` pairs
will be unpacked with the ``**``-operator.

.. code-block:: python

    def square(x):
        return x * x

    with WorkerPool(n_jobs=4) as pool:
        # 1. Square the numbers, results should be: [0, 1, 4, 9, 16, 25, ...]
        results = pool.map(square, range(100))

The first example should work as expected, the numbers are simply squared. MPIRE knows how many tasks there are because
a ``range`` object implements the ``__len__`` method (see :ref:`Task chunking`).

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        # 2. Square the numbers, results should be: [0, 1, 4, 9, 16, 25, ...]
        # Note: don't execute this, it will take a long time ...
        results = pool.map(square, range(int(1e30)), iterable_len=int(1e30), chunk_size=1)

In the second example the ``1e30`` number is too large for Python: try calling ``len(range(int(1e30)))``, this will
throw an ``OverflowError`` (don't get me started ...). Therefore, we must use the ``iterable_len`` parameter to let
MPIRE know how large the tasks list is. We also have to specify a chunk size here as the chunk size should be lower than
``sys.maxsize``.

.. code-block:: python

    def multiply(x, y):
        return x * y

    with WorkerPool(n_jobs=4) as pool:
        # 3. Multiply the numbers, results should be [0, 101, 204, 309, 416, ...]
        for result in pool.imap(multiply, zip(range(100), range(100, 200)), iterable_len=100):
            ...

The third example shows an example of using multiple function arguments. Note that we use ``imap`` in this example,
which allows us to process the results whenever they come available, not having to wait for all results to be ready.

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        # 4. Multiply the numbers, results should be [0, 101, ...]
        for result in pool.imap(multiply, [{'x': 0, 'y': 100}, {'y': 101, 'x': 1}, ...]):
            ...

The final example shows the use of an iterable of dictionaries. The (key, value) pairs are unpacked with the
``**``-operator, as you would expect. So it doesn't matter in what order the keys are stored. This should work for
``collection.OrderedDict`` objects as well.

Circumvent argument unpacking
-----------------------------

If you want to avoid unpacking and pass the tuples in example 3 or the dictionaries in example 4 as a whole, you can.
We'll continue on example 4, but the workaround for example 3 is similar.

Suppose we have the following function which expects a dictionary:

.. code-block:: python

    def multiply_dict(d):
        return d['x'] * d['y']

Then you would have to convert the list of dictionaries to a list of single argument tuples, where each argument is a
dictionary:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        # Multiply the numbers, results should be [0, 101, ...]
        for result in pool.imap(multiply_dict, [({'x': 0, 'y': 100},),
                                                ({'y': 101, 'x': 1},),
                                                ...]):
            ...

There is a utility function available that does this transformation for you:

.. code-block:: python

    from mpire.utils import make_single_arguments

    with WorkerPool(n_jobs=4) as pool:
        # Multiply the numbers, results should be [0, 101, ...]
        for result in pool.imap(multiply_dict, make_single_arguments([{'x': 0, 'y': 100},
                                                                      {'y': 101, 'x': 1}, ...],
                                                                     generator=False)):
            ...

:meth:`mpire.utils.make_single_arguments` expects an iterable of arguments and converts them to tuples accordingly. The
second argument of this function specifies if you want the function to return a generator or a materialized list. If we
would like to return a generator we would need to pass on the iterable length as well.

.. _mixing-multiple-map-calls:

Mixing ``map`` functions
------------------------

``map`` functions cannot be used while another ``map`` function is still running. E.g., the following will raise an
exception:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        imap_results = pool.imap(multiply, zip(range(100), range(100, 200)), iterable_len=100)
        next(imap_results)  # We actually have to start the imap function

        # Will raise because the imap function is still running
        map_results = pool.map(square, range(100))

Make sure to first finish the ``imap`` function before starting a new ``map`` function. This holds for all ``map``
functions.

Not exhausting a lazy ``imap`` function
---------------------------------------

If you don't exhaust a lazy ``imap`` function, but do close the pool, the remaining tasks and results will be lost.
E.g., the following will raise an exception:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        imap_results = pool.imap(multiply, zip(range(100), range(100, 200)), iterable_len=100)
        first_result = next(imap_results)  # We actually have to start the imap function
        pool.terminate()

        # This will raise
        results = list(imap_results)

Similarly, exiting the ``with`` block terminates the pool as well:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        imap_results = pool.imap(multiply, zip(range(100), range(100, 200)), iterable_len=100)
        first_result = next(imap_results)  # We actually have to start the imap function

    # This will raise
    results = list(imap_results)
