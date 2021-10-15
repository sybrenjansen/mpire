Numpy arrays
============

.. contents:: Contents
    :depth: 2
    :local:

Chunking
--------

Numpy arrays are treated a little bit differently when passed on to the ``map`` functions. Usually MPIRE uses
``itertools.islice`` for chunking, which depends on the ``__iter__`` special function of the container object. But
applying that to numpy arrays:

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

would yield:

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
------------

When the user provided function returns numpy arrays and you're applying the :meth:`mpire.WorkerPool.map` function MPIRE
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
