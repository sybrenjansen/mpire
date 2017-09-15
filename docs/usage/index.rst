Usage
=====

This section describes the different methods and options of the :obj:`mpire.WorkerPool` class.

.. contents:: Contents
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

When using ``n_jobs=None`` MPIRE will spawn as many processes as there are CPUs on your system, minus one. The minus one
is done in favor of the ``imap`` functions, where the main thread is usually busy as well. Specifying more jobs than you
have CPUs is, of course, possible as well.

.. warning::

    The results queue should be drained first before joining the workers, otherwise you can get a deadlock. For more
    information, see the warnings in the Python docs here_.

.. _here: https://docs.python.org/3.4/library/multiprocessing.html#pipes-and-queues

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

When using a single worker, the unordered versions are equivalent to their ordered counterpart.

Each ``map`` function should receive a function pointer and an iterable of arguments, where the elements of the iterable
are expected to be iterables that are unpacked as arguments. For example:

.. code-block:: python

    def square(x):
        return x * x

    def multiply(x, y):
        return x * y

    with WorkerPool(n_jobs=4) as pool:
        # 1. This will fail!
        results = pool.map(square, range(100))

    with WorkerPool(n_jobs=4) as pool:
        # 2. Square the numbers, results should be: [0, 1, 4, 9, 16, 25, ...]
        results = pool.map(square, [(x,) for x in range(100)])

    with WorkerPool(n_jobs=4) as pool:
        # 3. Multiply the numbers, results should be [0, 101, 204, 309, 416, ...]
        for result in pool.imap(multiply, [(x, y) for x, y in zip(range(100), range(100, 200))]):
            # Do something with this result
            pass

In the first example the function call will fail because the elements of the provided iterable are not iterables, but
single integer values. The second example should work as expected. The third examples shows an example of using multiple
function arguments. Also note that we use ``imap`` in the third example, which allows us to process the results whenever
they come available, not having to wait for all results to be ready.

Manual chunking
~~~~~~~~~~~~~~~

By default, MPIRE chunks the given tasks in to four times the number of jobs chunks. Each worker is given one chunk of
tasks at a time before returning its results. This usually makes processing faster when you have rather small tasks
(computation wise) as tasks and results are pickled/unpickled when they are send to a worker or main thread. Chunking
the tasks and results ensures that each process has to pickle/unpickle less often.

However, to determine the number of tasks in the argument list the iterable should implement the ``__len__`` method,
which is available in default containers like ``list`` or ``tuple``, but isn't available in generator objects. To allow
working with generators each ``map`` function has the option to pass the iterable length:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        # 1. This will fail!
        results = pool.map(square, ((x,) for x in range(100)))

        # 2. Square the numbers using a generator using automatic chunking
        results = pool.map(square, ((x,) for x in range(100)), iterable_len=100)

        # 3. Square the numbers using a generator using a fixed chunk size
        results = pool.map(square, ((x,) for x in range(100)), chunk_size=4)

In the first example the function call will fail because MPIRE doesn't know how large the chunks should be. The second
example should work as expected where 16 chunks are used (four times the number of workers). The third example uses a
fixed chunk size of four, so MPIRE doesn't need to know the iterable length.

Maximum number of active tasks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
        results = pool.map(square, ((x,) for x in range(int(1e300))), iterable_len=int(1e300),
                           max_tasks_active=2*4)

Worker lifespan
~~~~~~~~~~~~~~~

Occasionally, workers that process multiple, memory intensive tasks do not release their used up memory properly, which
results in memory usage building up. This is not a bug in MPIRE, but a consequence of Python's poor garbage collection
in child processes. To avoid this type of problem you can set the worker lifespan: the number of tasks (well, actually
the number of chunks of tasks) after which a worker should restart.

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        # Square the numbers using a generator
        results = pool.map(square, ((x,) for x in range(100)), iterable_len=100, worker_lifespan=1)

In this example each worker is restarted after finishing a single chunk of tasks.

Restarting workers
~~~~~~~~~~~~~~~~~~

The first time you call one of the ``map`` functions the pool of workers is started with the appropriate argument
values, including the function pointer, lifespan, etc. When you want to call a ``map`` function for the second time the
workers of the first call still exist and they can be reused if you don't want to change the settings of the first call.
The main benefit to this is that the overhead of starting/terminating child processes is avoided:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        # 1. Square the numbers using a generator, results should be: [0, 1, 4, 9, 16, 25, ...]
        results = pool.map(square, ((x,) for x in range(100)), iterable_len=100, worker_lifespan=1)

        # 2. Still square the numbers using a generator, results should be: [0, 1, 4, 9, 16, 25, ...]
        results = pool.map(multiply, ((x,) for x in range(100)), iterable_len=100, worker_lifespan=2,
                           restart_workers=False)

        # 3. Multiply the numbers using a generator, results should be [0, 101, 204, 309, 416, ...]
        results = pool.map(multiply, ((x,y) for x, y in zip(range(100), range(100, 200)),
                           iterable_len=100, worker_lifespan=2, restart_workers=True)

The first example spawns workers with the task of squaring the provided numbers. In the second example we reuse the
workers of the first example by stating that we don't want to restart the workers. This means that the function pointer
and worker lifespan are not provided to the workers, so this example is still calling the ``square`` function. Only when
we tell the function that we want to restart the workers we can provide a different function pointer and worker
lifespan.


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
        pool.map_unordered(square_with_index, ((idx, x) for idx, x in enumerate(range(100))),
                           iterable_len=100)

        # 2, Use a shared array of size 100 and type float to store the results
        square_results_container = Array('f', 100, lock=False)
        add_results_container = Array('f', 100, lock=False)
        pool.set_shared_objects((square_results_container, add_results_container))

        # Square, add and modulo the results and store them in the results containers
        modulo_results = pool.map(square_add_and_modulo_with_index,
                                  ((idx, x) for idx, x in enumerate(range(100))),
                                  iterable_len=100, restart_workers=True)

We use the :meth:`mpire.WorkerPool.set_shared_objects` function to let MPIRE know we want to pass shared objects to all
the workers. Multiple objects can be provided by placing them, for example, in a tuple container as is done in example
two. When providing shared objects the provided function pointer in the map functions should receive the shared objects
as its first argument (or the second argument when the worker ID is passed on as well, see :ref:`workerID`).

In the first example we create a results container and disable locking. We can safely disable locking here as each task
writes to a different index in the array, so no race conditions can occur. Disabling locking is, of course, a lot faster
than enabling it.

In the second example we create two different results containers, one for squaring and for adding the given value.
Additionally, we also return a value, even though we use shared objects for storing results. Note that we have to
restart the workers in this example.


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
        pool.map_unordered(square_sum, ((x,) for x in range(100)), iterable_len=100)

The worker ID will always be the first passed on argument to the provided function pointer.
