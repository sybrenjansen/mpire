.. _shared_objects:

Shared objects
==============

.. contents:: Contents
    :depth: 2
    :local:

MPIRE allows you to provide shared objects to the workers in a similar way as is possible with the
``multiprocessing.Process`` class. For the start method ``fork`` these shared objects are treated as ``copy-on-write``,
which means they are only copied once changes are made to them. Otherwise they share the same memory address. This is
convenient if you want to let workers access a large dataset that wouldn't fit in memory when copied multiple times.

.. note::

    The start method ``fork`` isn't available on Windows, which means copy-on-write isn't supported there.

For ``threading`` these shared objects are readable and writable without copies being made. For the start methods
``spawn`` and ``forkserver`` the shared objects are copied once for each worker, in contrast to copying it for each
task which is done when using a regular ``multiprocessing.Pool``.

.. code-block:: python

    def task(dataset, x):
        # Do something with this copy-on-write dataset
        ...

    def main():
        dataset = ... # Load big dataset
        with WorkerPool(n_jobs=4, shared_objects=dataset, start_method='fork') as pool:
            ... = pool.map(task, range(100))

Multiple objects can be provided by placing them, for example, in a tuple container.

Apart from sharing regular Python objects between workers, you can also share multiprocessing synchronization
primitives such as ``multiprocessing.Lock`` using this method. Objects like these require to be shared through
inheritance, which is exactly how shared objects in MPIRE are passed on.

.. important::

    Shared objects are passed on as the second argument, after the worker ID (when enabled), to the provided function.

Instead of passing the shared objects to the :obj:`mpire.WorkerPool` constructor you can also use the
:meth:`mpire.WorkerPool.set_shared_objects` function:

.. code-block:: python

    def main():
        dataset = ... # Load big dataset
        with WorkerPool(n_jobs=4, start_method='fork') as pool:
            pool.set_shared_objects(dataset)
            ... = pool.map(task, range(100))

Shared objects have to be specified before the workers are started. Workers are started once the first ``map`` call is
executed. When ``keep_alive=True`` and the workers are reused, changing the shared objects between two consecutive
``map`` calls won't work.


Copy-on-write alternatives
--------------------------

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

In the example above we create two results containers, one for squaring and for adding the given value, and disable
locking for both. Additionally, we also return a value, even though we use shared objects for storing results. We can
safely disable locking here as each task writes to a different index in the array, so no race conditions can occur.
Disabling locking is, of course, a lot faster than having it enabled.
