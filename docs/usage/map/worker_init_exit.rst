.. _worker_init_exit:

Worker init and exit
====================

When you want to initialize a worker you can make use of the ``worker_init`` parameter of any ``map`` function. This
will call the initialization function only once per worker. Similarly, if you need to clean up the worker at the end of
its lifecycle you can use the ``worker_exit`` parameter. Additionally, the exit function can return anything you like,
which can be collected using :meth:`mpire.WorkerPool.get_exit_results` after the workers are done.

Both init and exit functions receive the worker ID, shared objects, and worker state in the same way as the task
function does, given they're enabled.

For example:

.. code-block:: python

    def init_func(worker_state):
        # Initialize a counter for each worker
        worker_state['count_even'] = 0

    def square_and_count_even(worker_state, x):
        # Count number of even numbers and return the square
        if x % 2 == 0:
            worker_state['count_even'] += 1
        return x * x

    def exit_func(worker_state):
        # Return the counter
        return worker_state['count_even']

    with WorkerPool(n_jobs=4, use_worker_state=True) as pool:
        pool.map(square_and_count_even, range(100), worker_init=init_func, worker_exit=exit_func)
        print(pool.get_exit_results())  # Output, e.g.: [13, 13, 12, 12]
        print(sum(pool.get_exit_results()))  # Output: 50

.. important::

    When the ``worker_lifespan`` option is used to restart workers during execution, the exit function will be called
    for the worker that's shutting down and the init function will be called again for the new worker. Therefore, the
    number of elements in the list that's returned from :meth:`mpire.WorkerPool.get_exit_results` does not always equal
    ``n_jobs``.

.. important::

    When ``keep_alive`` is enabled the workers won't be terminated after a ``map`` call. This means the exit function
    won't be called until it's time for cleaning up the entire pool. You will have to explicitly call
    :meth:`mpire.WorkerPool.stop_and_join` to receive the exit results.
