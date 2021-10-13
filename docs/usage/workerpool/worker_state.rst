.. _worker_state:

Worker state
============

.. contents:: Contents
    :depth: 2
    :local:

If you want to let each worker have its own state you can use the ``use_worker_state`` flag:

.. code-block:: python

    def task(worker_state, x):
        if "local_sum" not in worker_state:
            worker_state["local_sum"] = 0
        worker_state["local_sum"] += x

    with WorkerPool(n_jobs=4, use_worker_state=True) as pool:
        results = pool.map(task, range(100))

.. important::

    The worker state is passed on as the third argument, after the worker ID and shared objects (when enabled), to the
    provided function.

Instead of passing the flag to the :obj:`mpire.WorkerPool` constructor you can also make use of
:meth:`mpire.WorkerPool.set_use_worker_state`:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        pool.set_use_worker_state()
        pool.map(task, range(100))

Combining worker state with worker_init and worker_exit
-------------------------------------------------------

The worker state can be combined with the ``worker_init`` and ``worker_exit`` parameters of each ``map`` function,
leading to some really useful capabilities:

.. code-block:: python

    import numpy as np
    import pickle

    def load_big_model(worker_state):
        # Load a model which takes up a lot of memory
        with open('./a_really_big_model.p3', 'rb') as f:
            worker_state['model'] = pickle.load(f)

    def model_predict(worker_state, x):
        # Predict
        return worker_state['model'].predict(x)

    with WorkerPool(n_jobs=4, use_worker_state=True) as pool:
        # Let the model predict
        data = np.array([[...]])
        results = pool.map(model_predict, data, worker_init=load_big_model)

More information about the ``worker_init`` and ``worker_exit`` parameters can be found at :ref:`worker_init_exit`.

Combining worker state with keep_alive
--------------------------------------

By default, workers are restarted each time a ``map`` function is executed. As described in :ref:`keep_alive` this can
be circumvented by using ``keep_alive=True``. This also ensures worker state is kept across consecutive ``map`` calls:

.. code-block:: python

    with WorkerPool(n_jobs=4, use_worker_state=True, keep_alive=True) as pool:
        # Let the model predict
        data = np.array([[...]])
        results = pool.map(model_predict, data, worker_init=load_big_model)

        # Predict some more
        more_data = np.array([[...]])
        more_results = pool.map(model_predict, more_data)

In this example we don't need to supply the ``worker_init`` function to the second ``map`` call, as the workers will be
reused. When ``worker_lifespan`` is set, though, this rule doesn't apply.
