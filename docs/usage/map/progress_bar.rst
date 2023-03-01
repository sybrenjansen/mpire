Progress bar
============

.. contents:: Contents
    :depth: 2
    :local:

Progress bar support is added through the tqdm_ package (installed by default when installing MPIRE). The most easy way
to include a progress bar is by enabling the ``progress_bar`` flag in any of the ``map`` functions:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        pool.map(task, range(100), progress_bar=True)

This will display a basic ``tqdm`` progress bar displaying the time elapsed and remaining, number of tasks completed
(including a percentage value) and the speed (i.e., number of tasks completed per time unit).

When inside a Jupyter/IPython notebook, the progress bar will change automatically to a native Jupyter widget.

.. note::

    If you run into problems with getting the progress bar to work in a Jupyter notebook, have a look at
    :ref:`troubleshooting_progress_bar`.


Progress bar options
--------------------

The ``tqdm`` progress bar can be configured using the ``progress_bar_options`` parameter. This parameter accepts a
dictionary with keyword arguments that will be passed to the ``tqdm`` constructor.

Some options in ``tqdm`` will be overwritten by MPIRE. These include the ``iterable``, ``total`` and ``leave``
parameters. The ``iterable`` is set to the iterable passed on to the ``map`` function. The ``total`` parameter is set to
the number of tasks to be completed. The ``leave`` parameter is always set to ``True``. Some other parameters have a
default value assigned to them, but can be overwritten by the user.

Here's an example where we change the description, the units, and the colour of the progress bar:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        pool.map(some_func, some_data, progress_bar=True,
                 progress_bar_options={'desc': 'Processing', 'unit': 'items', 'colour': 'green'})

For a complete list of available options, check out the `tqdm docs`_.

.. _`tqdm docs`: https://tqdm.github.io/docs/tqdm/#__init__

Progress bar position
~~~~~~~~~~~~~~~~~~~~~

You can easily print a progress bar on a different position on the terminal using the ``position`` parameter of
``tqdm``, which facilitates the use of multiple progress bars. Here's an example of using multiple progress bars using
nested WorkerPools:

.. code-block:: python

    def dispatcher(worker_id, X):
        with WorkerPool(n_jobs=4) as nested_pool:
            return nested_pool.map(task, X, progress_bar=True,
                                   progress_bar_options={'position': worker_id + 1})

    def main():
        with WorkerPool(n_jobs=4, daemon=False, pass_worker_id=True) as pool:
            pool.map(dispatcher, ((range(x, x + 100),) for x in range(100)), iterable_len=100,
                     n_splits=4, progress_bar=True)

    main()

We use ``worker_id + 1`` here because the worker IDs start at zero and we reserve position 0 for the progress bar of
the main WorkerPool (which is the default).

It goes without saying that you shouldn't specify the same progress bar position multiple times.

.. note::

    Most progress bar options are completely ignored when in a Jupyter/IPython notebook session or in the MPIRE
    dashboard.

.. _tqdm: https://pypi.python.org/pypi/tqdm
