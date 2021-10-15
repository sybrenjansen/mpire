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

    The Jupyter ``tqdm`` widget requires the Javascript widget to run, which might not be enabled by default. You will
    notice a ``Widget Javascript not detected`` error message in your notebook if so. To remedy this, enable the widget
    by executing ``jupyter nbextension enable --py --sys-prefix widgetsnbextension`` in your terminal before starting
    the notebook.

.. note::

    Please keep in mind that to show real-time progress information MPIRE starts an additional child process, which
    could consume a bit of the available compute power of your machine. However, this is often negligible.


Multiple progress bars with nested WorkerPools
----------------------------------------------

In MPIRE you can easily print a progress bar on a different position on the terminal using the ``progress_bar_position``
parameter in the map functions, which facilitates the use of multiple progress bars. Here's an example of using multiple
progress bars using nested WorkerPools:

.. code-block:: python

    from mpire import tqdm

    def dispatcher(worker_id, X):
        with WorkerPool(n_jobs=4) as nested_pool:
            return nested_pool.map(task, X, progress_bar=True,
                                   progress_bar_position=worker_id + 1)

    def main():
        with WorkerPool(n_jobs=4, daemon=False, pass_worker_id=True) as pool:
            pool.map(dispatcher, ((range(x, x + 100),) for x in range(100)), iterable_len=100,
                     n_splits=4, progress_bar=True)

    main()

We use ``worker_id + 1`` here because the worker IDs start at zero and we reserve position 0 for the progress bar of
the main WorkerPool (which is the default).

It goes without saying that you shouldn't specify the same progress bar position multiple times.

.. note::

    The progress bar position is completely ignored when in a Jupyter/IPython notebook session or in the MPIRE
    dashboard.

.. _tqdm: https://pypi.python.org/pypi/tqdm
