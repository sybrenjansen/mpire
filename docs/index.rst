Welcome to the MPIRE documentation!
===================================

MPIRE, short for MultiProcessing Is Really Easy, is a Python package for multiprocessing, but faster and more
user-friendly than the default multiprocessing package. It combines the convenient map like functions of
``multiprocessing.Pool`` with the benefits of using copy-on-write shared objects of ``multiprocessing.Process``,
together with easy-to-use worker state, worker insights, and progress bar functionality.

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
found at :ref:`troubleshooting_windows`.

.. _benchmarks: https://towardsdatascience.com/mpire-for-python-multiprocessing-is-really-easy-d2ae7999a3e9
.. _dill: https://pypi.org/project/dill/
.. _multiprocess: https://github.com/uqfoundation/multiprocess
.. _tqdm: https://tqdm.github.io/

Contents
--------

.. toctree::
    :hidden:

    self

.. toctree::
    :maxdepth: 3
    :titlesonly:

    install
    getting_started
    usage/index
    troubleshooting
    reference/index
    changelog
