Welcome to the MPIRE documentation!
===================================

MPIRE, short for MultiProcessing Is Really Easy, is a Python package for multiprocessing, but faster and more
user-friendly than the default multiprocessing package. It combines the convenient map like functions of
``multiprocessing.Pool`` with the benefits of using copy-on-write shared objects of ``multiprocessing.Process``.

Features
--------

- Multiprocessing with map/map_unordered/imap/imap_unordered functions
- Easy use of copy-on-write shared objects with a pool of workers
- Each worker can have its own state (e.g., to load a memory-intensive model only once for each worker without the
  need of sending it through a queue)
- Automatic task chunking for all available map functions to speed up processing of small task queues (including numpy
  arrays)
- Adjustable maximum number of active tasks to avoid memory problems
- Automatic restarting of workers after a specified number of tasks to reduce memory footprint
- Nested pool of workers are allowed when setting the ``daemon`` option
- Child processes can be pinned to specific or a range of CPUs
- Multiple process start methods available, including: ``fork`` (default), ``forkserver``, ``spawn``, and ``threading``
- Progress bar support using tqdm_
- Progress dashboard support
- (Optional) dill_ support

If you have any issues or suggestions please inform the author.

.. _dill: https://pypi.org/project/dill/
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
    usage/index
    reference/index
    changelog
