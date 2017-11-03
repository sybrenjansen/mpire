Welcome to the MPIRE documentation!
===================================

MPIRE, short for MultiProcessing: Insanely Rapid Execution, combines the convenient map like functions of
``multiprocessing.Pool`` with the benefits of using copy-on-write shared objects of ``multiprocessing.Process``.

Features
--------

- Multiprocessing with map/map_unordered/imap/imap_unordered functions
- Easy use of copy-on-write shared objects with a pool of workers
- Automatic task chunking for all available map functions to speed up processing of small task queues
- Functions are only pickled once for each worker
- Adjustable maximum number of active tasks to avoid memory problems
- Automatic restarting of workers after a specified number of tasks to reduce memory footprint
- Nested pool of workers are allowed when setting the ``daemon`` option
- Child processes can be pinned to specific or a range of CPUs on Linux systems
- Progress bar support

If you have any issues or suggestions please inform the author.

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
