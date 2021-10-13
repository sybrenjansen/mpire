.. _start_methods:

Process start method
====================

.. contents:: Contents
    :depth: 2
    :local:

The ``multiprocessing`` package allows you to start processes using a few different methods: ``'fork'``, ``'spawn'`` or
``'forkserver'``. Threading is also available by using ``'threading'``. For detailed information on the multiprocessing
contexts, please refer to the multiprocessing documentation_ and caveats_ section. In short:

fork
    Copies the parent process such that the child process is effectively identical. This includes copying everything
    currently in memory. This is sometimes useful, but other times useless or even a serious bottleneck. ``fork``
    enables the use of copy-on-write shared objects (see :ref:`shared_objects`).
spawn
    Starts a fresh python interpreter where only those resources necessary are inherited.
forkserver
    First starts a server process (using ``'spawn'``). Whenever a new process is needed the parent process requests the
    server to fork a new process.
threading
    Starts child threads. Suffers from the Global Interpreter Lock (GIL), but works fine for I/O intensive tasks.

For an overview of start method availability and defaults, please refer to the following table:

.. list-table::
    :header-rows: 1

    * - Start method
      - Available on Unix
      - Available on Windows
    * - ``fork``
      - Yes (default)
      - No
    * - ``spawn``
      - Yes
      - Yes (default)
    * - ``forkserver``
      - Yes
      - No
    * - ``threading``
      - Yes
      - Yes

Spawn and forkserver
--------------------

When using ``spawn`` or ``forkserver`` as start method, be aware that global variables (constants are fine) might have a
different value than you might expect. You also have to import packages within the called function:

.. code-block:: python

    import os

    def failing_job(folder, filename):
        return os.path.join(folder, filename)

    # This will fail because 'os' is not copied to the child processes
    with WorkerPool(n_jobs=2, start_method='spawn') as pool:
        pool.map(failing_job, [('folder', '0.p3'), ('folder', '1.p3')])

.. code-block:: python

    def working_job(folder, filename):
        import os
        return os.path.join(folder, filename)

    # This will work
    with WorkerPool(n_jobs=2, start_method='spawn') as pool:
        pool.map(working_job, [('folder', '0.p3'), ('folder', '1.p3')])

A lot of effort has been put into making the progress bar, dashboard, and nested pools (with multiple progress bars)
work well with ``spawn`` and ``forkserver``. So, everything should work fine.

.. _documentation: https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods
.. _caveats: https://docs.python.org/3/library/multiprocessing.html#the-spawn-and-forkserver-start-methods
