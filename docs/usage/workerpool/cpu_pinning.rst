CPU pinning
===========

You can pin the child processes of :obj:`mpire.WorkerPool` to specific CPUs by using the ``cpu_ids`` parameter in the
constructor:

.. code-block:: python

    # Pin the two child processes to CPUs 2 and 3
    with WorkerPool(n_jobs=2, cpu_ids=[2, 3]) as pool:
        ...

    # Pin the child processes to CPUs 40-59
    with WorkerPool(n_jobs=20, cpu_ids=list(range(40, 60))) as pool:
        ...

    # All child processes have to share a single core:
    with WorkerPool(n_jobs=4, cpu_ids=[0]) as pool:
        ...

    # All child processes have to share multiple cores, namely 4-7:
    with WorkerPool(n_jobs=4, cpu_ids=[[4, 5, 6, 7]]) as pool:
        ...

    # Each child process can use two distinctive cores:
    with WorkerPool(n_jobs=4, cpu_ids=[[0, 1], [2, 3], [4, 5], [6, 7]]) as pool:
        ...

CPU IDs have to be positive integers, not exceeding the number of CPUs available (which can be retrieved by using
:meth:`mpire.cpu_count`). Use ``None`` to disable CPU pinning (which is the default).

.. note::

    Pinning processes to CPU IDs doesn't work when using threading or when you're on macOS.