Troubleshooting
===============

This section describes some known problems that can arise when using MPIRE.

.. contents:: Contents
    :depth: 2
    :local:


Unit tests
----------

When using the ``'spawn'`` or ``'forkserver'`` method you'll probably run into one or two issues when running
unittests in your own package. One problem that might occur is that your unittests will restart whenever the piece of
code containing such a start method is called, leading to very funky terminal output. To remedy this problem make sure
your ``setup`` call in ``setup.py`` is surrounded by an ``if __name__ == '__main__':`` clause:

.. code-block:: python

    from setuptools import setup

    if __name__ == '__main__':

        # Call setup and install any dependencies you have inside the if-clause
        setup(...)

See the 'Safe importing of main module' section at caveats_.

The second problem you might encounter is that the semaphore tracker of multiprocessing will complain when you run
individual (or a selection of) unittests using ``python setup.py test -s tests.some_test``. At the end of the tests you
will see errors like:

.. code-block:: python

    Traceback (most recent call last):
      File ".../site-packages/multiprocess/semaphore_tracker.py", line 132, in main
        cache.remove(name)
    KeyError: b'/mp-d3i13qd5'
    .../site-packages/multiprocess/semaphore_tracker.py:146: UserWarning: semaphore_tracker: There appear to be 58
                                                             leaked semaphores to clean up at shutdown
      len(cache))
    .../site-packages/multiprocess/semaphore_tracker.py:158: UserWarning: semaphore_tracker: '/mp-f45dt4d6': [Errno 2]
                                                             No such file or directory
      warnings.warn('semaphore_tracker: %r: %s' % (name, e))
    ...

Your unittests will still succeed and run OK. Unfortunately, I've not found a remedy to this problem using
``python setup.py test`` yet. What you can use instead is something like the following:

.. code-block:: python

    python -m unittest tests.some_test

This will work just fine. See the unittest_ documentation for more information.

.. _caveats: https://docs.python.org/3/library/multiprocessing.html#the-spawn-and-forkserver-start-methods
.. _unittest: https://docs.python.org/3.4/library/unittest.html#command-line-interface


Shutting down takes a long time on error
----------------------------------------

When you issue a ``KeyboardInterrupt`` or when an error occured in the function that's run in parallel, there are
situations where MPIRE needs a few seconds to gracefully shutdown. This has to do with the fact that in these situations
the task or results queue can be quite full, still. MPIRE drains these queues until they're completely empty, as to
properly shutdown and clean up every communication channel.

To remedy this issue you can use the ``max_tasks_active`` parameter and set it to ``n_jobs * 2``, or similar. Aside
from the added benefit that the workers can start more quickly, the queues won't get that full anymore and shutting down
will be much quicker. See :ref:`max_active_tasks` for more information.

When you're using a lazy map function also be sure to iterate through the results, otherwise that queue will be full and
draining it will take a longer time.


Unpicklable tasks/results
-------------------------

Sometimes you can encounter deadlocks in your code when using MPIRE. When you encounter this, chances are some tasks or
results from your script can't be pickled. MPIRE makes use of multiprocessing queues for inter-process communication and
if your function returns unpicklable results the queue will unfortunately deadlock.

The only way to remedy this problem in MPIRE would be to manually pickle objects before sending it to a queue and quit
gracefully when encountering a pickle error. However, this would mean objects would always be pickled twice. This would
add a heavy performance penalty and is therefore not an acceptable solution.

Instead, the user should make sure their tasks and results are always picklable (which in most cases won't be a
problem), or resort to setting ``use_dill=True``. The latter is capable of pickling a lot more exotic types. See
:ref:`use_dill` for more information.


Windows
-------

This package has been tested only on Linux-based systems. So I'm not sure if everything works on Windows. You can submit
an issue on GitHub if you encounter issues.

Windows doesn't support forking, so you will need to manually switch to spawn as start method. See :ref:`start_methods`
for more information on start methods.

Disclaimer: it could be that MPIRE can't be imported on systems without fork. If so, let me know by submitting an issue
on GitHub.
