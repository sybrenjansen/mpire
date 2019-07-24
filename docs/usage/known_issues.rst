Known issues
============

This section describes the known issues in MPIRE.

.. contents:: Contents
    :depth: 2
    :local:


Unit tests
----------

When using the ``'spawn'`` or ``'forkserver'`` method you'll probably run in to one or two issues when running
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
