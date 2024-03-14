Changelog
=========

Unreleased
----------

* Fixed a bug in the timeout handler where the cache dictionary could be changed during iteration (`#123`_)

.. _#123: https://github.com/sybrenjansen/mpire/issues/123


2.10.0
------

*(2024-02-19)*

* Added support for macOS (`#27`_, `#79`_, `#91`_)

  - Fixes memory leaks on macOS
  - Reduced the amount of semaphores used
  - Issues a warning when ``cpu_ids`` is used on macOS

* Added :meth:`mpire.dashboard.set_stacklevel` to set the stack level in the dashboard. This influences what line to 
  display in the 'Invoked on line' section. (`#118`_)
* Use function details from the `__call__` method on the dashboard in case the
  callable being executed is a class instance (`#117`_)
* Use (global) average rate for the estimate on the dashboard when smoothing=0
  (`#117`_)
* Make it possible to reuse the same `progress_bar_options` without raising warnings (`#117`_)
* Removed deprecated `progress_bar_position` parameter from the `map` functions. Use 
  `progress_bar_options['position']` instead (added since v2.6.0)

.. _#27: https://github.com/sybrenjansen/mpire/issues/27
.. _#79: https://github.com/sybrenjansen/mpire/issues/79
.. _#91: https://github.com/sybrenjansen/mpire/issues/91
.. _#117: https://github.com/sybrenjansen/mpire/pull/117
.. _#118: https://github.com/sybrenjansen/mpire/pull/118


2.9.0
-----

*(2024-01-08)*

* Added support for the ``rich`` progress bar style (`#96`_)
* Added the option to only show progress on the dashboard. (`#107`_)
* Progress bars are now supported on Windows when using threading as start method.
* Insights now also work when using the ``forkserver`` and ``spawn`` start methods. (`#104`_)
* When using insights on Windows the arguments of the top 5 longest tasks are now available as well.
* Fixed deprecated ``escape`` import from ``flask`` by importing directly from ``markupsafe``. (`#106`_)
* Fixed :meth:`mpire.dashboard.start_dashboard` freeze when there are no two ports available. (`#112`_)
* Added :meth:`mpire.dashboard.shutdown_dashboard` to shutdown the dashboard.
* Added ``py.typed`` file to prompt ``mypy`` for type checking. (`#108`_)

.. _#96: https://github.com/sybrenjansen/mpire/issues/96
.. _#107: https://github.com/sybrenjansen/mpire/pull/107
.. _#104: https://github.com/sybrenjansen/mpire/issues/104
.. _#106: https://github.com/sybrenjansen/mpire/issues/106
.. _#112: https://github.com/sybrenjansen/mpire/issues/112
.. _#108: https://github.com/sybrenjansen/mpire/pull/108


2.8.1
-----

*(2023-11-08)*

* Excluded the ``tests`` folder from MPIRE distributions (`#89`_)
* Added a workaround for semaphore leakage on macOS and fixed a bug when working in a fork context while the system
  default is spawn (`#92`_)
* Fix progressbar percentage on dashboard (`#101`_)
* Fixed a bug where starting multiple `apply_async` tasks with a task timeout didn't interrupt all tasks when the
  timeout was reached (`#98`_)
* Add testing python 3.12 to workflow and drop 3.6 and 3.7 (`#102`_)

.. _#89: https://github.com/sybrenjansen/mpire/issues/89
.. _#92: https://github.com/sybrenjansen/mpire/issues/92
.. _#98: https://github.com/sybrenjansen/mpire/issues/98
.. _#101: https://github.com/sybrenjansen/mpire/pull/101
.. _#102: https://github.com/sybrenjansen/mpire/pull/102


2.8.0
-----

*(2023-08-16)*

* Added support for Python 3.11 (`#67`_)

.. _#67: https://github.com/sybrenjansen/mpire/issues/67

2.7.1
-----

*(2023-04-14)*

* Transfered ownership of the project from `Slimmer AI` to `sybrenjansen`

2.7.0
-----

*(2023-03-17)*

* Added the :meth:`mpire.WorkerPool.apply` and :meth:`mpire.WorkerPool.apply_async` functions (`#63`_)
* When inside a Jupyter notebook, the progress bar will not automatically switch to a widget anymore. ``tqdm`` cannot
  always determine with certainty that someone is in a notebook or, e.g., a Jupyter console. Another reason is to avoid
  the many errors people get when having widgets or javascript disabled. See :ref:`progress_bar_style` for changing
  the progress bar to a widget (`#71`_)
* The :meth:`mpire.dashboard.connect_to_dashboard` function now raises a `ConnectionRefused` error when the dashboard
  isn't running, instead of silently failing and deadlocking the next ``map`` call with a progress bar (`#68`_)
* Added support for a progress bar without knowing the size of the iterable. It used to disable the progress bar when
  the size was unknown
* Changed how ``max_tasks_active`` is handled. It now applies to the number of tasks that are currently being
  processed, instead of the number of chunks of tasks, as you would expect from the name. Previously, when the chunk
  size was set to anything other than 1, the number of active tasks could be higher than ``max_tasks_active``
* Updated some exception messages and docs (`#69`_)
* Changed how worker results, restarts, timeouts, unexpected deaths, and exceptions are handled. They are now handled
  by individual threads such that the main thread is more responsive. The API is the same, so no user changes are
  needed
* Mixing multiple ``map`` calls now raises an error (see :ref:`mixing-multiple-map-calls`)
* Fixed a bug where calling a ``map`` function with a progress bar multiple times in a row didn't display the progress
  bar correctly
* Fixed a bug where the dashboard didn't show an error when an exit function raised an exception

.. _#63: https://github.com/sybrenjansen/mpire/issues/63
.. _#68: https://github.com/sybrenjansen/mpire/issues/68
.. _#69: https://github.com/sybrenjansen/mpire/issues/69
.. _#71: https://github.com/sybrenjansen/mpire/issues/71


2.6.0
-----

*(2022-08-29)*

* Added Python 3.10 support
* The ``tqdm`` progress bar can now be customized using the ``progress_bar_options`` parameter in the ``map`` functions
  (`#57`_)
* Using ``progress_bar_position`` from a ``map`` function is now deprecated and will be removed in MPIRE v2.10.0. Use
  ``progress_bar_options['position']`` instead
* Deprecated ``enable_insights`` from a ``map`` function, use ``enable_insights`` in the WorkerPool constructor instead
* Fixed a bug where a worker could exit before an exception was entirely sent over the queue, causing a deadlock
  (`#56`_)
* Fixed a bug where exceptions with init arguments weren't handled correctly (`#58`_)
* Fixed a rare and weird bug in Windows that could cause a deadlock (probably fixes `#55`_)

.. _#55: https://github.com/sybrenjansen/mpire/issues/55
.. _#56: https://github.com/sybrenjansen/mpire/issues/56
.. _#57: https://github.com/sybrenjansen/mpire/issues/57
.. _#58: https://github.com/sybrenjansen/mpire/issues/58


2.5.0
-----

*(2022-07-25)*

* Added the option to fix the order of tasks given to the workers (`#46`_)
* Fixed a bug where updated WorkerPool parameters aren't used in subsequent ``map`` calls when ``keep_alive`` is enabled

.. _#46: https://github.com/sybrenjansen/mpire/issues/46

2.4.0
-----

*(2022-05-25)*

* A timeout for the target, ``worker_init``, and ``worker_exit`` functions can be specified after which a worker is
  stopped (`#36`_)
* A WorkerPool can now be started within a thread which isn't the main thread (`#44`_)

.. _#36: https://github.com/sybrenjansen/mpire/issues/36
.. _#44: https://github.com/sybrenjansen/mpire/issues/44


2.3.5
-----

*(2022-04-25)*

* MPIRE now handles defunct child processes properly, instead of deadlocking (`#34`_)
* Added benchmark highlights to README (`#38`_)

.. _#34: https://github.com/sybrenjansen/mpire/issues/34
.. _#38: https://github.com/sybrenjansen/mpire/issues/38


2.3.4
-----

*(2022-03-29)*

* Platform specific dependencies are now handled using environment markers as defined in PEP-508_ (`#30`_)
* Fixes hanging ``WorkerPool`` when using ``worker_lifespan`` and returning results that exceed the pipe capacity
  (`#32`_)
* Fixes insights unit tests that could sometime fail because it was too fast

.. _PEP-508: https://www.python.org/dev/peps/pep-0508/#environment-markers
.. _#30: https://github.com/sybrenjansen/mpire/issues/30
.. _#32: https://github.com/sybrenjansen/mpire/issues/32

2.3.3
-----

*(2021-11-29)*

* Changed progress bar handler process to thread, making it more stable (especially in notebooks)
* Changed progress bar tasks completed queue to array, to make it more responsive and faster
* Disabled the tqdm monitor thread which, in combination with MPIRE's own tqdm lock, could result in deadlocks

2.3.2
-----

*(2021-11-19)*

* Included license file in source distribution (`#25`_)

.. _#25: https://github.com/sybrenjansen/mpire/pull/25

2.3.1
-----

*(2021-11-16)*

* Made connecting to the tqdm manager more robust (`#23`_)

.. _#23: https://github.com/sybrenjansen/mpire/issues/23

2.3.0
-----

*(2021-10-15)*

* Fixed progress bar in a particular setting with iPython and django installed (`#13`_)
* ``keep_alive`` now works even when the function to be called or any other parameter passed to the ``map`` function is
  changed (`#15`_)
* Moved ``enable_insights`` to the WorkerPool constructor. Using ``enable_insights`` from a ``map`` function is now
  deprecated and will be removed in MPIRE v2.6.0.
* Restructured docs and updated several sections for Windows users.

.. _#13: https://github.com/sybrenjansen/mpire/pull/13
.. _#15: https://github.com/sybrenjansen/mpire/issues/15

2.2.1
-----

*(2021-08-31)*

* Fixed compatibility with newer tqdm versions (``>= 4.62.2``) (`#11`_)

.. _#11: https://github.com/sybrenjansen/mpire/issues/11

2.2.0
-----

*(2021-08-30)*

* Added support for Windows (`#6`_, `#7`_). Support has a few caveats:

  * When using worker insights the arguments of the top 5 longest tasks are not available
  * Progress bar is not supported when using threading as start method
  * When using ``dill`` and an exception occurs, or when the exception occurs in an exit function, it can print
    additional ``OSError`` messages in the terminal, but these can be safely ignored.

.. _#6: https://github.com/sybrenjansen/mpire/issues/6
.. _#7: https://github.com/sybrenjansen/mpire/issues/7

2.1.1
-----

*(2021-08-26)*

* Fixed a bug with newer versions of tqdm. The progress bar would throw an ``AttributeError`` when connected to a
  dashboard.
* README and documentation updated

2.1.0
-----

*(2021-08-06)*

* Workers now have their own task queue, which speeds up tasks with bigger payloads
* Fixed progress bar showing error information when completed without error
* Fixed progress bar and worker insights not displaying properly when using threading
* Progress bar handling improved accross several scenarios
* Dashboard can now handle progress bars when using ``spawn`` or ``forkserver`` as start method
* Added closing of ``multiprocessing.JoinableQueue`` objects, to clean up intermediate junk
* Removed ``numpy`` dependency
* Made ``dill`` optional again. In many cases it slows processing down

2.0.0
-----

*(2021-07-07)*

* Worker insights added, providing users insight in multiprocessing efficiency
* ``worker_init`` and ``worker_exit`` parameters added to each ``map`` function
* ``max_active_tasks`` is now set to ``n_jobs * 2`` when ``max_active_tasks=None``, to speed up most jobs
* ``n_splits`` is now set to ``n_jobs * 64`` when both ``chunk_size`` and ``n_splits`` are ``None``
* Dashboard ports can now be configured
* Renamed ``func_pointer`` to ``func`` in each ``map`` function
* Fixed a bug with the `threading` backend not terminating correctly
* Fixed a bug with the progress bar not showing correctly in notebooks
* Using ``multiprocess`` is now the default
* Added some debug logging
* Refactored a lot of code
* Minor bug fixes, which should make things more stable.
* Removed Python 3.5 support
* Removed ``add_task``, ``get_result``, ``insert_poison_pill``, ``stop_workers``, and ``join`` functions from
  :obj:`mpire.WorkerPool`. Made ``start_workers`` private.  There wasn't any reason to use these functions.

1.2.2
-----

*(2021-04-23)*

* Updated documentation CSS which fixes bullet lists not showing properly

1.2.1
-----

*(2021-04-22)*

* Updated some unittests and fixed some linting issues
* Minor improvements in documentation

1.2.0
-----

*(2021-04-22)*

* Workers can be kept alive in between consecutive map calls
* Setting CPU affinity is no longer restricted to Linux platforms
* README updated to use RST format for better compatibility with PyPI
* Added classifiers to the setup file

1.1.3
-----

*(2020-09-03)*

* First public release on Github and PyPi

1.1.2
-----

*(2020-08-27)*

* Added missing typing information
* Updated some docstrings
* Added license

1.1.1
-----

*(2020-02-19)*

* Changed ``collections.Iterable`` to ``collections.abc.Iterable`` due to deprecation of the former

1.1.0
-----

*(2019-10-31)*

* Removed custom progress bar support to fix Jupyter notebook support
* New ``progress_bar_position`` parameter is now available to set the position of the progress bar when using nested
  worker pools
* Screen resizing is now supported when using a progress bar

1.0.0
-----

*(2019-10-29)*

* Added the MPIRE dashboard
* Added ``threading`` as a possible backend
* Progress bar handling now occurs in a separate process, instead of a thread, to improve responsiveness
* Refactoring of code and small bug fixes in error handling
* Removed deprecated functionality

0.9.0
-----

*(2019-03-11)*

* Added support for using different start methods ('spawn' and 'forkserver') instead of only the default method 'fork'
* Added optional support for using dill_ in multiprocessing by utilizing the multiprocess_ library
* The ``mpire.Worker`` class is no longer directly available

.. _dill: https://pypi.org/project/dill/
.. _multiprocess: https://pypi.org/project/multiprocess/

0.8.1
-----

*(2019-02-06)*

* Fixed bug when process would hang when progress bar was set to ``True`` and an empty iterable was provided

0.8.0
-----

*(2018-11-01)*

* Added support for worker state
* Chunking numpy arrays is now done using numpy slicing
* :meth:`mpire.WorkerPool.map` now supports automatic concatenation of numpy array output

0.7.2
-----

*(2018-06-14)*

* Small bug fix when not passing on a boolean or ``tqdm`` object for the ``progress_bar`` parameter

0.7.1
-----

*(2017-12-20)*

* You can now pass on a dictionary as an argument which will be unpacked accordingly using the ``**``-operator.
* New function :meth:`mpire.utils.make_single_arguments` added which allows you to create an iterable of single argument
  tuples out of an iterable of single arguments

0.7.0
-----

*(2017-12-11)*

* :meth:`mpire.utils.chunk_tasks` is now available as a public function
* Chunking in above function and map functions now accept a ``n_splits`` parameter
* ``iterable_of_args`` in map functions can now contain single values instead of only iterables
* ``tqdm`` is now available from the MPIRE package which automatically switches to the Jupyter/IPython notebook widget
  when available
* Small bugfix in cleaning up a worker pool when no map function was called

0.6.2
-----

*(2017-11-07)*

* Fixed a second bug where the main process could get unresponsive when an exception was raised

0.6.1
-----

*(2017-11-06)*

* Fixed bug where sometimes exceptions fail to pickle
* Fixed a bug where the main process could get unresponsive when an exception was raised
* Child processes are now cleaned up in parallel when an exception was raised

0.6.0
-----

*(2017-11-03)*

* ``restart_workers`` parameter is now deprecated and will be removed from v1.0.0
* Progress bar functionality added (using tqdm_)
* Improved error handling in user provided functions
* Fixed randomly occurring ``BrokenPipeErrors`` and deadlocks


0.5.1
-----

*(2017-10-12)*

* Child processes can now also be pinned to a range of CPUs, instead of only a single one. You can also specify a single
  CPU or range of CPUs that have to be shared between all child processes

0.5.0
-----

*(2017-10-06)*

* Added CPU pinning.
* Default number of processes to spawn when using ``n_jobs=None`` is now set to the number of CPUs available, instead of
  ``cpu_count() - 1``

0.4.0
-----

*(2017-10-05)*

* Workers can now be started as normal child processes (non-deamon) such that nested :obj:`mpire.WorkerPool` s are
  possible

0.3.0
-----

*(2017-09-15)*

* The worker ID can now be passed on the function to be executed by using the :meth:`mpire.WorkerPool.pass_on_worker_id`
  function
* Removed the use of ``has_return_value_with_shared_objects`` when using :meth:`mpire.WorkerPool.set_shared_objects`.
  MPIRE now handles both cases out of the box

0.2.0
-----

*(2017-06-27)*

* Added docs

0.1.0
-----

First release


.. _tqdm: https://pypi.python.org/pypi/tqdm
