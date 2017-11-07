Changelog
=========

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

* ``restart_workers`` parameter is now deprecated and will be removed from v1.0.0.
* Progress bar functionality added (using tqdm_).
* Improved error handling in user provided functions.
* Fixed randomly occurring ``BrokenPipeErrors`` and deadlocks.


0.5.1
-----

*(2017-10-12)*

* Child processes can now also be pinned to a range of CPUs, instead of only a single one. You can also use specify a
  single CPU or range of CPUs that have to be shared between all child processes.

0.5.0
-----

*(2017-10-06)*

* Added CPU pinning.
* Default number of processes to spawn when using ``n_jobs=None`` is now set to the number of CPUs available, instead of
  ``cpu_count() - 1``.

0.4.0
-----

*(2017-10-05)*

* Workers can now be started as normal child processes (non-deamon) such that nested :obj:`mpire.WorkerPool` s are
  possible.

0.3.0
-----

*(2017-09-15)*

* The worker ID can now be passed on the function to be executed by using the :meth:`mpire.WorkerPool.pass_on_worker_id`
  function.
* Removed the use of ``has_return_value_with_shared_objects`` when using :meth:`mpire.WorkerPool.set_shared_objects`.
  MPIRE now handles both cases out of the box.

0.2.0
-----

*(2017-06-27)*

* Added docs


.. _tqdm: https://pypi.python.org/pypi/tqdm
