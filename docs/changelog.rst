Changelog
=========

Develop
-------

Nothing in development at the moment.

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