.. _worker insights:

Worker insights
===============

Worker insights gives you insight in your multiprocessing efficiency by tracking worker start up time, waiting time and
time spend on executing tasks. Tracking is disabled by default, but can be enabled by setting ``enable_insights``:

.. code-block:: python

    with WorkerPool(n_jobs=4, enable_insights=True) as pool:
        pool.map(task, range(100))

The overhead is very minimal and you shouldn't really notice it, even on very small tasks. You can view the tracking
results using :meth:`mpire.WorkerPool.get_insights` or use :meth:`mpire.WorkerPool.print_insights` to directly print
the insights to console:

.. code-block:: python

    import time

    def sleep_and_square(x):
        # For illustration purposes
        time.sleep(x / 1000)
        return x * x

    with WorkerPool(n_jobs=4, enable_insights=True) as pool:
        pool.map(sleep_and_square, range(100))
        insights = pool.get_insights()
        print(insights)

    # Output:
    {'n_completed_tasks': [28, 24, 24, 24],
     'total_start_up_time': '0:00:00.038',
     'total_init_time': '0:00:00',
     'total_waiting_time': '0:00:00.798',
     'total_working_time': '0:00:04.980',
     'total_exit_time': '0:00:00',
     'total_time': '0:00:05.816',
     'start_up_time': ['0:00:00.010', '0:00:00.008', '0:00:00.008', '0:00:00.011'],
     'start_up_time_mean': '0:00:00.009',
     'start_up_time_std': '0:00:00.001',
     'start_up_ratio': 0.006610452621805033,
     'init_time': ['0:00:00', '0:00:00', '0:00:00', '0:00:00'],
     'init_time_mean': '0:00:00',
     'init_time_std': '0:00:00',
     'init_ratio': 0.0,
     'waiting_time': ['0:00:00.309', '0:00:00.311', '0:00:00.165', '0:00:00.012'],
     'waiting_time_mean': '0:00:00.199',
     'waiting_time_std': '0:00:00.123',
     'waiting_ratio': 0.13722942739284952,
     'working_time': ['0:00:01.142', '0:00:01.135', '0:00:01.278', '0:00:01.423'],
     'working_time_mean': '0:00:01.245',
     'working_time_std': '0:00:00.117',
     'working_ratio': 0.8561601182661567,
     'exit_time': ['0:00:00', '0:00:00', '0:00:00', '0:00:00']
     'exit_time_mean': '0:00:00',
     'exit_time_std': '0:00:00',
     'exit_ratio': 0.0,
     'top_5_max_task_durations': ['0:00:00.099', '0:00:00.098', '0:00:00.097', '0:00:00.096',
                                  '0:00:00.095'],
     'top_5_max_task_args': ['Arg 0: 99', 'Arg 0: 98', 'Arg 0: 97', 'Arg 0: 96', 'Arg 0: 95']}

We specified 4 workers, so there are 4 entries in the ``n_completed_tasks``, ``start_up_time``, ``init_time``,
``waiting_time``, ``working_time``, and ``exit_time`` containers. They show per worker the number of completed tasks,
the total start up time, the total time spend on the ``worker_init`` function, the total time waiting for new tasks,
total time spend on main function, and the total time spend on the ``worker_exit`` function, respectively. The insights
also contain mean, standard deviation, and ratio of the tracked time. The ratio is the time for that part divided by the
total time. In general, the higher the working ratio the more efficient your multiprocessing setup is. Of course, your
setup might still not be optimal because the task itself is inefficient, but timing that is beyond the scope of MPIRE.

Additionally, the insights keep track of the top 5 tasks that took the longest to run. The data is split up in two
containers: one for the duration and one for the arguments that were passed on to the task function. Both are sorted
based on task duration (desc), so index ``0`` of the args list corresponds to index ``0`` of the duration list, etc.

When using the MPIRE :ref:`Dashboard` you can track these insights in real-time. See :ref:`Dashboard` for more
information.

.. note::

    When using `imap` or `imap_unordered` you can view the insights during execution. Simply call ``get_insights()``
    or ``print_insights()`` inside your loop where you process the results.

.. note::

    When using Windows the arguments of the top 5 longest tasks are not available.
