.. _max_active_tasks:

Maximum number of active tasks
==============================

When you have tasks that take up a lot of memory you can limit the number of jobs or limit the number of active tasks
(i.e., the number of tasks currently being available to the workers, tasks that are in the queue ready to be processed).
The first option is the most obvious one to save memory when the processes themselves use up much memory. The second is
convenient when the argument list takes up too much memory. For example, suppose you want to kick off an enormous amount
of jobs (let's say a billion) of which the arguments take up 1 KB per task (e.g., large strings), then that task queue
would take up ~1 TB of memory!

In such cases, a good rule of thumb would be to have twice the amount of active tasks than there are jobs. This means
that when all workers complete their task at the same time each would directly be able to continue with another task.
When workers take on their new tasks the generator of tasks is iterated to the point that again there would be twice the
amount of active tasks.

In MPIRE, the maximum number of active tasks by default is set to ``n_jobs * 2``, so you don't have to tweak it for
memory optimization. If, for whatever reason, you want to change this behavior, you can do so by setting the
``max_active_tasks`` parameter:

.. code-block:: python

    with WorkerPool(n_jobs=4) as pool:
        results = pool.map(task, range(int(1e300)), iterable_len=int(1e300),
                           chunk_size=int(1e5), max_tasks_active=4)
