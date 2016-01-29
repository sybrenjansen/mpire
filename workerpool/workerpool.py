from multiprocessing import cpu_count, Event, Process, Queue
import queue

class Worker(Process):
    """
    A multiprocessing helper class which continuously asks the queue for new jobs, until a poison pill is inserted
    """

    def __init__(self, tasks_queue, results_queue, func_pointer, keep_order_event, shared_array):
        """
        :param tasks_queue: Queue object for retrieving new task arguments
        :param results_queue: Queue object for storing the results
        :param func_pointer: Function pointer to call each time new task arguments become available
        :param keep_order_event: Event object which signals if the task arguments contain an order index which should
            be preserved and not fed to the function pointer (e.g., used in map)
        :param shared_array: Multiprocessing.Array object or None. When it is an array, results will not be written to
            the results queue, but to the array instead. In this case the user has to supply the index of the array to
            write to.
        """
        super().__init__()
        self.tasks_queue = tasks_queue
        self.results_queue = results_queue
        self.func_pointer = func_pointer
        self.keep_order_event = keep_order_event
        self.shared_array = shared_array

    def helper_func(self, idx, args):
        """
        Helper function which calls the function pointer but preserves the order index.

        :param idx: order index (handled by the WorkerPool)
        :param args: Task arguments
        """
        return idx, self.func_pointer(*args)

    def run(self):
        """
        Continuously asks the tasks queue for new task arguments. When not receiving a poisonous pill it will execute
        the new task and put the results in the results queue.
        """
        while True:
            # Obtain new job
            next_args = self.tasks_queue.get()
            if next_args is None:
                # Poison pill means we should exit
                break

            # Function to call
            func = self.helper_func if self.keep_order_event.is_set() else self.func_pointer

            # Execute job
            if self.shared_array is not None:
                func(self.shared_array, *next_args)
            else:
                self.results_queue.put(func(*next_args))

class WorkerPool:
    """
    A multiprocessing worker pool which acts like a multiprocessing.Pool, but spawns workers and takes care of inserting
    poison pills and so on.
    """

    def __init__(self, func_pointer, n_jobs=None, shared_array=None):
        """
        :param func_pointer: Function pointer to call each time new task arguments become available
        :param n_jobs: Int or None. Number of workers to spawn. If None, will use cpu_count() - 1.
        """
        self.n_jobs = n_jobs if n_jobs is not None else cpu_count() - 1
        self.tasks_queue = Queue()
        self.results_queue = Queue()
        self.func_pointer = func_pointer
        self.keep_order_event = Event()
        self.shared_array = shared_array
        self.workers = []

    def start_workers(self):
        """
        Spawns the workers and starts them so they're ready to start reading from the tasks queue
        """
        for _ in range(self.n_jobs):
            w = Worker(self.tasks_queue, self.results_queue, self.func_pointer, self.keep_order_event,
                       self.shared_array)
            w.daemon = True
            w.start()
            self.workers.append(w)

    def add_task(self, args):
        """
        Add a task to the queue so a worker can process it

        :param args: A tuple of arguments to pass to a worker, which passes it to the function pointer
        """
        self.tasks_queue.put(args)

    def insert_poison_pill(self):
        """
        Tell the workers their job is done by killing them brutally
        """
        for _ in range(self.n_jobs):
            self.tasks_queue.put(None)

    stop_workers = insert_poison_pill

    def get_result(self):
        """
        Obtain the next result from the results queue

        :return: Various. The next result from the queue, which is the result of calling the function pointer.
        """
        return self.results_queue.get(block=True)

    def join(self):
        """
        Waits until all workers are finished.

        Note that the results queue should be drained first before joining the workers, otherwise we can get a deadlock.
        For more information, see the warnings at:
        https://docs.python.org/3.4/library/multiprocessing.html#pipes-and-queues.
        """
        for w in self.workers:
            w.join()

    def __enter__(self):
        """
        Enable the use of a 'with' statement. Starts the workers automatically.
        """
        self.start_workers()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Enable the use of a 'with' statement. Waits until the workers are finished automatically.
        """
        self.insert_poison_pill()
        self.join()

    def map(self, iterable_of_args, max_tasks_active=None):
        """
        Same as multiprocessing.map(). Also allows a user to set the maximum number of tasks available in the queue.
        Note that this function can be slower than the unordered version.

        :param iterable_of_args: An iterable containing tuples of arguments to pass to a worker, which passes it to the
            function pointer
        :param max_tasks_active: Int or None. Maximum number of active tasks in the queue. Use None to not limit the
            queue
        :return: List with ordered results
        """
        # Notify workers to keep order in mind
        self.keep_order_event.set()

        # Process all args
        results = self.map_unordered(((args_idx, args) for args_idx, args in enumerate(iterable_of_args)),
                                     max_tasks_active)

        # Notify workers to forget about order
        self.keep_order_event.clear()

        # Rearrange and return
        return [result[1] for result in sorted(results, key=lambda result: result[0])]

    def map_unordered(self, iterable_of_args, max_tasks_active=None):
        """
        Same as multiprocessing.map(), but then unordered. Also allows a user to set the maximum number of tasks
        available in the queue.

        :param iterable_of_args: An iterable containing tuples of arguments to pass to a worker, which passes it to the
            function pointer
        :param max_tasks_active: Int or None. Maximum number of active tasks in the queue. Use None to not limit the
            queue
        :return: List with unordered results
        """
        # Simply call imap and cast it to a list. This make sure all elements are there before returning
        return list(self.imap_unordered(iterable_of_args, max_tasks_active))

    def imap(self, iterable_of_args, max_tasks_active=None):
        """
        Same as multiprocessing.imap_unordered(), but then ordered. Also allows a user to set the maximum number of
        tasks available in the queue.

        :param iterable_of_args: An iterable containing tuples of arguments to pass to a worker, which passes it to the
            function pointer
        :param max_tasks_active: Int or None. Maximum number of active tasks in the queue. Use None to not limit the
            queue
        :return: Generator yielding ordered results
        """
        # Notify workers to keep order in mind
        self.keep_order_event.set()

        # Yield results in order
        next_result_idx = 0
        tmp_results = {}
        for result_idx, result in self.imap_unordered(((args_idx, args) for args_idx, args
                                                       in enumerate(iterable_of_args)), max_tasks_active):
            # Check if the next one(s) to return is/are temporarily stored. We use a while-true block with dict.pop() to
            # keep the temporary store as small as possible
            while True:
                if next_result_idx in tmp_results:
                    yield tmp_results.pop(next_result_idx)
                    next_result_idx += 1
                else:
                    break

            # Check if the current result is the next one to return. If so, return it
            if result_idx == next_result_idx:
                yield result
                next_result_idx += 1
            # Otherwise, temporarily store the current result
            else:
                tmp_results[result_idx] = result

        # Yield all remaining results
        for result_idx in sorted(tmp_results.keys()):
            yield tmp_results.pop(result_idx)

        # Notify workers to forget about order
        self.keep_order_event.clear()

    def imap_unordered(self, iterable_of_args, max_tasks_active=None):
        """
        Same as multiprocessing.imap_unordered(). Also allows a user to set the maximum number of tasks available in the
        queue.

        :param iterable_of_args: An iterable containing tuples of arguments to pass to a worker, which passes it to the
            function pointer
        :param max_tasks_active: Int or None. Maximum number of active tasks in the queue. Use None to not limit the
            queue
        :return: Generator yielding unordered results
        """
        # Process all args in the iterable. If maximum number of active tasks is None, we avoid all the if and
        # try-except clauses to speed up the process.
        n_active = 0
        if max_tasks_active is None:
            for args in iterable_of_args:
                self.add_task(args)
                n_active += 1
        elif max_tasks_active > 0:
            iterator_of_args = iter(iterable_of_args)
            while True:
                # Add task, only if allowed and if there are any
                if n_active < max_tasks_active:
                    try:
                        self.add_task(next(iterator_of_args))
                        n_active += 1
                    except StopIteration:
                        break

                # Check if new results are available, but don't wait for it
                try:
                    yield self.results_queue.get(block=False)
                    n_active -= 1
                except queue.Empty:
                    pass
        else:
            raise ValueError("Maximum number of active tasks must be at least 1")

        # Obtain the results not yet obtained
        for _ in range(n_active):
            yield self.results_queue.get(block=True)
