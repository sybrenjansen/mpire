import itertools
import queue
from multiprocessing import cpu_count, Event, Process, Queue


class Worker(Process):
    """
    A multiprocessing helper class which continuously asks the queue for new jobs, until a poison pill is inserted
    """

    def __init__(self, worker_id, tasks_queue, results_queue, restart_queue, func_pointer, keep_order_event,
                 shared_objects=None, has_return_value_with_shared_objects=False, worker_lifespan=None):
        """
        :param worker_id: Worker id
        :param tasks_queue: Queue object for retrieving new task arguments
        :param results_queue: Queue object for storing the results
        :param restart_queue: Queue object for notifying the WorkerPool to restart this worker
        :param func_pointer: Function pointer to call each time new task arguments become available
        :param keep_order_event: Event object which signals if the task arguments contain an order index which should
            be preserved and not fed to the function pointer (e.g., used in map)
        :param shared_objects: None or an iterable of process-aware shared objects (e.g., multiprocessing.Array) to pass
            to the function as the first argument. When shared_object is specified it will assume the function to
            execute will not have any return value. If it both uses shared objects and a return value, set
            has_return_value_with_shared_objects to True.
        :param worker_lifespan: Int or None. Number of chunks a worker can handle before it is restarted. If None,
            workers will stay alive the entire time. Use this when workers use up too much memory over the course of
            time.
        :param has_return_value_with_shared_objects: Boolean. Whether or not the function has a return value when shared
            objects are passed to it. If False, will not put any returned values in the results queue.
        """
        super().__init__()
        self.worker_id = worker_id
        self.tasks_queue = tasks_queue
        self.results_queue = results_queue
        self.restart_queue = restart_queue
        self.func_pointer = func_pointer
        self.keep_order_event = keep_order_event
        self.shared_objects = shared_objects
        self.has_return_value_with_shared_objects = has_return_value_with_shared_objects
        self.worker_lifespan = worker_lifespan

    def helper_func(self, idx, args):
        """
        Helper function which calls the function pointer but preserves the order index.

        :param idx: Order index (handled by the WorkerPool)
        :param args: Task arguments
        """
        return idx, self.func_pointer(*args)

    def helper_func_with_shared_objects(self, shared_objects, idx, args):
        """
        Helper function which calls the function pointer but preserves the order index.

        :param shared_objects: An iterable of process-aware shared objects (e.g., multiprocessing.Array) to pass to the
            function as the first argument.
        :param idx: Order index (handled by the WorkerPool)
        :param args: Task arguments
        """
        return idx, self.func_pointer(shared_objects, *args)

    def run(self):
        """
        Continuously asks the tasks queue for new task arguments. When not receiving a poisonous pill or when the max
        life span is not yet reached it will execute the new task and put the results in the results queue.
        """
        n_chunks_executed = 0
        while self.worker_lifespan is None or n_chunks_executed < self.worker_lifespan:
            # Obtain new job
            next_chunked_args = self.tasks_queue.get()
            if next_chunked_args is None:
                # Poison pill means we should exit
                break

            # Function to call
            if self.keep_order_event.is_set():
                func = self.helper_func_with_shared_objects if self.shared_objects else self.helper_func
            else:
                func = self.func_pointer

            # Execute job
            if self.shared_objects is None:
                self.results_queue.put([func(*args) for args in next_chunked_args])
            # If shared objects are used check if the function also has a return value
            elif self.has_return_value_with_shared_objects:
                self.results_queue.put([func(self.shared_objects, *args) for args in next_chunked_args])
            else:
                for args in next_chunked_args:
                    func(self.shared_objects, *args)
                self.results_queue.put([None])

            # Increment counter
            n_chunks_executed += 1

        # Notify WorkerPool to start a new worker if max lifespan is reached
        if self.worker_lifespan is not None and n_chunks_executed == self.worker_lifespan:
            self.restart_queue.put(self.worker_id)


class WorkerPool:
    """
    A multiprocessing worker pool which acts like a multiprocessing.Pool, but is faster.
    """

    def __init__(self, n_jobs=None):
        """
        :param n_jobs: Int or None. Number of workers to spawn. If None, will use cpu_count() - 1.
        """
        self.n_jobs = n_jobs
        self.tasks_queue = None
        self.results_queue = None
        self.restart_queue = None
        self.keep_order_event = Event()
        self.workers = []
        self.shared_objects = None
        self.has_return_value_with_shared_objects = False
        self.func_pointer = None
        self.worker_lifespan = None

    def set_shared_objects(self, shared_objects=None, has_return_value_with_shared_objects=True):
        """
        Set shared objects to pass to the workers.

        :param shared_objects: None or an iterable of process-aware shared objects (e.g., multiprocessing.Array) to pass
            to the function as the first argument. When shared_object is specified and the function to execute does not
            have any return value, set has_return_value_with_shared_objects to False.
        :param has_return_value_with_shared_objects: Boolean. Whether or not the function has a return value when shared
            objects are passed to it. If False, will not put any returned values in the results queue. In this case, the
            user has to check whether or not the workers are done processing.
        """
        self.shared_objects = shared_objects
        self.has_return_value_with_shared_objects = has_return_value_with_shared_objects

    def start_workers(self, func_pointer, worker_lifespan):
        """
        Spawns the workers and starts them so they're ready to start reading from the tasks queue

        :param func_pointer: Function pointer to call each time new task arguments become available
        :param worker_lifespan: Int or None. Number of chunks a worker can handle before it is restarted. If None,
            workers will stay alive the entire time. Use this when workers use up too much memory over the course of
            time.
        """
        # If there are workers, join them first
        self.stop_and_join()

        # If worker lifespan is not None or not a positive integer, raise
        if worker_lifespan is not None and not (isinstance(worker_lifespan, int) and worker_lifespan > 0):
            raise ValueError("worker_lifespan should be either None or a positive integer (> 0)")

        # Save params for later reference (for example, when restarting workers)
        self.func_pointer = func_pointer
        self.worker_lifespan = worker_lifespan

        # Start new workers
        self.tasks_queue = Queue()
        self.results_queue = Queue()
        self.restart_queue = Queue()
        for worker_id in range(self.n_jobs if self.n_jobs is not None else cpu_count() - 1):
            w = Worker(worker_id, self.tasks_queue, self.results_queue, self.restart_queue, self.func_pointer,
                       self.keep_order_event, self.shared_objects, self.has_return_value_with_shared_objects,
                       self.worker_lifespan)
            w.daemon = True
            w.start()
            self.workers.append(w)

    def _restart_workers(self):
        """
        Restarts workers that need to be restarted
        """
        while True:
            try:
                # Obtain worker id that needs to be restarted
                worker_id = self.restart_queue.get(block=False)
                self.workers[worker_id].join()

                # Start worker
                w = Worker(worker_id, self.tasks_queue, self.results_queue, self.restart_queue, self.func_pointer,
                           self.keep_order_event, self.shared_objects, self.has_return_value_with_shared_objects,
                           self.worker_lifespan)
                w.daemon = True
                w.start()
                self.workers[worker_id] = w
            except queue.Empty:
                # There are no workers to be restarted, we're done here
                break

    def add_task(self, args):
        """
        Add a task to the queue so a worker can process it

        :param args: A tuple of arguments to pass to a worker, which passes it to the function pointer
        """
        self.tasks_queue.put(args)

    def get_result(self):
        """
        Obtain the next result from the results queue

        :return: Various. The next result from the queue, which is the result of calling the function pointer.
        """
        return self.results_queue.get(block=True)

    def insert_poison_pill(self):
        """
        Tell the workers their job is done by killing them brutally
        """
        for _ in range(len(self.workers)):
            self.tasks_queue.put(None)

    stop_workers = insert_poison_pill

    def join(self):
        """
        Waits until all workers are finished.

        Note that the results queue should be drained first before joining the workers, otherwise we can get a deadlock.
        For more information, see the warnings at:
        https://docs.python.org/3.4/library/multiprocessing.html#pipes-and-queues.
        """
        for w in self.workers:
            w.join()

    def stop_and_join(self):
        """
        Inserts a poison pill and waits until all workers are finished.

        Note that the results queue should be drained first before joining the workers, otherwise we can get a deadlock.
        For more information, see the warnings at:
        https://docs.python.org/3.4/library/multiprocessing.html#pipes-and-queues.
        """
        if self.workers:
            self.insert_poison_pill()
            self.join()
            self.workers = []
            self.tasks_queue = None
            self.results_queue = None

    def terminate(self):
        """
        Does not wait until all workers are finished, but terminates them with a SIGTERM.
        """
        for w in self.workers:
            w.terminate()
            self.workers = []
            self.tasks_queue = None
            self.results_queue = None

    def __enter__(self):
        """
        Enable the use of a 'with' statement.
        """
        return self

    def __exit__(self, *_):
        """
        Enable the use of a 'with' statement. Waits until the workers are finished automatically.
        """
        if self.tasks_queue is not None and (not self.tasks_queue.empty() or not self.results_queue.empty()):
            self.terminate()
        else:
            self.stop_and_join()

    def map(self, func_pointer, iterable_of_args, iterable_len=None, max_tasks_active=None, chunk_size=None,
            restart_workers=False, worker_lifespan=None):
        """
        Same as multiprocessing.map(). Also allows a user to set the maximum number of tasks available in the queue.
        Note that this function can be slower than the unordered version.

        :param func_pointer: Function pointer to call each time new task arguments become available
        :param iterable_of_args: An iterable containing tuples of arguments to pass to a worker, which passes it to the
            function pointer
        :param iterable_len: Int or None. When chunk_size is set to None it needs to know the number of tasks. This can
            either be provided by implementing the __len__ function on the iterable object, or by specifying the number
            of tasks.
        :param max_tasks_active: Int or None. Maximum number of active tasks in the queue. Use None to not limit the
            queue
        :param chunk_size: Int or None. Number of simultaneous tasks to give to a worker. If None, will generate n_jobs
            * 4 number of chunks.
        :param restart_workers: Boolean. Whether to restart the possibly already existing workers or use the old ones.
            Note: in the latter case the func_pointer parameter will have no effect. Will start workers either way when
            there are none.
        :param worker_lifespan: Int or None. Number of chunks a worker can handle before it is restarted. If None,
            workers will stay alive the entire time. Use this when workers use up too much memory over the course of
            time.
        :return: List with ordered results
        """
        # Notify workers to keep order in mind
        self.keep_order_event.set()

        # Process all args
        if iterable_len is None and hasattr(iterable_of_args, '__len__'):
            iterable_len = len(iterable_of_args)
        results = self.map_unordered(func_pointer, ((args_idx, args) for args_idx, args in enumerate(iterable_of_args)),
                                     iterable_len, max_tasks_active, chunk_size, restart_workers, worker_lifespan)

        # Notify workers to forget about order
        self.keep_order_event.clear()

        # Rearrange and return
        return [result[1] for result in sorted(results, key=lambda result: result[0])]

    def map_unordered(self, func_pointer, iterable_of_args, iterable_len=None, max_tasks_active=None, chunk_size=None,
                      restart_workers=False, worker_lifespan=None):
        """
        Same as multiprocessing.map(), but then unordered. Also allows a user to set the maximum number of tasks
        available in the queue.

        :param func_pointer: Function pointer to call each time new task arguments become available
        :param iterable_of_args: An iterable containing tuples of arguments to pass to a worker, which passes it to the
            function pointer
        :param iterable_len: Int or None. When chunk_size is set to None it needs to know the number of tasks. This can
            either be provided by implementing the __len__ function on the iterable object, or by specifying the number
            of tasks.
        :param max_tasks_active: Int or None. Maximum number of active tasks in the queue. Use None to not limit the
            queue
        :param chunk_size: Int or None. Number of simultaneous tasks to give to a worker. If None, will generate n_jobs
            * 4 number of chunks.
        :param restart_workers: Boolean. Whether to restart the possibly already existing workers or use the old ones.
            Note: in the latter case the func_pointer parameter will have no effect. Will start workers either way when
            there are none.
        :param worker_lifespan: Int or None. Number of chunks a worker can handle before it is restarted. If None,
            workers will stay alive the entire time. Use this when workers use up too much memory over the course of
            time.
        :return: List with unordered results
        """
        # Simply call imap and cast it to a list. This make sure all elements are there before returning
        return list(self.imap_unordered(func_pointer, iterable_of_args, iterable_len, max_tasks_active, chunk_size,
                                        restart_workers, worker_lifespan))

    def imap(self, func_pointer, iterable_of_args, iterable_len=None, max_tasks_active=None, chunk_size=None,
             restart_workers=False, worker_lifespan=None):
        """
        Same as multiprocessing.imap_unordered(), but then ordered. Also allows a user to set the maximum number of
        tasks available in the queue.

        :param func_pointer: Function pointer to call each time new task arguments become available
        :param iterable_of_args: An iterable containing tuples of arguments to pass to a worker, which passes it to the
            function pointer
        :param iterable_len: Int or None. When chunk_size is set to None it needs to know the number of tasks. This can
            either be provided by implementing the __len__ function on the iterable object, or by specifying the number
            of tasks.
        :param max_tasks_active: Int or None. Maximum number of active tasks in the queue. Use None to not limit the
            queue
        :param chunk_size: Int or None. Number of simultaneous tasks to give to a worker. If None, will generate n_jobs
            * 4 number of chunks.
        :param restart_workers: Boolean. Whether to restart the possibly already existing workers or use the old ones.
            Note: in the latter case the func_pointer parameter will have no effect. Will start workers either way when
            there are none.
        :param worker_lifespan: Int or None. Number of chunks a worker can handle before it is restarted. If None,
            workers will stay alive the entire time. Use this when workers use up too much memory over the course of
            time.
        :return: Generator yielding ordered results
        """
        # Notify workers to keep order in mind
        self.keep_order_event.set()

        # Yield results in order
        next_result_idx = 0
        tmp_results = {}
        if iterable_len is None and hasattr(iterable_of_args, '__len__'):
            iterable_len = len(iterable_of_args)
        for result_idx, result in self.imap_unordered(func_pointer, ((args_idx, args) for args_idx, args
                                                      in enumerate(iterable_of_args)), iterable_len, max_tasks_active,
                                                      chunk_size, restart_workers, worker_lifespan):
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

    def imap_unordered(self, func_pointer, iterable_of_args, iterable_len=None, max_tasks_active=None, chunk_size=None,
                       restart_workers=True, worker_lifespan=None):
        """
        Same as multiprocessing.imap_unordered(). Also allows a user to set the maximum number of tasks available in the
        queue.

        :param func_pointer: Function pointer to call each time new task arguments become available
        :param iterable_of_args: An iterable containing tuples of arguments to pass to a worker, which passes it to the
            function pointer
        :param iterable_len: Int or None. When chunk_size is set to None it needs to know the number of tasks. This can
            either be provided by implementing the __len__ function on the iterable object, or by specifying the number
            of tasks.
        :param max_tasks_active: Int, 'n_jobs*2', or None. Maximum number of active tasks in the queue. Use None to not
            limit the queue. Use 'n_jobs*2' to specify twice the number of jobs.
        :param chunk_size: Int or None. Number of simultaneous tasks to give to a worker. If None, will generate n_jobs
            * 4 number of chunks.
        :param restart_workers: Boolean. Whether to restart the possibly already existing workers or use the old ones.
            Note: in the latter case the func_pointer parameter will have no effect. Will start workers either way when
            there are none.
        :param worker_lifespan: Int or None. Number of chunks a worker can handle before it is restarted. If None,
            workers will stay alive the entire time. Use this when workers use up too much memory over the course of
            time.
        :return: Generator yielding unordered results
        """
        # Start workers
        if not self.workers or restart_workers:
            self.start_workers(func_pointer, worker_lifespan)

        # Chunk the function arguments
        iterator_of_chunked_args = self.chunk_tasks(iterable_of_args, iterable_len, chunk_size)

        # Process all args in the iterable. If maximum number of active tasks is None, we avoid all the if and
        # try-except clauses to speed up the process.
        n_active = 0
        if max_tasks_active == 'n_jobs*2':
            max_tasks_active = len(self.workers) * 2
        if max_tasks_active is None:
            for chunked_args in iterator_of_chunked_args:
                self.add_task(chunked_args)
                n_active += 1

                # Restart workers if necessary
                self._restart_workers()
        elif isinstance(max_tasks_active, int) and max_tasks_active > 0:
            while True:
                # Add task, only if allowed and if there are any
                if n_active < max_tasks_active:
                    try:
                        self.add_task(next(iterator_of_chunked_args))
                        n_active += 1
                    except StopIteration:
                        break

                # Check if new results are available, but don't wait for it
                try:
                    yield from self.results_queue.get(block=False)
                    n_active -= 1
                except queue.Empty:
                    pass

                # Restart workers if necessary
                self._restart_workers()
        else:
            raise ValueError("Maximum number of active tasks must be at least 1")

        # Obtain the results not yet obtained
        while n_active != 0:
            try:
                yield from self.results_queue.get(block=True, timeout=0.1)
                n_active -= 1
            except queue.Empty:
                pass

            # Restart workers if necessary
            self._restart_workers()

    def chunk_tasks(self, iterable_of_args, iterable_len=None, chunk_size=None):
        """
        Chunks tasks such that individual workers will receive chunks of tasks rather than individual ones, which can
        speed up processing drastically.

        :param iterable_of_args: An iterable containing tuples of arguments to pass to a worker, which passes it to the
            function pointer
        :param iterable_len: Int or None. When chunk_size is set to None it needs to know the number of tasks. This can
            either be provided by implementing the __len__ function on the iterable object, or by specifying the number
            of tasks.
        :param chunk_size: Int or None. Number of simultaneous tasks to give to a worker. If None, will generate n_jobs
            * 4 number of chunks.
        :return: Generator of chunked task arguments
        """
        # Determine chunk size
        if chunk_size is None:
            if iterable_len is not None:
                chunk_size, extra = divmod(iterable_len, len(self.workers) * 4)
            elif hasattr(iterable_of_args, '__len__'):
                chunk_size, extra = divmod(len(iterable_of_args), len(self.workers) * 4)
            else:
                raise ValueError("Failed to obtain length of iterable when chunk size is None. Remedy: either provide "
                                 "an iterable with a len() function or specify iterable_len in the function call")
            if extra:
                chunk_size += 1

        # Chunk tasks
        args_iter = iter(iterable_of_args)
        while True:
            chunk = tuple(itertools.islice(args_iter, chunk_size))
            if not chunk:
                return
            yield chunk
