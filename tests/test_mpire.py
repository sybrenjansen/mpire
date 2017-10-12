import unittest
from itertools import product, repeat
from mpire import cpu_count, WorkerPool


def square(idx, x):
    return idx, x * x


def square_daemon(X):
    with WorkerPool(n_jobs=4) as pool:
        return pool.map(square, X, chunk_size=1)


def square_daemon_raises(X):
    try:
        with WorkerPool(n_jobs=4) as pool:
            pool.map(square, X, chunk_size=1)
    except AssertionError:
        return True

    return False


def get_generator(iterable):
    yield from iterable


class MPIRETest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Create some test data. Note that the regular map reads the inputs as a list of single tuples (one argument),
        # whereas parallel.map sees it as a list of argument lists. Therefore we give the regular map a lambda function
        # which mimics the parallel.map behavior.
        self.test_data = list(enumerate([1, 2, 3, 5, 6, 9, 37, 42, 1337, 0, 3, 5, 0]))
        self.test_desired_output = list(map(lambda _args: square(*_args), self.test_data))
        self.test_data_len = len(self.test_data)

    def test_worker_pool_map(self):
        """
        Tests the map related function of the worker pool class
        """
        import types

        # Test results for different number of jobs to run in parallel and the maximum number of active tasks in the
        # queue
        for n_jobs, n_tasks_max_active, worker_lifespan in product([1, 2, None], [None, 1, 3], [None, 1, 3]):
            with WorkerPool(n_jobs=n_jobs) as pool:
                # Test if parallel map results in the same as ordinary map function. Should work both for generators
                # and iterators. Also check if an empty list works as desired.
                results_list = pool.map(square, self.test_data, max_tasks_active=n_tasks_max_active,
                                        worker_lifespan=worker_lifespan)
                self.assertTrue(isinstance(results_list, list))
                self.assertEqual(self.test_desired_output, results_list)

                results_list = pool.map(square, get_generator(self.test_data), iterable_len=self.test_data_len,
                                        max_tasks_active=n_tasks_max_active, worker_lifespan=worker_lifespan)
                self.assertTrue(isinstance(results_list, list))
                self.assertEqual(self.test_desired_output, results_list)

                results_list = pool.map(square, [], max_tasks_active=n_tasks_max_active,
                                        worker_lifespan=worker_lifespan)
                self.assertTrue(isinstance(results_list, list))
                self.assertEqual([], results_list)

                # Test if parallel map_unordered contains all results. Should work both for generators and iterators.
                # Also check if an empty list works as desired.
                results_list = pool.map_unordered(square, self.test_data, max_tasks_active=n_tasks_max_active,
                                                  worker_lifespan=worker_lifespan)
                self.assertTrue(isinstance(results_list, list))
                self.assertEqual(self.test_desired_output, sorted(results_list, key=lambda tup: tup[0]))

                results_list = pool.map_unordered(square, get_generator(self.test_data),
                                                  iterable_len=self.test_data_len, max_tasks_active=n_tasks_max_active,
                                                  worker_lifespan=worker_lifespan)
                self.assertTrue(isinstance(results_list, list))
                self.assertEqual(self.test_desired_output, sorted(results_list, key=lambda tup: tup[0]))

                results_list = pool.map_unordered(square, [], max_tasks_active=n_tasks_max_active,
                                                  worker_lifespan=worker_lifespan)
                self.assertTrue(isinstance(results_list, list))
                self.assertEqual([], sorted(results_list, key=lambda tup: tup[0]))

                # Test if parallel imap contains all results and if it returns an iterator. Should work for both
                # generators and iterators. Also check if an empty list works as desired.
                result_generator = pool.imap(square, self.test_data, max_tasks_active=n_tasks_max_active,
                                             worker_lifespan=worker_lifespan)
                self.assertTrue(isinstance(result_generator, types.GeneratorType))
                self.assertEqual(self.test_desired_output, list(result_generator))

                result_generator = pool.imap(square, get_generator(self.test_data), iterable_len=self.test_data_len,
                                             max_tasks_active=n_tasks_max_active, worker_lifespan=worker_lifespan)
                self.assertTrue(isinstance(result_generator, types.GeneratorType))
                self.assertEqual(self.test_desired_output, list(result_generator))

                result_generator = pool.imap(square, [], max_tasks_active=n_tasks_max_active,
                                             worker_lifespan=worker_lifespan)
                self.assertTrue(isinstance(result_generator, types.GeneratorType))
                self.assertEqual([], list(result_generator))

                # Test if parallel imap_unordered contains all results and if it returns an iterator. Should work for
                # both generators and iterators. Also check if an empty list works as desired.
                result_generator = pool.imap_unordered(square, self.test_data, max_tasks_active=n_tasks_max_active,
                                                       worker_lifespan=worker_lifespan)
                self.assertTrue(isinstance(result_generator, types.GeneratorType))
                self.assertEqual(self.test_desired_output, sorted(result_generator, key=lambda tup: tup[0]))

                result_generator = pool.imap_unordered(square, get_generator(self.test_data),
                                                       iterable_len=self.test_data_len,
                                                       max_tasks_active=n_tasks_max_active,
                                                       worker_lifespan=worker_lifespan)
                self.assertTrue(isinstance(result_generator, types.GeneratorType))
                self.assertEqual(self.test_desired_output, sorted(result_generator, key=lambda tup: tup[0]))

                result_generator = pool.imap_unordered(square, [], max_tasks_active=n_tasks_max_active,
                                                       worker_lifespan=worker_lifespan)
                self.assertTrue(isinstance(result_generator, types.GeneratorType))
                self.assertEqual([], sorted(result_generator, key=lambda tup: tup[0]))

        # Zero (or a negative number of) active tasks/lifespan should result in a value error
        for n in [-3, -1, 0, 3.14]:
            with self.assertRaises(ValueError):
                with WorkerPool(n_jobs=4) as pool:
                    pool.map(square, self.test_data, max_tasks_active=n)

            with self.assertRaises(ValueError):
                with WorkerPool(n_jobs=4) as pool:
                    pool.map_unordered(square, self.test_data, max_tasks_active=n)

            with self.assertRaises(ValueError):
                with WorkerPool(n_jobs=4) as pool:
                    for _ in pool.imap(square, self.test_data, max_tasks_active=n):
                        pass

            with self.assertRaises(ValueError):
                with WorkerPool(n_jobs=4) as pool:
                    for _ in pool.imap_unordered(square, self.test_data, max_tasks_active=n):
                        pass

            with self.assertRaises(ValueError):
                with WorkerPool(n_jobs=4) as pool:
                    pool.map(square, self.test_data, worker_lifespan=n)

            with self.assertRaises(ValueError):
                with WorkerPool(n_jobs=4) as pool:
                    pool.map_unordered(square, self.test_data, worker_lifespan=n)

            with self.assertRaises(ValueError):
                with WorkerPool(n_jobs=4) as pool:
                    for _ in pool.imap(square, self.test_data, worker_lifespan=n):
                        pass

            with self.assertRaises(ValueError):
                with WorkerPool(n_jobs=4) as pool:
                    for _ in pool.imap_unordered(square, self.test_data, worker_lifespan=n):
                        pass

    def test_worker_id_shared_objects(self):
        """
        Tests passing the worker ID and shared objects
        """
        for n_jobs, pass_worker_id, shared_objects in product([1, 2, 4], [False, True],
                                                              [None, (37, 42), ({'1', '2', '3'})]):
            # Function with worker ID and shared objects
            def f1(_wid, _sobjects, _args):
                self.assertTrue(isinstance(_wid, int))
                self.assertGreaterEqual(_wid, 0)
                self.assertLessEqual(_wid, n_jobs)
                self.assertEqual(_sobjects, _args)

            # Function with worker ID
            def f2(_wid, _):
                self.assertTrue(isinstance(_wid, int))
                self.assertGreaterEqual(_wid, 0)
                self.assertLessEqual(_wid, n_jobs)

            # Function with shared objects
            def f3(_sobjects, _args):
                self.assertEqual(_sobjects, _args)

            # Function without worker ID and shared objects
            def f4(_):
                pass

            with WorkerPool(n_jobs=n_jobs) as pool:
                # Configure pool
                pool.pass_on_worker_id(pass_worker_id)
                pool.set_shared_objects(shared_objects)

                # Tests should fail when number of arguments in function is incorrect, worker ID is not within range,
                # or when the shared objects are not equal to the given arguments
                f = f1 if pass_worker_id and shared_objects else (f2 if pass_worker_id else (f3 if shared_objects else
                                                                                             f4))
                pool.map(f, ((shared_objects,) for _ in range(10)), iterable_len=10)

    def test_daemon(self):
        """
        Tests nested WorkerPools
        """
        with WorkerPool(n_jobs=4, daemon=False) as pool:
            # Obtain results using nested WorkerPools
            results = pool.map(square_daemon, ((X,) for X in repeat(self.test_data, 4)), chunk_size=1)

            # Each of the results should match
            for results_list in results:
                self.assertTrue(isinstance(results_list, list))
                self.assertEqual(self.test_desired_output, results_list)

        # Daemon processes are not allowed to spawn children
        with WorkerPool(n_jobs=4, daemon=True) as pool:
            # Obtain results using nested WorkerPools
            results = pool.map(square_daemon_raises, ((X,) for X in repeat(self.test_data, 4)), chunk_size=1)

            # Results should be all True
            for result in results:
                self.assertTrue(result)

    def test_cpu_pinning(self):
        """
        Tests CPU pinning
        """
        for n_jobs, cpu_ids in product([None, 1, 2, 4], [None, [0], [0, 1], [0, 1, 2, 3], [[0, 3]], [[0, 1], [0, 1]]]):
            # Things should work fine when cpu_ids is None or number of cpu_ids given is one or equals the number of
            # jobs,
            if cpu_ids is None or len(cpu_ids) == 1 or len(cpu_ids) == (n_jobs or cpu_count()):
                with WorkerPool(n_jobs=n_jobs, cpu_ids=cpu_ids) as pool:
                    results_list = pool.map(square, self.test_data)
                    self.assertTrue(isinstance(results_list, list))
                    self.assertEqual(self.test_desired_output, results_list)
            # Should raise
            else:
                with self.assertRaises(ValueError):
                    WorkerPool(n_jobs=n_jobs, cpu_ids=cpu_ids)

        # Should raise when CPU IDs are out of scope
        with self.assertRaises(ValueError):
            WorkerPool(n_jobs=1, cpu_ids=[-1])
        with self.assertRaises(ValueError):
            WorkerPool(n_jobs=1, cpu_ids=[cpu_count()])
