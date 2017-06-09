import unittest


def square(idx, x):
    return idx, x * x


def get_generator(iterable):
    yield from iterable


class ParallellTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Create some test data. Note that the regular map reads the inputs as a list of single tuples (one argument),
        # whereas parallel.map sees it as a list of argument lists. Therefore we give the regular map a lambda function
        # which mimics the parallel.map behavior.
        self.test_data = [(idx, x) for idx, x in enumerate([1, 2, 3, 5, 6, 9, 37, 42, 1337, 0, 3, 5, 0])]
        self.test_desired_output = list(map(lambda _args: square(*_args), self.test_data))
        self.test_data_len = len(self.test_data)

    def test_worker_pool_map(self):
        """
        Tests the map related function of the worker pool class
        """
        from mpire import WorkerPool
        from itertools import product
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
