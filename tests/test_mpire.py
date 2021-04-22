import types
import unittest
from itertools import product, repeat
from multiprocessing import Barrier, Value
from unittest.mock import patch

import numpy as np

from mpire import cpu_count, WorkerPool


def square(idx, x):
    return idx, x * x


def square_numpy(x):
    return x * x


class MapTest(unittest.TestCase):

    def setUp(self):
        # Create some test data. Note that the regular map reads the inputs as a list of single tuples (one argument),
        # whereas parallel.map sees it as a list of argument lists. Therefore we give the regular map a lambda function
        # which mimics the parallel.map behavior.
        self.test_data = list(enumerate([1, 2, 3, 5, 6, 9, 37, 42, 1337, 0, 3, 5, 0]))
        self.test_desired_output = list(map(lambda _args: square(*_args), self.test_data))
        self.test_data_len = len(self.test_data)

        # Numpy test data
        self.test_data_numpy = np.random.rand(100, 2)
        self.test_desired_output_numpy = square_numpy(self.test_data_numpy)
        self.test_data_len_numpy = len(self.test_data_numpy)

    def test_all_maps(self):
        """
        Tests the map related functions
        """

        def get_generator(iterable):
            yield from iterable

        # Test results for different number of jobs to run in parallel and the maximum number of active tasks in the
        # queue
        for n_jobs, n_tasks_max_active, worker_lifespan, chunk_size, n_splits in \
                product([1, 2, None], [None, 2], [None, 2], [None, 3], [None, 3]):

            with WorkerPool(n_jobs=n_jobs) as pool:

                for map_func, sort, result_type in ((pool.map, False, list), (pool.map_unordered, True, list),
                                                    (pool.imap, False, types.GeneratorType),
                                                    (pool.imap_unordered, True, types.GeneratorType)):

                    with self.subTest(map_func=map_func, input='list', n_jobs=n_jobs,
                                      n_tasks_max_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                      chunk_size=chunk_size, n_splits=n_splits):

                        # Test if parallel map results in the same as ordinary map function. Should work both for
                        # generators and iterators. Also check if an empty list works as desired.
                        results_list = map_func(square, self.test_data, max_tasks_active=n_tasks_max_active,
                                                worker_lifespan=worker_lifespan)
                        self.assertTrue(isinstance(results_list, result_type))
                        self.assertEqual(self.test_desired_output,
                                         sorted(results_list, key=lambda tup: tup[0]) if sort else list(results_list))

                    with self.subTest(map_func=map_func, input='generator', n_jobs=n_jobs,
                                      n_tasks_max_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                      chunk_size=chunk_size, n_splits=n_splits):

                        results_list = map_func(square, get_generator(self.test_data), iterable_len=self.test_data_len,
                                                max_tasks_active=n_tasks_max_active, worker_lifespan=worker_lifespan)
                        self.assertTrue(isinstance(results_list, result_type))
                        self.assertEqual(self.test_desired_output,
                                         sorted(results_list, key=lambda tup: tup[0]) if sort else list(results_list))

                    with self.subTest(map_func=map_func, input='empty list', n_jobs=n_jobs,
                                      n_tasks_max_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                      chunk_size=chunk_size, n_splits=n_splits):

                        results_list = map_func(square, [], max_tasks_active=n_tasks_max_active,
                                                worker_lifespan=worker_lifespan)
                        self.assertTrue(isinstance(results_list, result_type))
                        self.assertEqual([], list(results_list))

    def test_numpy_input(self):
        """
        Test map with numpy input
        """
        for n_jobs, n_tasks_max_active, worker_lifespan, chunk_size, n_splits in \
                product([1, 2, None], [None, 2], [None, 2], [None, 3], [None, 3]):

            with WorkerPool(n_jobs=n_jobs) as pool:

                # Test numpy input. map should concatenate chunks of numpy output to a single output array if we
                # instruct it to
                with self.subTest(concatenate_numpy_output=True, map_function='map', n_jobs=n_jobs,
                                  n_tasks_max_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                  chunk_size=chunk_size, n_splits=n_splits):
                    results = pool.map(square_numpy, self.test_data_numpy, max_tasks_active=n_tasks_max_active,
                                       worker_lifespan=worker_lifespan, concatenate_numpy_output=True)
                    self.assertTrue(isinstance(results, np.ndarray))
                    np.testing.assert_array_equal(results, self.test_desired_output_numpy)

                # If we disable it we should get back chunks of the original array
                with self.subTest(concatenate_numpy_output=False, map_function='map', n_jobs=n_jobs,
                                  n_tasks_max_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                  chunk_size=chunk_size, n_splits=n_splits):
                    results = pool.map(square_numpy, self.test_data_numpy, max_tasks_active=n_tasks_max_active,
                                       worker_lifespan=worker_lifespan, concatenate_numpy_output=False)
                    self.assertTrue(isinstance(results, list))
                    np.testing.assert_array_equal(np.concatenate(results), self.test_desired_output_numpy)

                # Numpy concatenation doesn't exist for the other functions
                with self.subTest(map_function='imap', n_jobs=n_jobs, n_tasks_max_active=n_tasks_max_active,
                                  worker_lifespan=worker_lifespan, chunk_size=chunk_size, n_splits=n_splits):
                    results = pool.imap(square_numpy, self.test_data_numpy, max_tasks_active=n_tasks_max_active,
                                        worker_lifespan=worker_lifespan)
                    self.assertTrue(isinstance(results, types.GeneratorType))
                    np.testing.assert_array_equal(np.concatenate(list(results)), self.test_desired_output_numpy)

                # map_unordered and imap_unordered cannot be checked for correctness as we don't know the order of the
                # returned results, except when n_jobs=1. In the other cases we could, however, check if all the values
                # (numpy rows) that are returned are present (albeit being in a different order)
                for map_func, result_type in ((pool.map_unordered, list), (pool.imap_unordered, types.GeneratorType)):

                    with self.subTest(map_function=map_func, n_jobs=n_jobs, n_tasks_max_active=n_tasks_max_active,
                                      worker_lifespan=worker_lifespan, chunk_size=chunk_size, n_splits=n_splits):

                        results = map_func(square_numpy, self.test_data_numpy, max_tasks_active=n_tasks_max_active,
                                           worker_lifespan=worker_lifespan)
                        self.assertTrue(isinstance(results, result_type))
                        concattenated_results = np.concatenate(list(results))
                        if n_jobs == 1:
                            np.testing.assert_array_equal(concattenated_results, self.test_desired_output_numpy)
                        else:
                            # We sort the expected and actual results using lexsort, which sorts using a sequence of
                            # keys. We transpose the array to sort on columns instead of rows.
                            np.testing.assert_array_equal(
                                concattenated_results[np.lexsort(concattenated_results.T)],
                                self.test_desired_output_numpy[np.lexsort(self.test_desired_output_numpy.T)]
                            )

    def test_dictionary_input(self):
        """
        Test map with dictionary input
        """
        def subtract(x, y):
            return x - y

        with WorkerPool(n_jobs=1) as pool:

            # Should work
            with self.subTest('correct input'):
                results_list = pool.map(subtract, [{'x': 5, 'y': 2}, {'y': 5, 'x': 2}])
                self.assertEqual(results_list, [3, -3])

            # Should throw
            with self.subTest("missing 'y', unknown parameter 'z'"), self.assertRaises(TypeError):
                pool.map(subtract, [{'x': 5, 'z': 2}])

            # Should throw
            with self.subTest("unknown parameter 'z'"), self.assertRaises(TypeError):
                pool.map(subtract, [{'x': 5, 'y': 2, 'z': 2}])

    def test_faulty_parameters(self):
        """
        Should raise when wrong parameter values are used
        """
        with WorkerPool(n_jobs=4) as pool:

            # Zero (or a negative number of) active tasks/lifespan should result in a value error
            for n, map_function in product([-3, -1, 0, 3.14],
                                           [pool.map, pool.map_unordered, pool.imap, pool.imap_unordered]):
                # max_tasks_active
                with self.subTest(max_tasks_active=n, map_function=map_function), \
                     self.assertRaises(ValueError if isinstance(n, int) else TypeError):
                    list(map_function(square, self.test_data, max_tasks_active=n))

                # worker_lifespan
                with self.subTest(worker_lifespan=n, map_function=map_function), \
                     self.assertRaises(ValueError if isinstance(n, int) else TypeError):
                    list(map_function(square, self.test_data, worker_lifespan=n))

            # chunk_size should be an integer or None
            with self.subTest(chunk_size='3'), self.assertRaises(TypeError):
                for _ in pool.imap(square, self.test_data, chunk_size='3'):
                    pass

            # chunk_size should be a positive integer
            with self.subTest(chunk_size=-5), self.assertRaises(ValueError):
                for _ in pool.imap(square, self.test_data, chunk_size=-5):
                    pass

            # n_splits should be an integer or None
            with self.subTest(n_splits='3'), self.assertRaises(TypeError):
                for _ in pool.imap(square, self.test_data, n_splits='3'):
                    pass

            # n_splits should be a positive integer
            with self.subTest(n_splits=-5), self.assertRaises(ValueError):
                for _ in pool.imap(square, self.test_data, n_splits=-5):
                    pass


class WorkerIDTest(unittest.TestCase):

    def test_by_config_function(self):
        """
        Test setting passing on the worker ID using the pass_on_worker_id function
        """
        for n_jobs, pass_worker_id in product([1, 2, 4], [True, False]):

            with self.subTest(n_jobs=n_jobs, pass_worker_id=pass_worker_id, config_type='function'), \
                 WorkerPool(n_jobs=n_jobs) as pool:

                pool.pass_on_worker_id(pass_worker_id)

                # Tests should fail when number of arguments in function is incorrect, worker ID is not within range,
                # or when the shared objects are not equal to the given arguments
                f = self._f1 if pass_worker_id else self._f2
                pool.map(f, ((n_jobs,) for _ in range(10)), iterable_len=10)

    def test_by_constructor(self):
        """
        Test setting passing on the worker ID in the constructor
        """
        for n_jobs, pass_worker_id in product([1, 2, 4], [True, False]):

            with self.subTest(n_jobs=n_jobs, pass_worker_id=pass_worker_id, config_type='constructor'), \
                 WorkerPool(n_jobs=n_jobs, pass_worker_id=pass_worker_id) as pool:

                # Tests should fail when number of arguments in function is incorrect, worker ID is not within range,
                # or when the shared objects are not equal to the given arguments
                f = self._f1 if pass_worker_id else self._f2
                pool.map(f, ((n_jobs,) for _ in range(10)), iterable_len=10)

    def _f1(self, _wid, _n_jobs):
        """
        Function with worker ID
        """
        self.assertIsInstance(_wid, int)
        self.assertGreaterEqual(_wid, 0)
        self.assertLessEqual(_wid, _n_jobs)

    def _f2(self, _n_jobs):
        """
        Function without worker ID (simply tests if WorkerPool correctly handles pass_worker_id=False)
        """
        pass


class SharedObjectsTest(unittest.TestCase):

    def test_by_config_function(self):
        """
        Tests passing shared objects using the set_shared_objects function
        """
        for n_jobs, shared_objects in product([1, 2, 4], [None, (37, 42), ({'1', '2', '3'})]):

            with self.subTest(n_jobs=n_jobs, shared_objects=shared_objects, config_type='function'), \
                 WorkerPool(n_jobs=n_jobs) as pool:

                # Configure pool
                pool.set_shared_objects(shared_objects)

                # Tests should fail when number of arguments in function is incorrect, worker ID is not within range,
                # or when the shared objects are not equal to the given arguments
                f = self._f1 if shared_objects else self._f2
                pool.map(f, ((shared_objects, n_jobs) for _ in range(10)), iterable_len=10)

    def test_by_constructor(self):
        """
        Tests passing shared objects in the constructor
        """
        for n_jobs, shared_objects in product([1, 2, 4], [None, (37, 42), ({'1', '2', '3'})]):

            # Pass on arguments using the constructor instead
            with self.subTest(n_jobs=n_jobs, shared_objects=shared_objects, config_type='constructor'), \
                 WorkerPool(n_jobs=n_jobs, shared_objects=shared_objects) as pool:

                # Tests should fail when number of arguments in function is incorrect, worker ID is not within range,
                # or when the shared objects are not equal to the given arguments
                f = self._f1 if shared_objects else self._f2
                pool.map(f, ((shared_objects, n_jobs) for _ in range(10)), iterable_len=10)

    def _f1(self, _sobjects, _args, _n_jobs):
        """
        Function with shared objects
        """
        self.assertEqual(_sobjects, _args, _n_jobs)

    def _f2(self, _args, _n_jobs):
        """
        Function without shared objects (simply tests if WorkerPool correctly handles shared_objects=None)
        """
        pass


class WorkerStateTest(unittest.TestCase):

    def test_by_config_function(self):
        """
        Tests setting worker state using the set_use_worker_state function
        """
        for n_jobs, use_worker_state, n_tasks in product([1, 2, 4], [False, True], [0, 1, 3, 150]):

            with self.subTest(n_jobs=n_jobs, use_worker_state=use_worker_state, n_tasks=n_tasks),\
                 WorkerPool(n_jobs=n_jobs, pass_worker_id=True) as pool:

                pool.set_use_worker_state(use_worker_state)

                # When use_worker_state is set, the final (worker_id, n_args) of each worker should add up to the
                # number of given tasks
                f = self._f1 if use_worker_state else self._f2
                results = pool.map(f, range(n_tasks), chunk_size=2)
                if use_worker_state:
                    n_processed_per_worker = [0] * n_jobs
                    for wid, n_processed in results:
                        n_processed_per_worker[wid] = n_processed
                    self.assertEqual(sum(n_processed_per_worker), n_tasks)

    def test_by_constructor(self):
        """
        Tests setting worker state in the constructor
        """
        for n_jobs, use_worker_state, n_tasks in product([1, 2, 4], [False, True], [0, 1, 3, 150]):

            with self.subTest(n_jobs=n_jobs, use_worker_state=use_worker_state, n_tasks=n_tasks), \
                 WorkerPool(n_jobs=n_jobs, pass_worker_id=True, use_worker_state=use_worker_state) as pool:

                # When use_worker_state is set, the final (worker_id, n_args) of each worker should add up to the
                # number of given tasks
                f = self._f1 if use_worker_state else self._f2
                results = pool.map(f, range(n_tasks), chunk_size=2)
                if use_worker_state:
                    n_processed_per_worker = [0] * n_jobs
                    for wid, n_processed in results:
                        n_processed_per_worker[wid] = n_processed
                    self.assertEqual(sum(n_processed_per_worker), n_tasks)

    def _f1(self, _wid, _wstate, _arg):
        """
        Function with worker ID and worker state
        """
        self.assertTrue(isinstance(_wstate, dict))

        # Worker id should always be the same
        _wstate.setdefault('worker_id', set()).add(_wid)
        self.assertEqual(_wstate['worker_id'], {_wid})

        # Should contain previous args
        _wstate.setdefault('args', []).append(_arg)
        return _wid, len(_wstate['args'])

    def _f2(self, _wid, _):
        """
        Function with worker ID (simply tests if WorkerPool correctly handles use_worker_state=False)
        """
        pass


class DaemonTest(unittest.TestCase):

    def setUp(self):
        # Create some test data. Note that the regular map reads the inputs as a list of single tuples (one argument),
        # whereas parallel.map sees it as a list of argument lists. Therefore we give the regular map a lambda function
        # which mimics the parallel.map behavior.
        self.test_data = list(enumerate([1, 2, 3, 5, 6, 9, 37, 42, 1337, 0, 3, 5, 0]))
        self.test_desired_output = list(map(lambda _args: square(*_args), self.test_data))

    def test_non_deamon_nested_workerpool(self):
        """
        Tests nested WorkerPools when daemon==False, which should work
        """
        with WorkerPool(n_jobs=4, daemon=False) as pool:
            # Obtain results using nested WorkerPools
            results = pool.map(self._square_daemon, ((X,) for X in repeat(self.test_data, 4)), chunk_size=1)

            # Each of the results should match
            for results_list in results:
                self.assertTrue(isinstance(results_list, list))
                self.assertEqual(self.test_desired_output, results_list)

    def test_deamon_nested_workerpool(self):
        """
        Tests nested WorkerPools when daemon==True, which should not work
        """
        with self.assertRaises(AssertionError), WorkerPool(n_jobs=4, daemon=True) as pool:
            pool.map(self._square_daemon, ((X,) for X in repeat(self.test_data, 4)), chunk_size=1)

    @staticmethod
    def _square_daemon(X):
        with WorkerPool(n_jobs=4) as pool:
            return pool.map(square, X, chunk_size=1)


class CPUPinningTest(unittest.TestCase):

    def setUp(self):
        # Create some test data. Note that the regular map reads the inputs as a list of single tuples (one argument),
        # whereas parallel.map sees it as a list of argument lists. Therefore we give the regular map a lambda function
        # which mimics the parallel.map behavior.
        self.test_data = list(enumerate([1, 2, 3, 5, 6, 9, 37, 42, 1337, 0, 3, 5, 0]))
        self.test_desired_output = list(map(lambda _args: square(*_args), self.test_data))

    def test_valid_input(self):
        """
        Test that when parameters are valid, nothing breaks. We don't actually check if CPU pinning is happening
        """
        for n_jobs, cpu_ids, expected_mask in [(None, [0], [[0]] * cpu_count()),
                                               (None, [[0, 3]], [[0, 3]] * cpu_count()),
                                               (1, [0], [[0]]),
                                               (1, [[0, 3]], [[0, 3]]),
                                               (2, [0], [[0], [0]]),
                                               (2, [0, 1], [[0], [1]]),
                                               (2, [[0, 3]], [[0, 3], [0, 3]]),
                                               (2, [[0, 1], [0, 1]], [[0, 1], [0, 1]]),
                                               (4, [0], [[0], [0], [0], [0]]),
                                               (4, [0, 1, 2, 3], [[0], [1], [2], [3]]),
                                               (4, [[0, 3]], [[0, 3], [0, 3], [0, 3], [0, 3]])]:
            # The test has been designed for a system with at least 4 cores. We'll skip those test cases where the CPU
            # IDs exceed the number of CPUs.
            if cpu_ids is not None and np.array(cpu_ids).max() >= cpu_count():
                continue

            else:
                with self.subTest(n_jobs=n_jobs, cpu_ids=cpu_ids), patch('os.sched_setaffinity') as p, \
                        WorkerPool(n_jobs=n_jobs, cpu_ids=cpu_ids) as pool:

                    # Verify results
                    results_list = pool.map(square, self.test_data)
                    self.assertTrue(isinstance(results_list, list))
                    self.assertEqual(self.test_desired_output, results_list)

                    # Verify that when CPU pinning is used, it is called as many times as there are jobs and is
                    # called for each worker process ID
                    if cpu_ids is None:
                        self.assertEqual(p.call_args_list, [])
                    else:
                        self.assertEqual(p.call_count, pool.n_jobs)
                        mask = [call[0][1] for call in p.call_args_list]
                        self.assertListEqual(mask, expected_mask)

    def test_invalid_input(self):
        """
        Test that when parameters are invalid, an error is raised
        """
        for n_jobs, cpu_ids in product([None, 1, 2, 4], [[0, 1], [0, 1, 2, 3], [[0, 1], [0, 1]]]):
            if len(cpu_ids) != (n_jobs or cpu_count()):
                with self.subTest(n_jobs=n_jobs, cpu_ids=cpu_ids), self.assertRaises(ValueError):
                    WorkerPool(n_jobs=n_jobs, cpu_ids=cpu_ids)

        # Should raise when CPU IDs are out of scope
        with self.assertRaises(ValueError):
            WorkerPool(n_jobs=1, cpu_ids=[-1])
        with self.assertRaises(ValueError):
            WorkerPool(n_jobs=1, cpu_ids=[cpu_count()])


class ProgressBarTest(unittest.TestCase):

    """
    Print statements in these tests are intentional as it will print multiple progress bars
    """

    def setUp(self):
        # Create some test data. Note that the regular map reads the inputs as a list of single tuples (one argument),
        # whereas parallel.map sees it as a list of argument lists. Therefore we give the regular map a lambda function
        # which mimics the parallel.map behavior.
        self.test_data = list(enumerate([1, 2, 3, 5, 6, 9, 37, 42, 1337, 0, 3, 5, 0]))
        self.test_desired_output = list(map(lambda _args: square(*_args), self.test_data))

        # Numpy test data
        self.test_data_numpy = np.random.rand(100, 2)
        self.test_desired_output_numpy = square_numpy(self.test_data_numpy)
        self.test_data_len_numpy = len(self.test_data_numpy)

    def test_valid_progress_bars_regular_input(self):
        """
        Valid progress bars are either False/True
        """
        print()
        for n_jobs, progress_bar in product([None, 1, 2], [True, False]):

            with self.subTest(n_jobs=n_jobs), WorkerPool(n_jobs=n_jobs) as pool:
                results_list = pool.map(square, self.test_data, progress_bar=progress_bar)
                self.assertTrue(isinstance(results_list, list))
                self.assertEqual(self.test_desired_output, results_list)

    def test_valid_progress_bars_numpy_input(self):
        """
        Test with numpy, as that will change the number of tasks
        """
        print()
        for n_jobs, progress_bar in product([None, 1, 2], [True, False]):

            # Should work just fine
            with self.subTest(n_jobs=n_jobs, progress_bar=progress_bar), WorkerPool(n_jobs=n_jobs) as pool:
                results = pool.map(square_numpy, self.test_data_numpy, progress_bar=progress_bar)
                self.assertTrue(isinstance(results, np.ndarray))
                np.testing.assert_array_equal(results, self.test_desired_output_numpy)

    def test_no_input_data(self):
        """
        Test with empty iterable (this failed before)
        """
        print()
        with WorkerPool() as pool:
            self.assertListEqual(pool.map(square, [], progress_bar=True), [])

    def test_invalid_progress_bar_position(self):
        """
        Test different values of progress_bar_position, which should be positive integer >= 0
        """
        for progress_bar_position, error in [(-1, ValueError), ('numero uno', TypeError)]:
            with self.subTest(input='regular input', progress_bar_position=progress_bar_position), \
                    self.assertRaises(error), WorkerPool(n_jobs=1) as pool:
                pool.map(square, self.test_data, progress_bar=True, progress_bar_position=progress_bar_position)

            with self.subTest(input='numpy input', progress_bar_position=progress_bar_position), \
                    self.assertRaises(error), WorkerPool(n_jobs=1) as pool:
                pool.map(square_numpy, self.test_data_numpy, progress_bar=True,
                         progress_bar_position=progress_bar_position)


class StartMethodTest(unittest.TestCase):

    def setUp(self):
        # Create some test data. Note that the regular map reads the inputs as a list of single tuples (one argument),
        # whereas parallel.map sees it as a list of argument lists. Therefore we give the regular map a lambda function
        # which mimics the parallel.map behavior.
        self.test_data = list(enumerate([1, 2, 3, 5, 6, 9, 37, 42, 1337, 0, 3, 5, 0]))
        self.test_desired_output = list(map(lambda _args: square(*_args), self.test_data))

    def test_start_method(self):
        """
        Test different start methods. All should work just fine
        """
        for n_jobs, start_method in product([1, 3], ['fork', 'forkserver', 'spawn', 'threading']):
            with self.subTest(n_jobs=n_jobs, start_method=start_method), \
                 WorkerPool(n_jobs, start_method=start_method) as pool:
                self.assertListEqual(pool.map(square, self.test_data), self.test_desired_output)


class KeepAliveTest(unittest.TestCase):

    """
    In these tests we make use of a barrier. This barrier ensures that we increase the counter for each worker. If it
    wasn't there there's a chance that the first, say 3, workers already performed all the available tasks, while the
    4th worker was still spinning up. In that case the poison pill would be inserted before the fourth worker could even
    start a task and therefore couldn't increase the counter value.
    """

    def setUp(self):
        # Create some test data
        self.test_data = [1, 2, 3, 5, 6, 9, 37, 42, 1337, 0, 3, 5, 0]
        self.test_desired_output_f1 = [x * 2 for x in self.test_data]
        self.test_desired_output_f2 = [x * 3 for x in self.test_data]

    def test_dont_keep_alive(self):
        """
        When keep_alive is set to False it should restart workers between map calls. This means the counter is updated
        each time as well.
        """
        for n_jobs in [1, 2, 4]:
            barrier = Barrier(n_jobs)
            counter = Value('i', 0)
            shared = barrier, counter
            with self.subTest(n_jobs=n_jobs), \
                    WorkerPool(n_jobs=n_jobs, shared_objects=shared, use_worker_state=True, keep_alive=False) as pool:

                self.assertListEqual(pool.map(self._f1, self.test_data), self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs)
                barrier.reset()

                self.assertListEqual(pool.map(self._f1, self.test_data), self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs * 2)
                barrier.reset()

                self.assertListEqual(pool.map(self._f1, self.test_data), self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs * 3)
                barrier.reset()

                self.assertListEqual(pool.map(self._f1, self.test_data), self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs * 4)

    def test_keep_alive(self):
        """
        When keep_alive is set to True it should reuse existing workers between map calls. This means the counter is
        only updated the first time.
        """
        for n_jobs in [1, 2, 4]:
            barrier = Barrier(n_jobs)
            counter = Value('i', 0)
            shared = barrier, counter
            with self.subTest(n_jobs=n_jobs), \
                    WorkerPool(n_jobs=n_jobs, shared_objects=shared, use_worker_state=True, keep_alive=True) as pool:

                self.assertListEqual(pool.map(self._f1, self.test_data), self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs)
                barrier.reset()

                self.assertListEqual(list(pool.imap(self._f1, self.test_data)), self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs)
                barrier.reset()

                self.assertListEqual(pool.map(self._f1, self.test_data), self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs)

    def test_keep_alive_func_changes(self):
        """
        When keep_alive is set to True it should reuse existing workers between map calls, but only when the called
        function is kept constant
        """
        for n_jobs in [1, 2, 4]:
            barrier = Barrier(n_jobs)
            counter = Value('i', 0)
            shared = barrier, counter
            with self.subTest(n_jobs=n_jobs), \
                    WorkerPool(n_jobs=n_jobs, shared_objects=shared, use_worker_state=True, keep_alive=True) as pool:

                self.assertListEqual(pool.map(self._f1, self.test_data), self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs)
                barrier.reset()

                self.assertListEqual(list(pool.imap(self._f2, self.test_data)), self.test_desired_output_f2)
                self.assertEqual(counter.value, n_jobs * 2)
                barrier.reset()

                self.assertListEqual(pool.map(self._f2, self.test_data), self.test_desired_output_f2)
                self.assertEqual(counter.value, n_jobs * 2)
                barrier.reset()

                self.assertListEqual(pool.map(self._f1, self.test_data), self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs * 3)

    def test_keep_alive_worker_lifespan_changes(self):
        """
        When keep_alive is set to True it should reuse existing workers between map calls, but only when the called
        function is kept constant
        """
        for n_jobs in [1, 2, 4]:
            barrier = Barrier(n_jobs)
            counter = Value('i', 0)
            shared = barrier, counter
            with self.subTest(n_jobs=n_jobs), \
                    WorkerPool(n_jobs=n_jobs, shared_objects=shared, use_worker_state=True, keep_alive=True) as pool:

                self.assertListEqual(pool.map(self._f1, self.test_data, worker_lifespan=100),
                                     self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs)
                barrier.reset()

                self.assertListEqual(list(pool.imap(self._f1, self.test_data, worker_lifespan=100)),
                                     self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs)
                barrier.reset()

                self.assertListEqual(pool.map(self._f1, self.test_data, worker_lifespan=200),
                                     self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs * 2)
                barrier.reset()

                self.assertListEqual(pool.map(self._f1, self.test_data, worker_lifespan=200),
                                     self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs * 2)
                barrier.reset()

                self.assertListEqual(pool.map(self._f1, self.test_data, worker_lifespan=100),
                                     self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs * 3)

    @staticmethod
    def _f1(shared, worker_state, x):
        """
        Function that waits for all workers to spin up and increases the counter by one only once per worker,
        returns x * 2
        """
        barrier, counter = shared
        if 'already_counted' not in worker_state:
            counter.value += 1
            worker_state['already_counted'] = True
            barrier.wait()
        return x * 2

    @staticmethod
    def _f2(shared, worker_state, x):
        """
        Function that waits for all workers to spin up and increases the counter by one only once per worker,
        returns x * 3
        """
        barrier, counter = shared
        if 'already_counted' not in worker_state:
            counter.value += 1
            worker_state['already_counted'] = True
            barrier.wait()
        return x * 3


class ExceptionTest(unittest.TestCase):

    def setUp(self):
        # Create some test data. Note that the regular map reads the inputs as a list of single tuples (one argument),
        # whereas parallel.map sees it as a list of argument lists. Therefore we give the regular map a lambda function
        # which mimics the parallel.map behavior.
        self.test_data = list(enumerate([1, 2, 3, 5, 6, 9, 37, 42, 1337, 0, 3, 5, 0]))
        self.test_desired_output = list(map(lambda _args: square(*_args), self.test_data))
        self.test_data_len = len(self.test_data)

    def test_exceptions(self):
        """
        Tests if MPIRE can handle exceptions well
        """
        # This print statement is intentional as it will print multiple progress bars
        print()
        for n_jobs, n_tasks_max_active, worker_lifespan, progress_bar in product([1, 20], [None, 1], [None, 1],
                                                                                 [False, True]):
            with WorkerPool(n_jobs=n_jobs) as pool:

                # Should work for map like functions
                with self.subTest(n_jobs=n_jobs, n_tasks_max_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                  progress_bar=progress_bar, function='square_raises', map='map'), \
                     self.assertRaises(ValueError):
                    pool.map(self._square_raises, self.test_data, max_tasks_active=n_tasks_max_active,
                             worker_lifespan=worker_lifespan, progress_bar=progress_bar)

                # Should work for imap like functions
                with self.subTest(n_jobs=n_jobs, n_tasks_max_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                  progress_bar=progress_bar, function='square_raises', map='imap'), \
                     self.assertRaises(ValueError):
                    list(pool.imap_unordered(self._square_raises, self.test_data, max_tasks_active=n_tasks_max_active,
                                             worker_lifespan=worker_lifespan, progress_bar=progress_bar))

                # Should work for map like functions
                with self.subTest(n_jobs=n_jobs, n_tasks_max_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                  progress_bar=progress_bar, function='square_raises_on_idx', map='map'), \
                     self.assertRaises(ValueError):
                    pool.map(self._square_raises_on_idx, self.test_data, max_tasks_active=n_tasks_max_active,
                             worker_lifespan=worker_lifespan, progress_bar=progress_bar)

                # Should work for imap like functions
                with self.subTest(n_jobs=n_jobs, n_tasks_max_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                  progress_bar=progress_bar, function='square_raises_on_idx', map='imap'), \
                     self.assertRaises(ValueError):
                    list(pool.imap_unordered(self._square_raises_on_idx, self.test_data,
                                             max_tasks_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                             progress_bar=progress_bar))

    @staticmethod
    def _square_raises(_, x):
        raise ValueError(x)

    @staticmethod
    def _square_raises_on_idx(idx, x):
        if idx == 5:
            raise ValueError(x)
        else:
            return idx, x * x
