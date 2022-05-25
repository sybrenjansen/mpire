import logging
import os
import time
import types
import unittest
import warnings
from itertools import product, repeat
from multiprocessing import Barrier, Value
from threading import current_thread, main_thread, Thread
from unittest.mock import patch

import numpy as np
from tqdm import tqdm

from mpire import cpu_count, WorkerPool
from mpire.context import FORK_AVAILABLE, RUNNING_WINDOWS
from mpire.tqdm_utils import TqdmLock

# Skip start methods that use fork if it's not available
if not FORK_AVAILABLE:
    TEST_START_METHODS = ['spawn', 'threading']
else:
    TEST_START_METHODS = ['fork', 'forkserver', 'spawn', 'threading']


def square(idx, x):
    return idx, x * x


def extremely_large_output(idx, _):
    return idx, os.urandom(1024 * 1024)


def square_numpy(x):
    return x * x


def subtract(x, y):
    return x - y


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

        # Test results for different parameter settings
        for n_jobs, n_tasks_max_active, worker_lifespan, chunk_size, n_splits in tqdm([
            (None, None, None, None, None),
            (1, None, None, None, None),
            (2, None, None, None, None),
            (2, 2, None, None, None),
            (2, None, 2, None, None),
            (2, None, None, 3, None),
            (2, None, None, None, 3),
            (2, None, None, 3, 3),
            (2, None, 1, 3, None)
        ]):
            with WorkerPool(n_jobs=n_jobs) as pool:

                for map_func, sort, result_type in ((pool.map, False, list), (pool.map_unordered, True, list),
                                                    (pool.imap, False, types.GeneratorType),
                                                    (pool.imap_unordered, True, types.GeneratorType)):

                    with self.subTest(map_func=map_func, input='list', n_jobs=n_jobs,
                                      n_tasks_max_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                      chunk_size=chunk_size, n_splits=n_splits):

                        # Test if parallel map results in the same as ordinary map function. Should work both for
                        # generators and iterators. Also check if an empty list and extremely large output (exceeding
                        # os.pipe limits) works as desired.
                        results_list = map_func(square, self.test_data, max_tasks_active=n_tasks_max_active,
                                                worker_lifespan=worker_lifespan)
                        self.assertIsInstance(results_list, result_type)
                        self.assertEqual(self.test_desired_output,
                                         sorted(results_list, key=lambda tup: tup[0]) if sort else list(results_list))

                    with self.subTest(map_func=map_func, input='generator', n_jobs=n_jobs,
                                      n_tasks_max_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                      chunk_size=chunk_size, n_splits=n_splits):

                        results_list = map_func(square, get_generator(self.test_data), iterable_len=self.test_data_len,
                                                max_tasks_active=n_tasks_max_active, worker_lifespan=worker_lifespan)
                        self.assertIsInstance(results_list, result_type)
                        self.assertEqual(self.test_desired_output,
                                         sorted(results_list, key=lambda tup: tup[0]) if sort else list(results_list))

                    with self.subTest(map_func=map_func, input='empty list', n_jobs=n_jobs,
                                      n_tasks_max_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                      chunk_size=chunk_size, n_splits=n_splits):

                        results_list = map_func(square, [], max_tasks_active=n_tasks_max_active,
                                                worker_lifespan=worker_lifespan)
                        self.assertIsInstance(results_list, result_type)
                        self.assertEqual([], list(results_list))

                    # When the os pipe capacity is exceeded, a worker restart based on worker lifespan would hang if we
                    # not fetch all the results from a worker. We only verify the amount of data returned here.
                    with self.subTest(map_func=map_func, output='data exceeding pipe limits', n_jobs=n_jobs,
                                      n_tasks_max_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                      chunk_size=chunk_size, n_splits=n_splits):
                        results_list = map_func(extremely_large_output, self.test_data,
                                                max_tasks_active=n_tasks_max_active, worker_lifespan=worker_lifespan)
                        self.assertIsInstance(results_list, result_type)
                        self.assertEqual(len(self.test_desired_output), len(list(results_list)))

    def test_numpy_input(self):
        """
        Test map with numpy input
        """
        print()
        for n_jobs, n_tasks_max_active, worker_lifespan, chunk_size, n_splits in tqdm([
            (None, None, None, None, None),
            (1, None, None, None, None),
            (2, None, None, None, None),
            (2, 2, None, None, None),
            (2, None, 2, None, None),
            (2, None, None, 3, None),
            (2, None, None, None, 3),
            (2, None, None, 3, 3),
            (2, None, 1, 3, None)
        ]):
            with WorkerPool(n_jobs=n_jobs) as pool:

                # Test numpy input. map should concatenate chunks of numpy output to a single output array if we
                # instruct it to
                with self.subTest(concatenate_numpy_output=True, map_function='map', n_jobs=n_jobs,
                                  n_tasks_max_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                  chunk_size=chunk_size, n_splits=n_splits):
                    results = pool.map(square_numpy, self.test_data_numpy, max_tasks_active=n_tasks_max_active,
                                       worker_lifespan=worker_lifespan, concatenate_numpy_output=True)
                    self.assertIsInstance(results, np.ndarray)
                    np.testing.assert_array_equal(results, self.test_desired_output_numpy)

                # If we disable it we should get back chunks of the original array
                with self.subTest(concatenate_numpy_output=False, map_function='map', n_jobs=n_jobs,
                                  n_tasks_max_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                  chunk_size=chunk_size, n_splits=n_splits):
                    results = pool.map(square_numpy, self.test_data_numpy, max_tasks_active=n_tasks_max_active,
                                       worker_lifespan=worker_lifespan, concatenate_numpy_output=False)
                    self.assertIsInstance(results, list)
                    np.testing.assert_array_equal(np.concatenate(results), self.test_desired_output_numpy)

                # Numpy concatenation doesn't exist for the other functions
                with self.subTest(map_function='imap', n_jobs=n_jobs, n_tasks_max_active=n_tasks_max_active,
                                  worker_lifespan=worker_lifespan, chunk_size=chunk_size, n_splits=n_splits):
                    results = pool.imap(square_numpy, self.test_data_numpy, max_tasks_active=n_tasks_max_active,
                                        worker_lifespan=worker_lifespan)
                    self.assertIsInstance(results, types.GeneratorType)
                    np.testing.assert_array_equal(np.concatenate(list(results)), self.test_desired_output_numpy)

                # map_unordered and imap_unordered cannot be checked for correctness as we don't know the order of the
                # returned results, except when n_jobs=1. In the other cases we could, however, check if all the values
                # (numpy rows) that are returned are present (albeit being in a different order)
                for map_func, result_type in ((pool.map_unordered, list), (pool.imap_unordered, types.GeneratorType)):

                    with self.subTest(map_function=map_func, n_jobs=n_jobs, n_tasks_max_active=n_tasks_max_active,
                                      worker_lifespan=worker_lifespan, chunk_size=chunk_size, n_splits=n_splits):

                        results = map_func(square_numpy, self.test_data_numpy, max_tasks_active=n_tasks_max_active,
                                           worker_lifespan=worker_lifespan)
                        self.assertIsInstance(results, result_type)
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

    def test_start_methods(self):
        """
        Test different start methods. All should work just fine
        """
        print()
        for start_method in tqdm(TEST_START_METHODS):
            with self.subTest(start_method=start_method, map='map'), WorkerPool(2, start_method=start_method) as pool:
                results_list = pool.map(square, self.test_data)
                self.assertIsInstance(results_list, list)
                self.assertEqual(self.test_desired_output, results_list)

            with self.subTest(start_method=start_method, map='map_unordered'), \
                    WorkerPool(2, start_method=start_method) as pool:
                results_list = pool.map_unordered(square, self.test_data)
                self.assertIsInstance(results_list, list)
                self.assertEqual(self.test_desired_output, sorted(results_list, key=lambda tup: tup[0]))

            with self.subTest(start_method=start_method, map='imap'), WorkerPool(2, start_method=start_method) as pool:
                results_list = pool.imap(square, self.test_data)
                self.assertIsInstance(results_list, types.GeneratorType)
                self.assertListEqual(list(results_list), self.test_desired_output)

            with self.subTest(start_method=start_method, map='imap_unordered'), \
                    WorkerPool(2, start_method=start_method) as pool:
                results_list = pool.imap_unordered(square, self.test_data)
                self.assertIsInstance(results_list, types.GeneratorType)
                self.assertEqual(self.test_desired_output, sorted(results_list, key=lambda tup: tup[0]))


class PoolInThreadTest(unittest.TestCase):

    def setUp(self):
        self.test_data = [1, 2, 3, 5, 6, 9, 37, 42, 1337, 0, 3, 5, 0]
        self.test_desired_output = [self._square(x) for x in self.test_data]

    def test_start_methods(self):
        """
        Test that a WorkerPool can be started inside a thread, which isn't the main thread. Test for different start
        methods. All should work just fine
        """
        for start_method in TEST_START_METHODS:
            with self.subTest(start_method=start_method):
                t = Thread(target=self._map_thread, args=(start_method,))
                t.start()
                t.join()

    def _map_thread(self, start_method):
        """
        This function is called from within a thread
        """
        self.assertNotEqual(current_thread(), main_thread())
        with WorkerPool(2, start_method=start_method) as pool:
            results_list = pool.map(self._square, self.test_data)
            self.assertIsInstance(results_list, list)
            self.assertListEqual(self.test_desired_output, results_list)

    @staticmethod
    def _square(x):
        return x * x


class WorkerIDTest(unittest.TestCase):

    def test_by_config_function(self):
        """
        Test setting passing on the worker ID using the pass_on_worker_id function
        """
        for n_jobs, pass_worker_id in product([1, 3], [True, False]):

            with self.subTest(n_jobs=n_jobs, pass_worker_id=pass_worker_id, config_type='function'), \
                 WorkerPool(n_jobs=n_jobs) as pool:

                pool.pass_on_worker_id(pass_worker_id)

                # Tests should fail when number of arguments in function is incorrect, worker ID is not within range,
                # or when the shared objects are not equal to the given arguments
                f = self._f1 if pass_worker_id else self._f2
                self.assertListEqual(pool.map(f, ((n_jobs,) for _ in range(10)), iterable_len=10), [True] * 10)

    def test_by_constructor(self):
        """
        Test setting passing on the worker ID in the constructor
        """
        for n_jobs, pass_worker_id in product([1, 3], [True, False]):

            with self.subTest(n_jobs=n_jobs, pass_worker_id=pass_worker_id, config_type='constructor'), \
                 WorkerPool(n_jobs=n_jobs, pass_worker_id=pass_worker_id) as pool:

                # Tests should fail when number of arguments in function is incorrect, worker ID is not within range,
                # or when the shared objects are not equal to the given arguments
                f = self._f1 if pass_worker_id else self._f2
                self.assertListEqual(pool.map(f, ((n_jobs,) for _ in range(10)), iterable_len=10), [True] * 10)

    def test_start_methods(self):
        """
        Test for different start methods
        """
        for start_method in TEST_START_METHODS:
            with self.subTest(start_method=start_method), \
                    WorkerPool(n_jobs=2, pass_worker_id=True, start_method=start_method) as pool:
                self.assertListEqual(pool.map(self._f1, ((2,) for _ in range(10)), iterable_len=10), [True] * 10)

    @staticmethod
    def _f1(_wid, _n_jobs):
        """
        Function with worker ID
        """
        tests_succeed = True
        tests_succeed &= isinstance(_wid, int)
        tests_succeed &= _wid >= 0
        tests_succeed &= _wid <= _n_jobs
        return tests_succeed

    @staticmethod
    def _f2(_n_jobs):
        """
        Function without worker ID (simply tests if WorkerPool correctly handles pass_worker_id=False)
        """
        return True


class SharedObjectsTest(unittest.TestCase):

    def test_by_config_function(self):
        """
        Tests passing shared objects using the set_shared_objects function
        """
        for n_jobs, shared_objects in product([1, 3], [None, (37, 42), ({'1', '2', '3'})]):

            with self.subTest(n_jobs=n_jobs, shared_objects=shared_objects, config_type='function'), \
                 WorkerPool(n_jobs=n_jobs) as pool:

                # Configure pool
                pool.set_shared_objects(shared_objects)

                # Tests should fail when number of arguments in function is incorrect, worker ID is not within range,
                # or when the shared objects are not equal to the given arguments
                f = self._f1 if shared_objects else self._f2
                self.assertListEqual(pool.map(f, ((shared_objects,) for _ in range(10)), iterable_len=10), [True] * 10)

    def test_by_constructor(self):
        """
        Tests passing shared objects in the constructor
        """
        for n_jobs, shared_objects in product([1, 3], [None, (37, 42), ({'1', '2', '3'})]):

            # Pass on arguments using the constructor instead
            with self.subTest(n_jobs=n_jobs, shared_objects=shared_objects, config_type='constructor'), \
                 WorkerPool(n_jobs=n_jobs, shared_objects=shared_objects) as pool:

                # Tests should fail when number of arguments in function is incorrect, worker ID is not within range,
                # or when the shared objects are not equal to the given arguments
                f = self._f1 if shared_objects else self._f2
                self.assertListEqual(pool.map(f, ((shared_objects,) for _ in range(10)), iterable_len=10), [True] * 10)

    def test_start_methods(self):
        """
        Tests for different start methods
        """
        for start_method in TEST_START_METHODS:
            with self.subTest(start_method=start_method), \
                    WorkerPool(n_jobs=2, shared_objects=({'1', '2', '3'}), start_method=start_method) as pool:
                self.assertListEqual(pool.map(self._f1, (({'1', '2', '3'},) for _ in range(10)), iterable_len=10),
                                     [True] * 10)

    @staticmethod
    def _f1(_sobjects, _args):
        """
        Function with shared objects
        """
        return _sobjects == _args

    @staticmethod
    def _f2(_args):
        """
        Function without shared objects (simply tests if WorkerPool correctly handles shared_objects=None)
        """
        return True


class WorkerStateTest(unittest.TestCase):

    def test_by_config_function(self):
        """
        Tests setting worker state using the set_use_worker_state function
        """
        for n_jobs, use_worker_state, n_tasks in product([1, 3], [False, True], [0, 1, 150]):

            with self.subTest(n_jobs=n_jobs, use_worker_state=use_worker_state, n_tasks=n_tasks),\
                 WorkerPool(n_jobs=n_jobs, pass_worker_id=True) as pool:

                pool.set_use_worker_state(use_worker_state)

                # When use_worker_state is set, the final (worker_id, n_args) of each worker should add up to the
                # number of given tasks
                f = self._f1 if use_worker_state else self._f2
                results = pool.map(f, range(n_tasks), chunk_size=2)
                if use_worker_state:
                    n_processed_per_worker = [0] * n_jobs
                    for wid, n_processed, tests_succeed in results:
                        n_processed_per_worker[wid] = n_processed
                        self.assertTrue(tests_succeed)
                    self.assertEqual(sum(n_processed_per_worker), n_tasks)

    def test_by_constructor(self):
        """
        Tests setting worker state in the constructor
        """
        for n_jobs, use_worker_state, n_tasks in product([1, 3], [False, True], [0, 1, 150]):

            with self.subTest(n_jobs=n_jobs, use_worker_state=use_worker_state, n_tasks=n_tasks), \
                 WorkerPool(n_jobs=n_jobs, pass_worker_id=True, use_worker_state=use_worker_state) as pool:

                # When use_worker_state is set, the final (worker_id, n_args) of each worker should add up to the
                # number of given tasks
                f = self._f1 if use_worker_state else self._f2
                results = pool.map(f, range(n_tasks), chunk_size=2)
                if use_worker_state:
                    n_processed_per_worker = [0] * n_jobs
                    for wid, n_processed, tests_succeed in results:
                        n_processed_per_worker[wid] = n_processed
                        self.assertTrue(tests_succeed)
                    self.assertEqual(sum(n_processed_per_worker), n_tasks)

    def test_start_methods(self):
        """
        Test for different start methods
        """
        for start_method in TEST_START_METHODS:
            with self.subTest(start_method=start_method), \
                    WorkerPool(n_jobs=2, pass_worker_id=True, use_worker_state=True, start_method=start_method) as pool:
                results = pool.map(self._f1, range(10), chunk_size=2)
                n_processed_per_worker = [0, 0, 0]
                for wid, n_processed, tests_succeed in results:
                    n_processed_per_worker[wid] = n_processed
                    self.assertTrue(tests_succeed)
                self.assertEqual(sum(n_processed_per_worker), 10)

    @staticmethod
    def _f1(_wid, _wstate, _arg):
        """
        Function with worker ID and worker state
        """
        tests_succeed = True
        tests_succeed &= isinstance(_wstate, dict)

        # Worker id should always be the same
        _wstate.setdefault('worker_id', set()).add(_wid)
        tests_succeed &= _wstate['worker_id'] == {_wid}

        # Should contain previous args
        _wstate.setdefault('args', []).append(_arg)
        return _wid, len(_wstate['args']), tests_succeed

    @staticmethod
    def _f2(_wid, _):
        """
        Function with worker ID (simply tests if WorkerPool correctly handles use_worker_state=False)
        """
        pass


class InitFuncTest(unittest.TestCase):

    def setUp(self) -> None:
        self.test_data = range(10)
        self.test_desired_output = [42, 43, 44, 45, 46, 47, 48, 49, 50, 51]

    def test_no_init_func(self):
        """
        If the init func is not provided, then `worker_state['test']` should fail
        """
        with self.assertRaises(KeyError), WorkerPool(n_jobs=4, shared_objects=(None,), use_worker_state=True) as pool:
            pool.map(self._f, range(10), worker_init=None)

    def test_init_func(self):
        """
        Test if init func is called. If it is, then `worker_state['test']` should be available. Due to the barrier we
        know for sure that the init func should be called as many times as there are workers
        """
        for n_jobs in [1, 3]:
            shared_objects = Barrier(n_jobs), Value('i', 0)
            with self.subTest(n_jobs=n_jobs), WorkerPool(n_jobs=n_jobs, shared_objects=shared_objects,
                                                         use_worker_state=True) as pool:
                results = pool.map(self._f, self.test_data, worker_init=self._init, chunk_size=1)
                self.assertListEqual(results, self.test_desired_output)
                self.assertEqual(shared_objects[1].value, n_jobs)

    def test_worker_lifespan(self):
        """
        When workers have a limited lifespan they are spawned multiple times. Each time a worker starts it should call
        the init function. Due to the chunk size we know for sure that the init func should be called at least once for
        each task. However, when all tasks have been processed the workers are terminated and we don't know exactly how
        many workers restarted. We only know for sure that the init func should be called between 10 and 10 + n_jobs
        times
        """
        for n_jobs in [1, 3]:
            shared_objects = Barrier(n_jobs), Value('i', 0)
            with self.subTest(n_jobs=n_jobs), WorkerPool(n_jobs=n_jobs, shared_objects=shared_objects,
                                                         use_worker_state=True) as pool:
                results = pool.map(self._f, self.test_data, worker_init=self._init, chunk_size=1, worker_lifespan=1)
                self.assertListEqual(results, self.test_desired_output)
                self.assertGreaterEqual(shared_objects[1].value, 10)
                self.assertLessEqual(shared_objects[1].value, 10 + n_jobs)

    def test_error(self):
        """
        When an exception occurs in the init function it should properly shut down
        """
        with self.assertRaises(ValueError), WorkerPool(n_jobs=4, shared_objects=(None,), use_worker_state=True) as pool:
            pool.map(self._f, self.test_data, worker_init=self._init_error)

    def test_start_methods(self):
        """
        Test for different start methods
        """
        for start_method in TEST_START_METHODS:
            with self.subTest(start_method=start_method), \
                    WorkerPool(n_jobs=2, use_worker_state=True, start_method=start_method) as pool:
                shared_objects = pool.ctx.Barrier(2), pool.ctx.Value('i', 0)
                pool.set_shared_objects(shared_objects)
                results = pool.map(self._f, self.test_data, worker_init=self._init, chunk_size=1)
                self.assertListEqual(results, self.test_desired_output)
                self.assertEqual(shared_objects[1].value, 2)

    @staticmethod
    def _init(shared_objects, worker_state):
        barrier, call_count = shared_objects

        # Only wait for the other workers the first time around (it will hang when worker_lifespan=1, otherwise)
        if call_count.value == 0:
            barrier.wait()

        with call_count.get_lock():
            call_count.value += 1
        worker_state['test'] = 42

    @staticmethod
    def _init_error(*_):
        raise ValueError(":(")

    @staticmethod
    def _f(_, worker_state, x):
        return worker_state['test'] + x


class ExitFuncTest(unittest.TestCase):

    def setUp(self) -> None:
        self.test_data = range(10)
        self.test_desired_output = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

    def test_no_exit_func(self):
        """
        If the exit func is not provided, then exit results shouldn't be available
        """
        shared_objects = Barrier(4), Value('i', 0)
        with WorkerPool(n_jobs=4, shared_objects=shared_objects, use_worker_state=True) as pool:
            results = pool.map(self._f1, range(10), worker_init=self._init, worker_exit=None)
            self.assertListEqual(results, self.test_desired_output)
            self.assertListEqual(pool.get_exit_results(), [])

    def test_exit_func(self):
        """
        Test if exit func is called. If it is, then exit results should be available. It should have as many elements
        as the number of jobs and should have the right content.
        """
        for n_jobs in [1, 3]:
            shared_objects = Barrier(n_jobs), Value('i', 0)
            with self.subTest(n_jobs=n_jobs), WorkerPool(n_jobs=n_jobs, shared_objects=shared_objects,
                                                         use_worker_state=True) as pool:
                results = pool.map(self._f1, self.test_data, worker_init=self._init, worker_exit=self._exit)
                self.assertListEqual(results, self.test_desired_output)
                self.assertEqual(shared_objects[1].value, n_jobs)
                self.assertEqual(len(pool.get_exit_results()), n_jobs)
                self.assertEqual(sum(pool.get_exit_results()), sum(range(10)))

    def test_worker_lifespan(self):
        """
        When workers have a limited lifespan they are spawned multiple times. Each time a worker exits it should call
        the exit function. Due to the chunk size we know for sure that the exit func should be called at least once for
        each task. However, when all tasks have been processed the workers are terminated and we don't know exactly how
        many workers restarted. We only know for sure that the exit func should be called between 10 and 10 + n_jobs
        times
        """
        for n_jobs in [1, 3]:
            shared_objects = Barrier(n_jobs), Value('i', 0)
            with self.subTest(n_jobs=n_jobs), WorkerPool(n_jobs=n_jobs, shared_objects=shared_objects,
                                                         use_worker_state=True) as pool:
                results = pool.map(self._f1, self.test_data, worker_init=self._init, worker_exit=self._exit,
                                   chunk_size=1, worker_lifespan=1)
                self.assertListEqual(results, self.test_desired_output)
                self.assertGreaterEqual(shared_objects[1].value, 10)
                self.assertLessEqual(shared_objects[1].value, 10 + n_jobs)
                self.assertEqual(len(pool.get_exit_results()), shared_objects[1].value)
                self.assertEqual(sum(pool.get_exit_results()), sum(range(10)))

    def test_exit_func_big_payload(self):
        """
        Multiprocessing Pipes have a maximum buffer size (depending on the system it can be anywhere between 16-1024kb).
        Results from the pipe need to be received from the other end, before the workers are joined. Otherwise the
        process can hang indefinitely. Because exit results are fetched in a different way as regular results, we test
        that here. We send a payload of 10_000kb.
        """
        for n_jobs, worker_lifespan in product([1, 3], [None, 2]):
            with self.subTest(n_jobs=n_jobs, worker_lifespan=worker_lifespan), WorkerPool(n_jobs=n_jobs) as pool:
                results = pool.map(self._f2, self.test_data, worker_exit=self._exit_big_payloud, chunk_size=1,
                                   worker_lifespan=worker_lifespan)
                self.assertListEqual(results, self.test_desired_output)
                self.assertTrue(bool(pool.get_exit_results()))
                for exit_result in pool.get_exit_results():
                    self.assertEqual(len(exit_result), 10_000 * 1024)

    # TODO: This function prints OSError messages on Windows. Not going to fix this now as it's harmless.
    def test_error(self):
        """
        When an exception occurs in the exit function it should properly shut down
        """
        for worker_lifespan in [None, 2]:
            with self.subTest(worker_lifespan=worker_lifespan), self.assertRaises(ValueError), \
                    WorkerPool(n_jobs=4) as pool:
                pool.map(self._f2, range(10), worker_lifespan=worker_lifespan, worker_exit=self._exit_error)

    def test_start_methods(self):
        """
        Test for different start methods
        """
        for start_method in TEST_START_METHODS:
            with self.subTest(start_method=start_method), \
                    WorkerPool(n_jobs=2, use_worker_state=True, start_method=start_method) as pool:
                shared_objects = pool.ctx.Barrier(2), pool.ctx.Value('i', 0)
                pool.set_shared_objects(shared_objects)
                results = pool.map(self._f1, self.test_data, worker_init=self._init, worker_exit=self._exit)
                self.assertListEqual(results, self.test_desired_output)
                self.assertEqual(shared_objects[1].value, 2)
                self.assertEqual(len(pool.get_exit_results()), 2)
                self.assertEqual(sum(pool.get_exit_results()), sum(range(10)))

    @staticmethod
    def _init(shared_objects, worker_state):
        barrier, call_count = shared_objects

        # Only wait for the other workers the first time around (it will hang when worker_lifespan=1, otherwise)
        if call_count.value == 0:
            barrier.wait()

        worker_state['count'] = 0

    @staticmethod
    def _f1(_, worker_state, x):
        worker_state['count'] += x
        return x

    @staticmethod
    def _f2(x):
        return x

    @staticmethod
    def _exit(shared_objects, worker_state):
        _, call_count = shared_objects
        with call_count.get_lock():
            call_count.value += 1
        return worker_state['count']

    @staticmethod
    def _exit_big_payloud():
        return np.random.bytes(10_000 * 1024)

    @staticmethod
    def _exit_error():
        raise ValueError(":'(")


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
                self.assertIsInstance(results_list, list)
                self.assertEqual(self.test_desired_output, results_list)

    def test_deamon_nested_workerpool(self):
        """
        Tests nested WorkerPools when daemon==True, which should not work
        """
        with self.assertRaises(AssertionError), WorkerPool(n_jobs=4, daemon=True) as pool:
            pool.map(self._square_daemon, ((X,) for X in repeat(self.test_data, 4)), chunk_size=1)

    def test_start_methods(self):
        """
        Test for different start methods
        """
        for start_method in TEST_START_METHODS:
            with self.subTest(start_method=start_method), \
                    WorkerPool(n_jobs=2, daemon=False, start_method=start_method) as pool:
                pool.map(self._square_daemon, ((X,) for X in repeat(self.test_data, 3)), chunk_size=1)

    @staticmethod
    def _square_daemon(x):
        with WorkerPool(n_jobs=4) as pool:
            return pool.map(square, x, chunk_size=1)


class CPUPinningTest(unittest.TestCase):

    def setUp(self):
        # Create some test data. Note that the regular map reads the inputs as a list of single tuples (one argument),
        # whereas parallel.map sees it as a list of argument lists. Therefore we give the regular map a lambda function
        # which mimics the parallel.map behavior.
        self.test_data = list(enumerate([1, 2, 3, 5, 6, 9, 37, 42, 1337, 0, 3, 5, 0]))
        self.test_desired_output = list(map(lambda _args: square(*_args), self.test_data))

    def test_cpu_pinning(self):
        """
        Test that when parameters are valid, nothing breaks and the pinning is actually happening
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
            if cpu_ids is not None and np.array(cpu_ids).max(initial=0) >= cpu_count():
                continue

            with self.subTest(n_jobs=n_jobs, cpu_ids=cpu_ids), patch('mpire.pool.set_cpu_affinity') as p, \
                    WorkerPool(n_jobs=n_jobs, cpu_ids=cpu_ids) as pool:

                # Verify results
                results_list = pool.map(square, self.test_data)
                self.assertIsInstance(results_list, list)
                self.assertEqual(self.test_desired_output, results_list)

                # Verify that when CPU pinning is used, it is called as many times as there are jobs and is called for
                # each worker process ID
                if cpu_ids is None:
                    self.assertEqual(p.call_args_list, [])
                else:
                    self.assertEqual(p.call_count, pool.pool_params.n_jobs)
                    mask = [call[0][1] for call in p.call_args_list]
                    self.assertListEqual(mask, expected_mask)

    def test_start_methods(self):
        """
        Test for different start methods
        """
        # This test will fail if there are less CPUs available than specified.
        if cpu_count() >= 2:
            n_jobs, cpu_ids, expected_mask = 2, [1, 0], [[1], [0]]
        else:
            n_jobs, cpu_ids, expected_mask = 1, [0], [[0]]

        for start_method in TEST_START_METHODS:
            if start_method == 'threading':
                continue
            with self.subTest(start_method=start_method), patch('mpire.pool.set_cpu_affinity') as p, \
                    WorkerPool(n_jobs=n_jobs, cpu_ids=cpu_ids, start_method=start_method) as pool:
                # Verify results
                results_list = pool.map(square, self.test_data)
                self.assertIsInstance(results_list, list)
                self.assertEqual(self.test_desired_output, results_list)

                # Verify that CPU pinning is used as many times as there are jobs and is called for each worker process
                # ID
                self.assertEqual(p.call_count, pool.pool_params.n_jobs)
                mask = [call[0][1] for call in p.call_args_list]
                self.assertListEqual(mask, expected_mask)

        # This won't work for threading
        with self.assertRaises(AttributeError), WorkerPool(n_jobs=n_jobs, cpu_ids=cpu_ids,
                                                           start_method='threading') as pool:
            pool.map(square, self.test_data)


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

        # Get original tqdm lock
        self.original_tqdm_lock = tqdm.get_lock()

    def tearDown(self):
        # The TQDM lock is temporarily changed when using a progress bar in MPIRE, here we check if it is restored
        # correctly afterwards.
        self.assertNotIsInstance(tqdm.get_lock(), TqdmLock)
        self.assertEqual(tqdm.get_lock(), self.original_tqdm_lock)

    def test_valid_progress_bars_regular_input(self):
        """
        Valid progress bars are either False/True
        """
        print()
        for n_jobs, progress_bar in product([None, 1, 2], [True, False]):

            with self.subTest(n_jobs=n_jobs), WorkerPool(n_jobs=n_jobs) as pool:
                results_list = pool.map(square, self.test_data, progress_bar=progress_bar)
                self.assertIsInstance(results_list, list)
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
                self.assertIsInstance(results, np.ndarray)
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

    def test_start_methods(self):
        """
        Test for different start methods
        """
        print()
        for start_method in TEST_START_METHODS:
            with self.subTest(start_method=start_method), \
                    WorkerPool(n_jobs=2, start_method=start_method) as pool:
                # Progress bar on Windows with threading is currently not supported
                if RUNNING_WINDOWS and start_method == 'threading':
                    with self.assertRaises(ValueError):
                        pool.map(square, self.test_data, progress_bar=True)
                else:
                    results_list = pool.map(square, self.test_data, progress_bar=True)
                    self.assertIsInstance(results_list, list)
                    self.assertEqual(self.test_desired_output, results_list)


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
        for n_jobs in [1, 3]:
            barrier = Barrier(n_jobs)
            counter = Value('i', 0)
            shared = barrier, counter
            with self.subTest(n_jobs=n_jobs), \
                    WorkerPool(n_jobs=n_jobs, shared_objects=shared, use_worker_state=True, keep_alive=False) as pool:

                self.assertListEqual(pool.map(self._f1, self.test_data, worker_init=self._init1),
                                     self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs)
                barrier.reset()

                self.assertListEqual(pool.map(self._f1, self.test_data, worker_init=self._init1),
                                     self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs * 2)
                barrier.reset()

                self.assertListEqual(pool.map(self._f1, self.test_data, worker_init=self._init1),
                                     self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs * 3)
                barrier.reset()

                self.assertListEqual(pool.map(self._f1, self.test_data, worker_init=self._init1),
                                     self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs * 4)

    def test_keep_alive(self):
        """
        When keep_alive is set to True it should reuse existing workers between map calls. This means the counter is
        only updated the first time.
        """
        for n_jobs in [1, 3]:
            barrier = Barrier(n_jobs)
            counter = Value('i', 0)
            shared = barrier, counter
            with self.subTest(n_jobs=n_jobs), \
                    WorkerPool(n_jobs=n_jobs, shared_objects=shared, use_worker_state=True, keep_alive=True) as pool:

                self.assertListEqual(pool.map(self._f1, self.test_data, worker_init=self._init1),
                                     self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs)
                barrier.reset()

                self.assertListEqual(list(pool.imap(self._f1, self.test_data, worker_init=self._init1)),
                                     self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs)
                barrier.reset()

                self.assertListEqual(pool.map(self._f1, self.test_data, worker_init=self._init1),
                                     self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs)

    def test_keep_alive_map_params_change(self):
        """
        When keep_alive is set to True it should reuse existing workers between map calls, even when the called
        function, init or exit functions, or the worker lifespan changes
        """
        for n_jobs in [1, 3]:
            barrier = Barrier(n_jobs)
            counter = Value('i', 0)
            shared = barrier, counter
            with self.subTest(n_jobs=n_jobs), warnings.catch_warnings(), \
                    WorkerPool(n_jobs=n_jobs, shared_objects=shared, use_worker_state=True, keep_alive=True) as pool:

                warnings.simplefilter('ignore')
                self.assertListEqual(pool.map(self._f1, self.test_data, worker_lifespan=100, worker_init=self._init1,
                                              worker_exit=self._exit1),
                                     self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs)
                barrier.reset()

                self.assertListEqual(list(pool.imap(self._f2, self.test_data, worker_lifespan=100,
                                                    worker_init=self._init1, worker_exit=self._exit2)),
                                     self.test_desired_output_f2)
                self.assertEqual(counter.value, n_jobs)
                barrier.reset()

                self.assertListEqual(pool.map(self._f2, self.test_data, worker_lifespan=200, worker_init=self._init2,
                                              worker_exit=self._exit1),
                                     self.test_desired_output_f2)
                self.assertEqual(counter.value, n_jobs)
                barrier.reset()

                self.assertListEqual(pool.map(self._f1, self.test_data, worker_lifespan=100, worker_init=self._init1,
                                              worker_exit=None),
                                     self.test_desired_output_f1)
                self.assertEqual(counter.value, n_jobs)

    def test_start_methods(self):
        """
        Test for different start methods
        """
        for start_method in TEST_START_METHODS:
            with self.subTest(start_method=start_method), \
                    WorkerPool(n_jobs=2, use_worker_state=True, keep_alive=True, start_method=start_method) as pool:

                barrier = pool.ctx.Barrier(2)
                counter = pool.ctx.Value('i', 0)
                pool.set_shared_objects((barrier, counter))
                self.assertListEqual(pool.map(self._f1, self.test_data, worker_init=self._init1),
                                     self.test_desired_output_f1)
                self.assertEqual(counter.value, 2)
                barrier.reset()

                self.assertListEqual(list(pool.imap(self._f1, self.test_data, worker_init=self._init1)),
                                     self.test_desired_output_f1)
                self.assertEqual(counter.value, 2)
                barrier.reset()

                self.assertListEqual(pool.map(self._f1, self.test_data, worker_init=self._init1),
                                     self.test_desired_output_f1)
                self.assertEqual(counter.value, 2)

    @staticmethod
    def _init1(_, worker_state):
        worker_state['already_counted'] = False

    @staticmethod
    def _init2(_, worker_state):
        worker_state['already_counted'] = False
        worker_state[4] = 2

    @staticmethod
    def _f1(shared, worker_state, x):
        """
        Function that waits for all workers to spin up and increases the counter by one only once per worker,
        returns x * 2
        """
        barrier, counter = shared
        if not worker_state['already_counted']:
            with counter.get_lock():
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
        if not worker_state['already_counted']:
            with counter.get_lock():
                counter.value += 1
            worker_state['already_counted'] = True
            barrier.wait()
        return x * 3

    @staticmethod
    def _exit1(_, worker_state):
        return worker_state['already_counted']

    @staticmethod
    def _exit2(_, worker_state):
        pass


class ExceptionTest(unittest.TestCase):

    def setUp(self):
        # Create some test data. Note that the regular map reads the inputs as a list of single tuples (one argument),
        # whereas parallel.map sees it as a list of argument lists. Therefore we give the regular map a lambda function
        # which mimics the parallel.map behavior.
        self.test_data = list(enumerate([1, 2, 3, 5, 6, 9, 37, 42, 1337, 0, 3, 5, 0]))
        self.test_desired_output = list(map(lambda _args: square(*_args), self.test_data))
        self.test_data_len = len(self.test_data)

        # Setup logger for debug purposes
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

        # Get original tqdm lock
        self.original_tqdm_lock = tqdm.get_lock()

    def tearDown(self):
        self.logger.setLevel(logging.NOTSET)

        # The TQDM lock is temporarily changed when using a progress bar in MPIRE, here we check if it is restored
        # correctly afterwards.
        self.assertNotIsInstance(tqdm.get_lock(), TqdmLock)
        self.assertEqual(tqdm.get_lock(), self.original_tqdm_lock)

    def test_exceptions(self):
        """
        Tests if MPIRE can handle exceptions well
        """
        # This print statement is intentional as it will print multiple progress bars
        print()
        for n_jobs, n_tasks_max_active, worker_lifespan, progress_bar in [
            (1, None, None, False),
            (3, None, None, False),
            (3, 1, None, False),
            (3, None, 1, False),
            (3, None, None, True),
            (3, 1, None, True),
            (3, None, 1, True),
            (3, 1, 1, True)
        ]:
            self.logger.debug(f"========== {n_jobs}, {n_tasks_max_active}, {worker_lifespan}, {progress_bar} "
                              f"==========")
            with WorkerPool(n_jobs=n_jobs) as pool:

                # Should work for map like functions
                self.logger.debug("----- square_raises, map -----")
                with self.subTest(n_jobs=n_jobs, n_tasks_max_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                  progress_bar=progress_bar, function='square_raises', map='map'), \
                     self.assertRaises(ValueError):
                    pool.map(self._square_raises, self.test_data, max_tasks_active=n_tasks_max_active,
                             worker_lifespan=worker_lifespan, progress_bar=progress_bar)

                # Should work for imap like functions
                self.logger.debug("----- square_raises, imap -----")
                with self.subTest(n_jobs=n_jobs, n_tasks_max_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                  progress_bar=progress_bar, function='square_raises', map='imap'), \
                     self.assertRaises(ValueError):
                    list(pool.imap_unordered(self._square_raises, self.test_data, max_tasks_active=n_tasks_max_active,
                                             worker_lifespan=worker_lifespan, progress_bar=progress_bar))

                # Should work for map like functions
                self.logger.debug("----- square_raises_on_idx, map -----")
                with self.subTest(n_jobs=n_jobs, n_tasks_max_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                  progress_bar=progress_bar, function='square_raises_on_idx', map='map'), \
                     self.assertRaises(ValueError):
                    pool.map(self._square_raises_on_idx, self.test_data, max_tasks_active=n_tasks_max_active,
                             worker_lifespan=worker_lifespan, progress_bar=progress_bar)

                # Should work for imap like functions
                self.logger.debug("----- square_raises_on_idx, imap -----")
                with self.subTest(n_jobs=n_jobs, n_tasks_max_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                  progress_bar=progress_bar, function='square_raises_on_idx', map='imap'), \
                     self.assertRaises(ValueError):
                    list(pool.imap_unordered(self._square_raises_on_idx, self.test_data,
                                             max_tasks_active=n_tasks_max_active, worker_lifespan=worker_lifespan,
                                             progress_bar=progress_bar))

    def test_start_methods(self):
        """
        Test for different start methods
        """
        print()
        for start_method, progress_bar in product(TEST_START_METHODS, [False, True]):
            self.logger.debug(f"========== {start_method}, {progress_bar} ==========")
            if RUNNING_WINDOWS and progress_bar and start_method == 'threading':
                self.logger.debug("Not yet supported on Windows")
                continue
            with self.subTest(start_method=start_method, progress_bar=progress_bar), \
                    WorkerPool(n_jobs=2, start_method=start_method) as pool:

                # Should work for map like functions
                self.logger.debug("----- square_raises, map -----")
                with self.subTest(function='square_raises', map='map'), self.assertRaises(ValueError):
                    pool.map(self._square_raises, self.test_data, progress_bar=progress_bar)

                # Should work for imap like functions
                self.logger.debug("----- square_raises, imap -----")
                with self.subTest(function='square_raises', map='imap'), self.assertRaises(ValueError):
                    list(pool.imap_unordered(self._square_raises, self.test_data, progress_bar=progress_bar))

                # Should work for map like functions
                self.logger.debug("----- square_raises_on_idx, map -----")
                with self.subTest(function='square_raises_on_idx', map='map'), self.assertRaises(ValueError):
                    pool.map(self._square_raises_on_idx, self.test_data, progress_bar=progress_bar)

                # Should work for imap like functions
                self.logger.debug("----- square_raises_on_idx, imap -----")
                with self.subTest(function='square_raises_on_idx', map='imap'), self.assertRaises(ValueError):
                    list(pool.imap_unordered(self._square_raises_on_idx, self.test_data, progress_bar=progress_bar))

    def test_defunct_processes_exit(self):
        """
        Tests if MPIRE correctly shuts down after process becomes defunct using exit()
        """
        print()
        for n_jobs, progress_bar, worker_lifespan in [(1, False, None),
                                                      (3, True, 1),
                                                      (3, False, 3)]:
            for start_method in TEST_START_METHODS:
                # Progress bar on Windows + threading is not supported right now
                if RUNNING_WINDOWS and start_method == 'threading' and progress_bar:
                    continue
                self.logger.debug(f"========== {start_method}, {n_jobs}, {progress_bar}, {worker_lifespan} ==========")
                with self.subTest(n_jobs=n_jobs, progress_bar=progress_bar, worker_lifespan=worker_lifespan,
                                  start_method=start_method), self.assertRaises(SystemExit), \
                        WorkerPool(n_jobs=n_jobs, start_method=start_method) as pool:
                    pool.map(self._exit, range(100), progress_bar=progress_bar, worker_lifespan=worker_lifespan)

    def test_defunct_processes_kill(self):
        """
        Tests if MPIRE correctly shuts down after one process becomes defunct using os.kill().

        We kill worker 0 and to be sure it's alive we set an event object and then go in an infinite loop. The kill
        thread waits until the event is set and then kills the worker. The other workers are also ensured to have done
        something so we can test what happens during restarts
        """
        print()
        for n_jobs, progress_bar, worker_lifespan in [(1, False, None),
                                                      (3, True, 1),
                                                      (3, False, 3)]:
            for start_method in TEST_START_METHODS:
                # Can't kill threads
                if start_method == 'threading':
                    continue

                self.logger.debug(f"========== {start_method}, {n_jobs}, {progress_bar}, {worker_lifespan} ==========")
                with self.subTest(n_jobs=n_jobs, progress_bar=progress_bar, worker_lifespan=worker_lifespan,
                                  start_method=start_method), self.assertRaises(RuntimeError), \
                        WorkerPool(n_jobs=n_jobs, pass_worker_id=True, start_method=start_method) as pool:
                    events = [pool.ctx.Event() for _ in range(n_jobs)]
                    kill_thread = Thread(target=self._kill_process, args=(events[0], pool))
                    kill_thread.start()
                    pool.set_shared_objects(events)
                    pool.map(self._worker_0_sleeps_others_square, range(1000), progress_bar=progress_bar,
                             worker_lifespan=worker_lifespan, chunk_size=1)

    @staticmethod
    def _square_raises(_, x):
        raise ValueError(x)

    @staticmethod
    def _square_raises_on_idx(idx, x):
        if idx == 5:
            raise ValueError(x)
        else:
            return idx, x * x

    @staticmethod
    def _exit(_):
        exit()

    @staticmethod
    def _worker_0_sleeps_others_square(worker_id, events, x):
        """
        Worker 0 waits until the other workers have at least spun up and then sets her event and sleeps
        """
        if worker_id == 0:
            [event.wait() for event in events[1:]]
            events[0].set()
            while True:
                pass
        else:
            events[worker_id].set()
            return x * x

    @staticmethod
    def _kill_process(event, pool):
        """
        Wait for event and kill
        """
        event.wait()
        pool._workers[0].terminate()


class TimeoutTest(unittest.TestCase):

    def setUp(self):
        # Create some test data
        self.test_data = [1, 2, 3]

    def test_worker_init_timeout(self):
        """
        Checks if the worker_init timeout is properly triggered
        """
        for start_method in TEST_START_METHODS:

            with self.subTest('Well below timeout', start_method=start_method), \
                    WorkerPool(2, start_method=start_method) as pool:
                self.assertListEqual(pool.map(self._f1, self.test_data, worker_init=self._init1,
                                              worker_init_timeout=100), self.test_data)

            with self.subTest('Exceeding timeout, map', start_method=start_method), \
                    WorkerPool(2, start_method=start_method) as pool, self.assertRaises(TimeoutError):
                pool.map(self._f1, self.test_data, worker_init=self._init2, worker_init_timeout=0.1)

            with self.subTest('Exceeding timeout, imap', start_method=start_method), \
                    WorkerPool(2, start_method=start_method) as pool, self.assertRaises(TimeoutError):
                for _ in pool.imap(self._f1, self.test_data, worker_init=self._init2, worker_init_timeout=0.1):
                    pass

    def test_worker_task_timeout(self):
        """
        Checks if the worker_init timeout is properly triggered
        """
        for start_method in TEST_START_METHODS:

            with self.subTest('Well below timeout', start_method=start_method), \
                    WorkerPool(2, start_method=start_method) as pool:
                self.assertListEqual(pool.map(self._f1, self.test_data, task_timeout=100), self.test_data)

            with self.subTest('Exceeding timeout, map', start_method=start_method), \
                    WorkerPool(2, start_method=start_method) as pool, self.assertRaises(TimeoutError):
                pool.map(self._f2, self.test_data, task_timeout=0.1)

            with self.subTest('Exceeding timeout, imap', start_method=start_method), \
                    WorkerPool(2, start_method=start_method) as pool, self.assertRaises(TimeoutError):
                for _ in pool.imap(self._f2, self.test_data, task_timeout=0.1):
                    pass

    def test_worker_exit_timeout(self):
        """
        Checks if the worker_exit timeout is properly triggered
        """
        for start_method in TEST_START_METHODS:

            with self.subTest('Well below timeout', start_method=start_method), \
                    WorkerPool(2, start_method=start_method) as pool:
                self.assertListEqual(pool.map(self._f1, self.test_data, worker_exit=self._exit1,
                                              worker_exit_timeout=100), self.test_data)

            with self.subTest('Exceeding timeout, map', start_method=start_method), \
                    WorkerPool(2, start_method=start_method) as pool, self.assertRaises(TimeoutError):
                pool.map(self._f1, self.test_data, worker_exit=self._exit2, worker_exit_timeout=0.1)

            with self.subTest('Exceeding timeout, imap', start_method=start_method), \
                    WorkerPool(2, start_method=start_method) as pool, self.assertRaises(TimeoutError):
                for _ in pool.imap(self._f1, self.test_data, worker_exit=self._exit2, worker_exit_timeout=0.1):
                    pass

    @staticmethod
    def _init1():
        pass

    @staticmethod
    def _init2():
        time.sleep(1)

    @staticmethod
    def _f1(x):
        return x

    @staticmethod
    def _f2(x):
        time.sleep(1)
        return x

    @staticmethod
    def _exit1():
        pass

    @staticmethod
    def _exit2():
        time.sleep(1)
