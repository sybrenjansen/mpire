import unittest
import warnings
from itertools import product
from unittest.mock import patch

import numpy as np

from mpire import cpu_count
from mpire.params import check_map_parameters, WorkerMapParams, WorkerPoolParams


def square(idx, x):
    return idx, x * x


class WorkerPoolParamsTest(unittest.TestCase):

    def setUp(self):
        # Create some test data. Note that the regular map reads the inputs as a list of single tuples (one argument),
        # whereas parallel.map sees it as a list of argument lists. Therefore we give the regular map a lambda function
        # which mimics the parallel.map behavior.
        self.test_data = list(enumerate([1, 2, 3, 5, 6, 9, 37, 42, 1337, 0, 3, 5, 0]))
        self.test_desired_output = list(map(lambda _args: square(*_args), self.test_data))

    def test_n_jobs(self):
        """
        When n_jobs is 0 or None it should evaluate to cpu_count(), otherwise it should stay is.
        """
        with patch('mpire.params.mp.cpu_count', return_value=4):
            for n_jobs, expected_njobs in [(0, 4), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (10, 10), (None, 4)]:
                with self.subTest(n_jobs=n_jobs):
                    self.assertEqual(WorkerPoolParams(n_jobs, None).n_jobs, expected_njobs)

    def test_check_cpu_ids_valid_input(self):
        """
        Test that when the parameters are valid, they are converted to the correct cpu ID mask
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

            with self.subTest(n_jobs=n_jobs, cpu_ids=cpu_ids):
                params = WorkerPoolParams(n_jobs=n_jobs, cpu_ids=cpu_ids)
                self.assertListEqual(params.cpu_ids, expected_mask)

    def test_check_cpu_ids_invalid_input(self):
        """
        Test that when parameters are invalid, an error is raised
        """
        for n_jobs, cpu_ids in product([None, 1, 2, 4], [[0, 1], [0, 1, 2, 3], [[0, 1], [0, 1]]]):
            if len(cpu_ids) != (n_jobs or cpu_count()):
                with self.subTest(n_jobs=n_jobs, cpu_ids=cpu_ids), self.assertRaises(ValueError):
                    WorkerPoolParams(n_jobs=n_jobs, cpu_ids=cpu_ids)

        # Should raise when CPU IDs are out of scope
        with self.assertRaises(ValueError):
            WorkerPoolParams(n_jobs=1, cpu_ids=[-1])
        with self.assertRaises(ValueError):
            WorkerPoolParams(n_jobs=1, cpu_ids=[cpu_count()])


class WorkerMapParamsTest(unittest.TestCase):

    def test_eq(self):
        """
        Test equality
        """
        params = WorkerMapParams(lambda x: x, None, None, None, False, None, None, None)

        with self.subTest('not initialized'), warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for (func, worker_init, worker_exit, worker_lifespan, progress_bar,
                 task_timeout, worker_init_timeout, worker_exit_timeout) in product(
                    [self._f1, self._f2],
                    [None, self._init1, self._init2],
                    [None, self._exit1, self._exit2],
                    [None, 42, 1337],
                    [False, True],
                    [None, 30],
                    [None, 42],
                    [None, 37],
            ):
                self.assertNotEqual(params, WorkerMapParams(func, worker_init, worker_exit, worker_lifespan,
                                                            progress_bar, task_timeout, worker_init_timeout,
                                                            worker_exit_timeout))

        params = WorkerMapParams(self._f1, self._init1, self._exit1, 42, True, 1, 2, 3)

        with self.subTest('initialized and nothing changed'):
            self.assertEqual(params, WorkerMapParams(self._f1, self._init1, self._exit1, 42, True, 1, 2, 3))

        with self.subTest('initialized and a parameter changed'), warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.assertNotEqual(params, WorkerMapParams(self._f2, self._init1, self._exit1, 42, True, 1, 2, 3))
            self.assertNotEqual(params, WorkerMapParams(self._f1, self._init2, self._exit1, 42, True, 1, 2, 3))
            self.assertNotEqual(params, WorkerMapParams(self._f1, self._init1, self._exit2, 42, True, 1, 2, 3))
            self.assertNotEqual(params, WorkerMapParams(self._f1, self._init1, self._exit1, 1337, True, 1, 2, 3))
            self.assertNotEqual(params, WorkerMapParams(self._f1, self._init1, self._exit1, 42, False, 1, 2, 3))
            self.assertNotEqual(params, WorkerMapParams(self._f1, self._init1, self._exit1, 42, True, 2, 2, 3))
            self.assertNotEqual(params, WorkerMapParams(self._f1, self._init1, self._exit1, 42, True, 1, 3, 3))
            self.assertNotEqual(params, WorkerMapParams(self._f1, self._init1, self._exit1, 42, True, 1, 2, 4))

    @staticmethod
    def _init1():
        pass

    @staticmethod
    def _init2():
        pass

    @staticmethod
    def _f1(_):
        pass

    @staticmethod
    def _f2(_):
        pass

    @staticmethod
    def _exit1():
        pass

    @staticmethod
    def _exit2():
        pass


class CheckMapParametersTest(unittest.TestCase):

    def test_n_tasks(self):
        """
        Should raise when wrong parameter values are used
        """
        pool_params = WorkerPoolParams(None, None)

        # Get number of tasks
        with self.subTest('get n_tasks', iterable_len=100):
            n_tasks, *_ = check_map_parameters(
                pool_params, iterable_of_args=(x for x in []), iterable_len=100, max_tasks_active=None, chunk_size=None,
                n_splits=None, worker_lifespan=None, progress_bar=False, progress_bar_position=0, task_timeout=None,
                worker_init_timeout=None, worker_exit_timeout=None
            )
            self.assertEqual(n_tasks, 100)
        with self.subTest('get n_tasks, __len__ implemented', iterable_len=100):
            n_tasks, *_ = check_map_parameters(
                pool_params, iterable_of_args=[1, 2, 3], iterable_len=100, max_tasks_active=None, chunk_size=None,
                n_splits=None, worker_lifespan=None, progress_bar=False, progress_bar_position=0, task_timeout=None,
                worker_init_timeout=None, worker_exit_timeout=None
            )
            self.assertEqual(n_tasks, 100)
        with self.subTest('get n_tasks, __len__ implemented', iterable_len=None):
            n_tasks, *_ = check_map_parameters(
                pool_params, iterable_of_args=[1, 2, 3], iterable_len=None, max_tasks_active=None, chunk_size=None,
                n_splits=None, worker_lifespan=None, progress_bar=False, progress_bar_position=0, task_timeout=None,
                worker_init_timeout=None, worker_exit_timeout=None
            )
            self.assertEqual(n_tasks, 3)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            # When number of tasks can't be determined and chunk_size is None or a progress bar is requested, the chunk
            # size should be set to 1 and no progress bar should be used
            with self.subTest('no iterable_len', chunk_size=None):
                n_tasks, max_tasks_active, chunk_size, progress_bar = check_map_parameters(
                    pool_params, iterable_of_args=(x for x in []), iterable_len=None, max_tasks_active=None,
                    chunk_size=None, n_splits=None, worker_lifespan=None, progress_bar=False, progress_bar_position=0,
                    task_timeout=None, worker_init_timeout=None, worker_exit_timeout=None
                )
                self.assertIsNone(n_tasks)
                self.assertEqual(chunk_size, 1)
                self.assertFalse(progress_bar)
            with self.subTest('no iterable_len', progress_bar=True):
                n_tasks, max_tasks_active, chunk_size, progress_bar = check_map_parameters(
                    pool_params, iterable_of_args=(x for x in []), iterable_len=None, max_tasks_active=None,
                    chunk_size=None, n_splits=None, worker_lifespan=None, progress_bar=True, progress_bar_position=0,
                    task_timeout=None, worker_init_timeout=None, worker_exit_timeout=None
                )
                self.assertIsNone(n_tasks)
                self.assertEqual(chunk_size, 1)
                self.assertFalse(progress_bar)

    def test_chunk_size(self):
        """
        Check chunk_size parameter. Should raise when wrong parameter values are used.
        """
        pool_params = WorkerPoolParams(None, None)

        # Should work fine
        for chunk_size in [1, 100, int(1e8), 0.8, 13.37, None]:
            with self.subTest(chunk_size=chunk_size):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=chunk_size, n_splits=None, worker_lifespan=None,
                                     progress_bar=False, progress_bar_position=0, task_timeout=None,
                                     worker_init_timeout=None, worker_exit_timeout=None)

        # chunk_size should be an integer, float, or None
        for chunk_size in ['3', {8}]:
            with self.subTest(chunk_size=chunk_size), self.assertRaises(TypeError):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=chunk_size, n_splits=None, worker_lifespan=None, progress_bar=False,
                                     progress_bar_position=0, task_timeout=None, worker_init_timeout=None,
                                     worker_exit_timeout=None)

        # chunk_size should be positive > 0
        for chunk_size in [0, -5]:
            with self.subTest(chunk_size=chunk_size), self.assertRaises(ValueError):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=chunk_size, n_splits=None, worker_lifespan=None, progress_bar=False,
                                     progress_bar_position=0, task_timeout=None, worker_init_timeout=None,
                                     worker_exit_timeout=None)

    def test_n_splits(self):
        """
        Check n_splits parameter. Should raise when wrong parameter values are used.
        """
        pool_params = WorkerPoolParams(None, None)

        # Should work fine
        for n_splits in [1, 100, int(1e8), None]:
            with self.subTest(n_splits=n_splits):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=None, n_splits=n_splits, worker_lifespan=None, progress_bar=False,
                                     progress_bar_position=0, task_timeout=None, worker_init_timeout=None,
                                     worker_exit_timeout=None)

        # n_splits should be an integer or None
        for n_splits in ['3', 3.14, {8}]:
            with self.subTest(n_splits=n_splits), self.assertRaises(TypeError):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=None, n_splits=n_splits, worker_lifespan=None, progress_bar=False,
                                     progress_bar_position=0, task_timeout=None, worker_init_timeout=None,
                                     worker_exit_timeout=None)

        # n_splits should be positive > 0
        for n_splits in [0, -5]:
            with self.subTest(n_splits=n_splits), self.assertRaises(ValueError):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=None, n_splits=n_splits, worker_lifespan=None, progress_bar=False,
                                     progress_bar_position=0, task_timeout=None, worker_init_timeout=None,
                                     worker_exit_timeout=None)

    def test_max_tasks_active(self):
        """
        Check max_tasks_active parameter. Should raise when wrong parameter values are used.
        """
        for n_jobs in [1, 2, 4]:
            pool_params = WorkerPoolParams(n_jobs, None)

            # Should work fine. When input is None, max_tasks_active should be converted to n_jobs * 2
            for max_tasks_active in [1, 100, int(1e8), None]:
                with self.subTest(n_jobs=n_jobs, max_tasks_active=max_tasks_active):
                    _, new_max_tasks_active, *_ = check_map_parameters(
                        pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=max_tasks_active,
                        chunk_size=None, n_splits=None, worker_lifespan=None, progress_bar=False,
                        progress_bar_position=0, task_timeout=None, worker_init_timeout=None, worker_exit_timeout=None
                    )
                    self.assertEqual(new_max_tasks_active, max_tasks_active or n_jobs * 2)

            # max_tasks_active should be an integer or None
            for max_tasks_active in ['3', 3.14, {8}]:
                with self.subTest(n_jobs=n_jobs, max_tasks_active=max_tasks_active), self.assertRaises(TypeError):
                    check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None,
                                         max_tasks_active=max_tasks_active, chunk_size=None, n_splits=None,
                                         worker_lifespan=None, progress_bar=False, progress_bar_position=0,
                                         task_timeout=None, worker_init_timeout=None, worker_exit_timeout=None)

            # max_tasks_active should be positive > 0
            for max_tasks_active in [0, -5]:
                with self.subTest(n_jobs=n_jobs, max_tasks_active=max_tasks_active), self.assertRaises(ValueError):
                    check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None,
                                         max_tasks_active=max_tasks_active, chunk_size=None, n_splits=None,
                                         worker_lifespan=None, progress_bar=False, progress_bar_position=0,
                                         task_timeout=None, worker_init_timeout=None, worker_exit_timeout=None)

    def test_worker_lifespan(self):
        """
        Check worker_lifespan parameter. Should raise when wrong parameter values are used.
        """
        pool_params = WorkerPoolParams(None, None)

        # Should work fine
        for worker_lifespan in [1, 100, int(1e8), None]:
            with self.subTest(worker_lifespan=worker_lifespan):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=None, n_splits=None, worker_lifespan=worker_lifespan,
                                     progress_bar=False, progress_bar_position=0, task_timeout=None,
                                     worker_init_timeout=None, worker_exit_timeout=None)

        # worker_lifespan should be an integer or None
        for worker_lifespan in ['3', 3.14, {8}]:
            with self.subTest(worker_lifespan=worker_lifespan), self.assertRaises(TypeError):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=None, n_splits=None, worker_lifespan=worker_lifespan,
                                     progress_bar=False, progress_bar_position=0, task_timeout=None,
                                     worker_init_timeout=None, worker_exit_timeout=None)

        # max_tasks_active should be positive > 0
        for worker_lifespan in [0, -5]:
            with self.subTest(worker_lifespan=worker_lifespan), self.assertRaises(ValueError):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=None, n_splits=None, worker_lifespan=worker_lifespan,
                                     progress_bar=False, progress_bar_position=0, task_timeout=None,
                                     worker_init_timeout=None, worker_exit_timeout=None)

    def test_progress_bar_position(self):
        """
        Check progress_bar_position parameter. Should raise when wrong parameter values are used.
        """
        pool_params = WorkerPoolParams(None, None)

        # Should work fine
        for progress_bar_position in [0, 1, 100, int(1e8)]:
            with self.subTest(progress_bar_position=progress_bar_position):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=None, n_splits=None, worker_lifespan=None, progress_bar=False,
                                     progress_bar_position=progress_bar_position, task_timeout=None,
                                     worker_init_timeout=None, worker_exit_timeout=None)

        # progress_bar_position should be an integer
        for progress_bar_position in ['3', {8}, 3.14, None]:
            with self.subTest(progress_bar_position=progress_bar_position), self.assertRaises(TypeError):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=None, n_splits=None, worker_lifespan=None, progress_bar=False,
                                     progress_bar_position=progress_bar_position, task_timeout=None,
                                     worker_init_timeout=None, worker_exit_timeout=None)

        # progress_bar_position should be positive >= 0
        for progress_bar_position in [-1, -5]:
            with self.subTest(progress_bar_position=progress_bar_position), self.assertRaises(ValueError):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=None, n_splits=None, worker_lifespan=None, progress_bar=False,
                                     progress_bar_position=progress_bar_position, task_timeout=None,
                                     worker_init_timeout=None, worker_exit_timeout=None)

    def test_timeout(self):
        """
        Check task_timeout, worker_init_timeout, and worker_exit_timeout. Should raise when wrong parameter values are
        used.
        """
        pool_params = WorkerPoolParams(None, None)

        # Should work fine
        for timeout in [None, 0.5, 1, 100.5, int(1e8)]:
            with self.subTest(task_timeout=timeout):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=None, n_splits=None, worker_lifespan=None, progress_bar=False,
                                     progress_bar_position=0, task_timeout=timeout, worker_init_timeout=None,
                                     worker_exit_timeout=None)
            with self.subTest(worker_init_timeout=timeout):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=None, n_splits=None, worker_lifespan=None, progress_bar=False,
                                     progress_bar_position=0, task_timeout=None, worker_init_timeout=timeout,
                                     worker_exit_timeout=None)
            with self.subTest(worker_exit_timeout=timeout):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=None, n_splits=None, worker_lifespan=None, progress_bar=False,
                                     progress_bar_position=0, task_timeout=None, worker_init_timeout=None,
                                     worker_exit_timeout=timeout)

        # timeout should be an integer, float, or None
        for timeout in ['3', {8}]:
            with self.subTest(task_timeout=timeout), self.assertRaises(TypeError):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=None, n_splits=None, worker_lifespan=None, progress_bar=False,
                                     progress_bar_position=0, task_timeout=timeout, worker_init_timeout=None,
                                     worker_exit_timeout=None)
            with self.subTest(worker_init_timeout=timeout), self.assertRaises(TypeError):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=None, n_splits=None, worker_lifespan=None, progress_bar=False,
                                     progress_bar_position=0, task_timeout=None, worker_init_timeout=timeout,
                                     worker_exit_timeout=None)
            with self.subTest(worker_exit_timeout=timeout), self.assertRaises(TypeError):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=None, n_splits=None, worker_lifespan=None, progress_bar=False,
                                     progress_bar_position=0, task_timeout=None, worker_init_timeout=None,
                                     worker_exit_timeout=timeout)

        # timeout should be positive > 0
        for timeout in [0, -1.337, -5]:
            with self.subTest(task_timeout=timeout), self.assertRaises(ValueError):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=None, n_splits=None, worker_lifespan=None, progress_bar=False,
                                     progress_bar_position=0, task_timeout=timeout, worker_init_timeout=None,
                                     worker_exit_timeout=None)
            with self.subTest(worker_init_timeout=timeout), self.assertRaises(ValueError):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=None, n_splits=None, worker_lifespan=None, progress_bar=False,
                                     progress_bar_position=0, task_timeout=None, worker_init_timeout=timeout,
                                     worker_exit_timeout=None)
            with self.subTest(worker_exit_timeout=timeout), self.assertRaises(ValueError):
                check_map_parameters(pool_params, iterable_of_args=[], iterable_len=None, max_tasks_active=None,
                                     chunk_size=None, n_splits=None, worker_lifespan=None, progress_bar=False,
                                     progress_bar_position=0, task_timeout=None, worker_init_timeout=None,
                                     worker_exit_timeout=timeout)
