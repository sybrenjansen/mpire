import unittest
import warnings
from functools import partial
from itertools import product
from unittest.mock import patch

import numpy as np
import pytest
from tqdm import TqdmKeyError

from mpire import cpu_count
from mpire.params import check_map_parameters, WorkerMapParams, WorkerPoolParams, get_number_of_tasks, check_number, \
    check_progress_bar_options


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
            if cpu_ids is not None and np.array(cpu_ids).max(initial=0) >= cpu_count():
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


class GetNumberOfTasksTest(unittest.TestCase):

    def test_get_number_of_tasks(self):
        """
        Test that the number of tasks is correctly derived
        """
        with self.subTest('iterable_len is provided'):
            self.assertEqual(get_number_of_tasks([], 100), 100)
        with self.subTest('iterable_len is not provided, __len__ implemented'):
            self.assertEqual(get_number_of_tasks([1, 2, 3], None), 3)
        with self.subTest('iterable_len is not provided, __len__ not implemented'):
            self.assertIsNone(get_number_of_tasks((x for x in []), None))


class CheckNumberTest(unittest.TestCase):

    def test_check_number(self):
        """
        Test that the check_number function works as expected
        """
        with self.subTest('correct type'):
            check_number(1, 'var', (int, float), False)
        with self.subTest('wrong type'), self.assertRaises(TypeError):
            check_number(1, 'var', (float,), False)
        with self.subTest('None allowed'):
            check_number(None, 'var', (int, float), True)
        with self.subTest('None not allowed'), self.assertRaises(TypeError):
            check_number(None, 'var', (int, float), False)
        with self.subTest('min_ provided'):
            check_number(1, 'var', (int, float), False, 0)
        with self.subTest('min_ provided, but not satisfied'), self.assertRaises(ValueError):
            check_number(1, 'var', (int, float), False, 2)


class CheckProgressBarOptions(unittest.TestCase):

    @pytest.mark.filterwarnings('ignore::pytest.PytestUnraisableExceptionWarning')
    def test_check_progress_bar_options(self):
        """
        Check progress_bar_options parameter. Should raise when wrong parameter values are used.
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            # Should work fine. We're ignoring warnings
            for progress_bar_options in [{}, {"position": 0}, {"desc": "hello", "total": 100},
                                         {"unit": "seconds", "mininterval": 0.1}]:
                with self.subTest(progress_bar_options=progress_bar_options):
                    returned_progress_bar_options = check_progress_bar_options(progress_bar_options, None, None, None)
                    self.assertEqual({k: None if k == "total" else returned_progress_bar_options[k]
                                      for k in progress_bar_options.keys()}, progress_bar_options)

            # progress_bar_options should be a dictionary
            for progress_bar_options in ['hello', {8}, 3.14]:
                with self.subTest(progress_bar_options=progress_bar_options), self.assertRaises(TypeError):
                    check_progress_bar_options(progress_bar_options, None, None, None)

            # When a non-existent parameter is passed, it should raise an error
            with self.assertRaises(TqdmKeyError):
                check_progress_bar_options({"non_existent_param": "hello"}, None, None, None)

            # When a parameter is passed with a wrong type, it should raise an error. Testing other parameters causes
            # deadlocks in other threading tests, which on their own run just fine. It's a tqdm thing which I don't
            # intend to investigate any further (I tried ...)
            for progress_bar_options in [{"position": "hello"}]:
                with self.subTest(progress_bar_options=progress_bar_options), self.assertRaises(TypeError):
                    check_progress_bar_options(progress_bar_options, None, None, None)

            # The total and leave options should be overwritten
            for total, leave in [(1, False), (100, True)]:
                returned_progress_bar_options = check_progress_bar_options({"total": 3, "leave": leave}, None, total,
                                                                           None)
                self.assertEqual(returned_progress_bar_options["total"], total)
                self.assertTrue(returned_progress_bar_options["leave"])

            # Some parameters have a default value
            for param, value in [("position", 3), ("dynamic_ncols", False), ("mininterval", 0.5), ("maxinterval", 0.1)]:
                returned_progress_bar_options = check_progress_bar_options({param: value}, None, None, None)
                self.assertEqual(returned_progress_bar_options[param], value)
                for other_param, expected_value in [("position", 0), ("dynamic_ncols", True),
                                                    ("mininterval", 0.1), ("maxinterval", 0.5)]:
                    if param != other_param:
                        self.assertEqual(returned_progress_bar_options[other_param], expected_value)

    def test_progress_bar_position(self):
        """
        Check progress_bar_position parameter. Should raise when wrong parameter values are used.
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            # Should work fine
            for progress_bar_position, expected_position in [(None, 0), (0, 0), (1, 1),
                                                             (100, 100), (int(1e8), int(1e8))]:
                with self.subTest(progress_bar_position=progress_bar_position):
                    returned_progress_bar_options = check_progress_bar_options(None, progress_bar_position, None, None)
                    self.assertEqual(returned_progress_bar_options['position'], expected_position)

            # progress_bar_position should be an integer
            for progress_bar_position in ['3', {8}, 3.14]:
                with self.subTest(progress_bar_position=progress_bar_position), self.assertRaises(TypeError):
                    check_progress_bar_options(None, progress_bar_position, None, None)

            # progress_bar_position should be positive >= 0
            for progress_bar_position in [-1, -5]:
                with self.subTest(progress_bar_position=progress_bar_position), self.assertRaises(ValueError):
                    check_progress_bar_options(None, progress_bar_position, None, None)

    def test_progress_bar_style(self):
        """
        Check progress_bar_style parameter. Should raise when wrong parameter values are used.
        """
        for progress_bar_style in [None, 'std', 'notebook']:
            with self.subTest(progress_bar_style=progress_bar_style):
                check_progress_bar_options(None, None, None, progress_bar_style)

        for progress_bar_style in [-1, 'rich', {}]:
            with self.subTest(progress_bar_style=progress_bar_style), self.assertRaises(ValueError):
                check_progress_bar_options(None, None, None, progress_bar_style)


class CheckMapParametersTest(unittest.TestCase):

    def setUp(self) -> None:
        # Set some defaults
        self.pool_params = WorkerPoolParams(3, None)
        self.check_map_parameters_func = partial(
            check_map_parameters, pool_params=self.pool_params, iterable_of_args=[], iterable_len=None,
            max_tasks_active=None, chunk_size=None, n_splits=None, worker_lifespan=None, progress_bar=False,
            progress_bar_position=None, progress_bar_options=None, progress_bar_style=None, task_timeout=None,
            worker_init_timeout=None, worker_exit_timeout=None
        )

    def test_n_tasks(self):
        """
        Should raise when wrong parameter values are used
        """
        pool_params = WorkerPoolParams(None, None)

        # Get number of tasks
        with self.subTest('get n_tasks', iterable_of_args=range(100)):
            n_tasks, *_ = self.check_map_parameters_func(iterable_of_args=range(100), iterable_len=None)
            self.assertEqual(n_tasks, 100)
        with self.subTest('get n_tasks, __len__ implemented', iterable_len=100):
            n_tasks, *_ = self.check_map_parameters_func(iterable_of_args=[1, 2, 3], iterable_len=100)
            self.assertEqual(n_tasks, 100)
        with self.subTest('get n_tasks, __len__ implemented', iterable_len=None):
            n_tasks, *_ = self.check_map_parameters_func(iterable_of_args=[1, 2, 3], iterable_len=None)
            self.assertEqual(n_tasks, 3)

    def test_chunk_size(self):
        """
        When chunk_size is provided, it should be used. Otherwise, if n_splits is used and the number of tasks is known,
        we use chunk_size=n_tasks/n_splits. If n_splits is not provided, it is set to 4 if the number of tasks can't be
        determined, or to n_tasks / (n_jobs * 64) when the number of tasks is known.
        """
        with self.subTest("check_number call"), patch('mpire.params.check_number') as p:
            self.check_map_parameters_func(chunk_size=10)
            chunk_size_call = [call for call in p.call_args_list if call[0][1] == 'chunk_size'][0]
            args, kwargs = chunk_size_call[0], chunk_size_call[1]
            self.assertEqual(args[0], 10)
            self.assertDictEqual(kwargs, {"allowed_types": (int, float), "none_allowed": True, "min_": 1})

        with self.subTest("chunk_size provided", chunk_size=10):
            _, _, chunk_size, *_ = self.check_map_parameters_func(chunk_size=10)
            self.assertEqual(chunk_size, 10)
        with self.subTest("chunk_size and n_splits not provided, n_tasks provided", chunk_size=None, n_splits=None,
                          n_tasks=11), \
                patch('mpire.params.get_number_of_tasks', side_effect=[11]):
            _, _, chunk_size, *_ = self.check_map_parameters_func(chunk_size=None, n_splits=None)
            self.assertEqual(chunk_size, 11 / (3 * 64))
        with self.subTest("chunk_size and n_splits not provided, n_tasks not provided", chunk_size=None, n_splits=None,
                          n_tasks=None), \
                patch('mpire.params.get_number_of_tasks', side_effect=[None]), \
                warnings.catch_warnings():
            warnings.simplefilter("ignore")
            _, _, chunk_size, *_ = self.check_map_parameters_func(chunk_size=None, n_splits=None)
            self.assertEqual(chunk_size, 4)
        with self.subTest("chunk_size not provided, n_splits provided, n_tasks not provided", chunk_size=None,
                          n_splits=11, n_tasks=None), \
                patch('mpire.params.get_number_of_tasks', side_effect=[None]), \
                warnings.catch_warnings():
            warnings.simplefilter("ignore")
            _, _, chunk_size, *_ = self.check_map_parameters_func(chunk_size=None, n_splits=None)
            self.assertEqual(chunk_size, 4)
        with self.subTest("chunk_size not provided, n_splits provided, n_tasks provided", chunk_size=None, n_splits=11,
                          n_tasks=22), \
                patch('mpire.params.get_number_of_tasks', side_effect=[22]):
            _, _, chunk_size, *_ = self.check_map_parameters_func(chunk_size=None, n_splits=11)
            self.assertEqual(chunk_size, 2)

    def test_n_splits(self):
        """
        Check n_splits parameter. The actual usage of n_splits is tested in test_chunk_size
        """
        with patch('mpire.params.check_number') as p:
            self.check_map_parameters_func(n_splits=11)
            n_splits_call = [call for call in p.call_args_list if call[0][1] == 'n_splits'][0]
            args, kwargs = n_splits_call[0], n_splits_call[1]
            self.assertEqual(args[0], 11)
            self.assertDictEqual(kwargs, {"allowed_types": (int,), "none_allowed": True, "min_": 1})

    def test_max_tasks_active(self):
        """
        Check max_tasks_active parameter. Should raise when wrong parameter values are used.
        """
        with self.subTest("check_number call"), patch('mpire.params.check_number') as p:
            self.check_map_parameters_func(max_tasks_active=12)
            max_tasks_active_call = [call for call in p.call_args_list if call[0][1] == 'max_tasks_active'][0]
            args, kwargs = max_tasks_active_call[0], max_tasks_active_call[1]
            self.assertEqual(args[0], 12)
            self.assertDictEqual(kwargs, {"allowed_types": (int,), "none_allowed": True, "min_": 1})

        # When max_active_tasks is None, it should be set to n_jobs * ceil(chunk_size) * 2
        for n_jobs, chunk_size, expected_max_tasks_active in [(1, 10, 20), (2, 1.8, 8), (4, 3.14, 32)]:
            with self.subTest("max_active_tasks is None", n_jobs=n_jobs):
                pool_params = WorkerPoolParams(n_jobs, None)
                _, max_tasks_active, *_ = self.check_map_parameters_func(pool_params=pool_params, max_tasks_active=None,
                                                                         chunk_size=chunk_size)
                self.assertEqual(max_tasks_active, expected_max_tasks_active)

    def test_worker_lifespan(self):
        """
        Check worker_lifespan parameter. Should raise when wrong parameter values are used.
        """
        with patch('mpire.params.check_number') as p:
            self.check_map_parameters_func(worker_lifespan=11)
            worker_lifespan_call = [call for call in p.call_args_list if call[0][1] == 'worker_lifespan'][0]
            args, kwargs = worker_lifespan_call[0], worker_lifespan_call[1]
            self.assertEqual(args[0], 11)
            self.assertDictEqual(kwargs, {"allowed_types": (int,), "none_allowed": True, "min_": 1})

    def test_windows_threading_progress_bar_error(self):
        """
        Check that when a progress bar is requested on windows when threading is used, a ValueError is raised.
        """
        with patch('mpire.params.RUNNING_WINDOWS', side_effect=True), self.assertRaises(ValueError):
            pool_params = WorkerPoolParams(2, None, start_method='threading')
            self.check_map_parameters_func(pool_params=pool_params, progress_bar=True)

    def test_timeout(self):
        """
        Check task_timeout, worker_init_timeout, and worker_exit_timeout. Should raise when wrong parameter values are
        used.
        """
        # Should work fine
        for timeout in [None, 0.5, 1, 100.5, int(1e8)]:
            with self.subTest(task_timeout=timeout), patch('mpire.params.check_number') as p:
                self.check_map_parameters_func(task_timeout=timeout)
                task_timeout_call = [call for call in p.call_args_list if call[0][1] == 'task_timeout'][0]
                args, kwargs = task_timeout_call[0], task_timeout_call[1]
                self.assertEqual(args[0], timeout)
                self.assertDictEqual(kwargs, {"allowed_types": (int, float), "none_allowed": True, "min_": 1e-8})
            with self.subTest(worker_init_timeout=timeout), patch('mpire.params.check_number') as p:
                self.check_map_parameters_func(worker_init_timeout=timeout)
                init_timeout_call = [call for call in p.call_args_list if call[0][1] == 'worker_init_timeout'][0]
                args, kwargs = init_timeout_call[0], init_timeout_call[1]
                self.assertEqual(args[0], timeout)
                self.assertDictEqual(kwargs, {"allowed_types": (int, float), "none_allowed": True, "min_": 1e-8})
            with self.subTest(worker_exit_timeout=timeout), patch('mpire.params.check_number') as p:
                self.check_map_parameters_func(worker_exit_timeout=timeout)
                exit_timeout_call = [call for call in p.call_args_list if call[0][1] == 'worker_exit_timeout'][0]
                args, kwargs = exit_timeout_call[0], exit_timeout_call[1]
                self.assertEqual(args[0], timeout)
                self.assertDictEqual(kwargs, {"allowed_types": (int, float), "none_allowed": True, "min_": 1e-8})

        # timeout should be an integer, float, or None
        for timeout in ['3', {8}]:
            with self.subTest(task_timeout=timeout), self.assertRaises(TypeError):
                self.check_map_parameters_func(task_timeout=timeout)
            with self.subTest(worker_init_timeout=timeout), self.assertRaises(TypeError):
                self.check_map_parameters_func(worker_init_timeout=timeout)
            with self.subTest(worker_exit_timeout=timeout), self.assertRaises(TypeError):
                self.check_map_parameters_func(worker_exit_timeout=timeout)

        # timeout should be positive > 0
        for timeout in [0, -1.337, -5]:
            with self.subTest(task_timeout=timeout), self.assertRaises(ValueError):
                self.check_map_parameters_func(task_timeout=timeout)
            with self.subTest(worker_init_timeout=timeout), self.assertRaises(ValueError):
                self.check_map_parameters_func(worker_init_timeout=timeout)
            with self.subTest(worker_exit_timeout=timeout), self.assertRaises(ValueError):
                self.check_map_parameters_func(worker_exit_timeout=timeout)
