import ctypes
import multiprocessing as mp
import unittest
from datetime import datetime
from multiprocessing import managers
from time import sleep
from unittest.mock import patch

from mpire import WorkerPool
from mpire.context import DEFAULT_START_METHOD
from mpire.insights import RUNNING_WINDOWS, WorkerInsights
from tests.utils import MockDatetimeNow


def square(x):
    # time.sleep is added for Windows compatibility, otherwise it says 0.0 time has passed
    sleep(0.001)
    return x * x


class WorkerInsightsTest(unittest.TestCase):

    def test_reset_insights(self):
        """
        Test if resetting the insights is done properly
        """
        for n_jobs in [1, 2, 4]:
            insights = WorkerInsights(mp.get_context(DEFAULT_START_METHOD), n_jobs)
            self.assertEqual(insights.ctx, mp.get_context(DEFAULT_START_METHOD))
            self.assertEqual(insights.n_jobs, n_jobs)

            with self.subTest('initialized', n_jobs=n_jobs):
                self.assertFalse(insights.insights_enabled)
                self.assertIsNone(insights.insights_manager)
                self.assertIsNone(insights.insights_manager_lock)
                self.assertIsNone(insights.worker_start_up_time)
                self.assertIsNone(insights.worker_init_time)
                self.assertIsNone(insights.worker_n_completed_tasks)
                self.assertIsNone(insights.worker_waiting_time)
                self.assertIsNone(insights.worker_working_time)
                self.assertIsNone(insights.worker_exit_time)
                self.assertIsNone(insights.max_task_duration)
                self.assertIsNone(insights.max_task_args)

            # Containers should be properly initialized
            MockDatetimeNow.RETURN_VALUES = [datetime(1970, 1, 1, 1, 2, 3, 4)]
            MockDatetimeNow.CURRENT_IDX = 0
            with self.subTest('without initial values', n_jobs=n_jobs, enable_insights=True), \
                    patch('mpire.insights.datetime', new=MockDatetimeNow):
                insights.reset_insights(enable_insights=True)
                self.assertTrue(insights.insights_enabled)
                self.assertIsInstance(insights.insights_manager_lock, mp.synchronize.Lock)
                if RUNNING_WINDOWS:
                    self.assertIsNone(insights.insights_manager)
                else:
                    self.assertIsInstance(insights.insights_manager, managers.SyncManager)
                self.assertIsInstance(insights.worker_start_up_time, ctypes.Array)
                self.assertIsInstance(insights.worker_init_time, ctypes.Array)
                self.assertIsInstance(insights.worker_n_completed_tasks, ctypes.Array)
                self.assertIsInstance(insights.worker_waiting_time, ctypes.Array)
                self.assertIsInstance(insights.worker_working_time, ctypes.Array)
                self.assertIsInstance(insights.worker_exit_time, ctypes.Array)
                self.assertIsInstance(insights.max_task_duration, ctypes.Array)
                self.assertIsInstance(insights.max_task_args, list if RUNNING_WINDOWS else managers.ListProxy)

                # Basic sanity checks for the values
                self.assertEqual(sum(insights.worker_start_up_time), 0)
                self.assertEqual(sum(insights.worker_init_time), 0)
                self.assertEqual(sum(insights.worker_n_completed_tasks), 0)
                self.assertEqual(sum(insights.worker_waiting_time), 0)
                self.assertEqual(sum(insights.worker_working_time), 0)
                self.assertEqual(sum(insights.worker_exit_time), 0)
                self.assertEqual(sum(insights.max_task_duration), 0)
                if not RUNNING_WINDOWS:
                    self.assertListEqual(list(insights.max_task_args), [''] * n_jobs * 5)

            # Set some values so we can test if the containers will be properly resetted
            insights.worker_start_up_time[0] = 1
            insights.worker_init_time[0] = 2
            insights.worker_n_completed_tasks[0] = 3
            insights.worker_waiting_time[0] = 4
            insights.worker_working_time[0] = 5
            insights.worker_exit_time[0] = 6
            insights.max_task_duration[0] = 7
            insights.max_task_args[0] = '8'

            # Containers should be properly initialized
            MockDatetimeNow.CURRENT_IDX = 0
            with self.subTest('with initial values', n_jobs=n_jobs, enable_insights=True), \
                    patch('mpire.insights.datetime', new=MockDatetimeNow):
                insights.reset_insights(enable_insights=True)
                # Basic sanity checks for the values
                self.assertEqual(sum(insights.worker_start_up_time), 0)
                self.assertEqual(sum(insights.worker_init_time), 0)
                self.assertEqual(sum(insights.worker_n_completed_tasks), 0)
                self.assertEqual(sum(insights.worker_waiting_time), 0)
                self.assertEqual(sum(insights.worker_working_time), 0)
                self.assertEqual(sum(insights.worker_exit_time), 0)
                self.assertEqual(sum(insights.max_task_duration), 0)
                self.assertListEqual(list(insights.max_task_args), [''] * n_jobs * 5)

            # Disabling should set things to None again
            with self.subTest(n_jobs=n_jobs, enable_insights=False):
                insights.reset_insights(enable_insights=False)
                self.assertFalse(insights.insights_enabled)
                self.assertIsNone(insights.insights_manager)
                self.assertIsNone(insights.insights_manager_lock)
                self.assertIsNone(insights.worker_start_up_time)
                self.assertIsNone(insights.worker_init_time)
                self.assertIsNone(insights.worker_n_completed_tasks)
                self.assertIsNone(insights.worker_waiting_time)
                self.assertIsNone(insights.worker_working_time)
                self.assertIsNone(insights.worker_exit_time)
                self.assertIsNone(insights.max_task_duration)
                self.assertIsNone(insights.max_task_args)

    def test_enable_insights(self):
        """
        Insight containers are initially set to None values. When enabled they should be changed to appropriate
        containers. When a second task is started it should reset them. If disabled, they should remain None
        """
        with WorkerPool(n_jobs=2, enable_insights=True) as pool:

            # We run this a few times to see if it resets properly. We only verify this by checking the
            # n_completed_tasks
            for idx in range(3):
                with self.subTest('enabled', idx=idx):

                    pool.map(square, self._get_tasks(10), worker_init=self._init, worker_exit=self._exit)

                    # Basic sanity checks for the values. Some max task args can be empty, in that case the duration
                    # should be 0 (= no data)
                    self.assertGreater(sum(pool._worker_insights.worker_start_up_time), 0)
                    self.assertGreater(sum(pool._worker_insights.worker_init_time), 0)
                    self.assertEqual(sum(pool._worker_insights.worker_n_completed_tasks), 10)
                    self.assertGreater(sum(pool._worker_insights.worker_waiting_time), 0)
                    self.assertGreater(sum(pool._worker_insights.worker_working_time), 0)
                    self.assertGreater(sum(pool._worker_insights.worker_exit_time), 0)
                    self.assertGreater(max(pool._worker_insights.max_task_duration), 0)
                    for duration, args in zip(pool._worker_insights.max_task_duration,
                                              pool._worker_insights.max_task_args):
                        if duration == 0:
                            self.assertEqual(args, '')
                        elif not RUNNING_WINDOWS:
                            self.assertIn(args, {'Arg 0: 0', 'Arg 0: 1', 'Arg 0: 2', 'Arg 0: 3', 'Arg 0: 4',
                                                 'Arg 0: 5', 'Arg 0: 6', 'Arg 0: 7', 'Arg 0: 8', 'Arg 0: 9'})

        with WorkerPool(n_jobs=2, enable_insights=False) as pool:

            # Disabling should set things to None again
            with self.subTest('disable'):
                pool.map(square, range(10))
                self.assertIsNone(pool._worker_insights.insights_manager)
                self.assertIsNone(pool._worker_insights.insights_manager_lock)
                self.assertIsNone(pool._worker_insights.worker_start_up_time)
                self.assertIsNone(pool._worker_insights.worker_init_time)
                self.assertIsNone(pool._worker_insights.worker_n_completed_tasks)
                self.assertIsNone(pool._worker_insights.worker_waiting_time)
                self.assertIsNone(pool._worker_insights.worker_working_time)
                self.assertIsNone(pool._worker_insights.worker_exit_time)
                self.assertIsNone(pool._worker_insights.max_task_duration)
                self.assertIsNone(pool._worker_insights.max_task_args)

    def test_get_max_task_duration_list(self):
        """
        Test that the right containers are selected given a worker ID
        """
        insights = WorkerInsights(mp.get_context(DEFAULT_START_METHOD), n_jobs=5)

        with self.subTest(insights_enabled=False):
            for worker_id in range(5):
                self.assertIsNone(insights.get_max_task_duration_list(worker_id))

        with self.subTest(insights_enabled=True):
            insights.reset_insights(enable_insights=True)
            insights.max_task_duration[:] = range(25)
            insights.max_task_args[:] = map(str, range(25))

            for worker_id, expected_task_duration_list in [
                (0, [(0.0, '0'), (1.0, '1'), (2.0, '2'), (3.0, '3'), (4.0, '4')]),
                (1, [(5.0, '5'), (6.0, '6'), (7.0, '7'), (8.0, '8'), (9.0, '9')]),
                (4, [(20.0, '20'), (21.0, '21'), (22.0, '22'), (23.0, '23'), (24.0, '24')])
            ]:
                with self.subTest(worker_id=worker_id):
                    self.assertListEqual(insights.get_max_task_duration_list(worker_id), expected_task_duration_list)

    def test_update_start_up_time(self):
        """
        Test that the start up time is correctly added to worker_start_up_time for the right index
        """
        MockDatetimeNow.RETURN_VALUES = [datetime(1970, 1, 1, 0, 0, 0, 0),
                                         datetime(1970, 1, 1, 0, 0, 2, 0),
                                         datetime(1970, 1, 1, 0, 0, 3, 0),
                                         datetime(1970, 1, 1, 0, 0, 7, 0),
                                         datetime(1970, 1, 1, 0, 0, 8, 0)]
        MockDatetimeNow.CURRENT_IDX = 0

        insights = WorkerInsights(mp.get_context(DEFAULT_START_METHOD), n_jobs=5)

        # Shouldn't do anything when insights haven't been enabled
        with self.subTest(insights_enabled=False), patch('mpire.insights.datetime', new=MockDatetimeNow):
            for worker_id in range(5):
                insights.update_start_up_time(worker_id, datetime(1970, 1, 1, 0, 0, 0, 0))
            self.assertIsNone(insights.worker_start_up_time)

        insights.reset_insights(enable_insights=True)
        MockDatetimeNow.CURRENT_IDX = 0

        with self.subTest(insights_enabled=True), patch('mpire.insights.datetime', new=MockDatetimeNow):
            for worker_id in range(5):
                insights.update_start_up_time(worker_id, datetime(1970, 1, 1, 0, 0, 0, 0))
            self.assertListEqual(list(insights.worker_start_up_time), [0, 2, 3, 7, 8])

    def test_update_n_completed_tasks(self):
        """
        Test that the number of completed tasks is correctly added to worker_n_completed_tasks for the right index
        """
        insights = WorkerInsights(mp.get_context(DEFAULT_START_METHOD), n_jobs=5)

        # Shouldn't do anything when insights haven't been enabled
        with self.subTest(insights_enabled=False):
            for worker_id, call_n_times in [(0, 1), (1, 0), (2, 5), (3, 8), (4, 11)]:
                for _ in range(call_n_times):
                    insights.update_n_completed_tasks(worker_id)
            self.assertIsNone(insights.worker_n_completed_tasks)

        with self.subTest(insights_enabled=True):
            insights.reset_insights(enable_insights=True)
            for worker_id, call_n_times in [(0, 1), (1, 0), (2, 5), (3, 8), (4, 11)]:
                for _ in range(call_n_times):
                    insights.update_n_completed_tasks(worker_id)
            self.assertListEqual(list(insights.worker_n_completed_tasks), [1, 0, 5, 8, 11])

    def test_update_task_insights_not_forced(self):
        """
        Test whether the update_task_insights is triggered correctly when ``force_update=False``.
        """
        MockDatetimeNow.RETURN_VALUES = [datetime(1970, 1, 1, 0, 0, 0, 0),
                                         datetime(1970, 1, 1, 0, 0, 2, 0),
                                         datetime(1970, 1, 1, 0, 0, 3, 0),
                                         datetime(1970, 1, 1, 0, 0, 7, 0),
                                         datetime(1970, 1, 1, 0, 0, 8, 0)]
        MockDatetimeNow.CURRENT_IDX = 0

        insights = WorkerInsights(mp.get_context(DEFAULT_START_METHOD), n_jobs=5)
        max_task_duration_last_updated = datetime(1970, 1, 1, 0, 0, 1, 0)

        # Shouldn't do anything when insights haven't been enabled
        with self.subTest(insights_enabled=False), patch('mpire.insights.datetime', new=MockDatetimeNow):
            for worker_id in range(5):
                max_task_duration_list = insights.get_max_task_duration_list(worker_id)
                insights.update_task_insights(worker_id, max_task_duration_last_updated, max_task_duration_list,
                                              force_update=False)
            self.assertIsNone(insights.max_task_duration)
            self.assertIsNone(insights.max_task_args)

        insights.reset_insights(enable_insights=True)
        max_task_duration_last_updated = datetime(1970, 1, 1, 0, 0, 1, 0)
        MockDatetimeNow.CURRENT_IDX = 0

        # The first three worker IDs won't send an update because the two seconds hasn't passed yet.
        with self.subTest(insights_enabled=True), patch('mpire.insights.datetime', new=MockDatetimeNow):
            last_updated_times = []
            for worker_id, max_task_duration_list in [
                (0, [(0.1, '0'), (0.2, '1'), (0.3, '2'), (0.4, '3'), (0.5, '4')]),
                (1, [(1.1, '0'), (1.2, '1'), (1.3, '2'), (1.4, '3'), (1.5, '4')]),
                (2, [(2.1, '0'), (2.2, '1'), (2.3, '2'), (2.4, '3'), (2.5, '4')]),
                (3, [(3.1, '0'), (3.2, '1'), (3.3, '2'), (3.4, '3'), (3.5, '4')]),
                (4, [(4.1, '0'), (4.2, '1'), (4.3, '2'), (4.4, '3'), (4.5, '4')])
            ]:
                last_updated_times.append(insights.update_task_insights(
                    worker_id, max_task_duration_last_updated, max_task_duration_list, force_update=False
                ))
            self.assertListEqual(last_updated_times, [datetime(1970, 1, 1, 0, 0, 1), datetime(1970, 1, 1, 0, 0, 1),
                                                      datetime(1970, 1, 1, 0, 0, 1), datetime(1970, 1, 1, 0, 0, 7),
                                                      datetime(1970, 1, 1, 0, 0, 8)])
            self.assertListEqual(list(insights.max_task_duration), [0.0, 0.0, 0.0, 0.0, 0.0,
                                                                    0.0, 0.0, 0.0, 0.0, 0.0,
                                                                    0.0, 0.0, 0.0, 0.0, 0.0,
                                                                    3.1, 3.2, 3.3, 3.4, 3.5,
                                                                    4.1, 4.2, 4.3, 4.4, 4.5])
            self.assertListEqual(list(insights.max_task_args), ['', '', '', '', '',
                                                                '', '', '', '', '',
                                                                '', '', '', '', '',
                                                                '0', '1', '2', '3', '4',
                                                                '0', '1', '2', '3', '4'])

    def test_update_task_insights_forced(self):
        """
        Test whether task insights are being updated correctly
        """
        MockDatetimeNow.RETURN_VALUES = [datetime(1970, 1, 1, 0, 0, 1, 0),
                                         datetime(1970, 1, 1, 0, 0, 2, 0)]
        MockDatetimeNow.CURRENT_IDX = 0

        insights = WorkerInsights(mp.get_context(DEFAULT_START_METHOD), n_jobs=2)
        max_task_duration_last_updated = datetime(1970, 1, 1, 0, 0, 0, 0)

        # Shouldn't do anything when insights haven't been enabled
        with self.subTest(insights_enabled=False), patch('mpire.insights.datetime', new=MockDatetimeNow):
            for worker_id in range(2):
                max_task_duration_last_updated = insights.update_task_insights(
                    worker_id, max_task_duration_last_updated, [(0.1, '1'), (0.2, '2')], force_update=True
                )
            self.assertIsNone(insights.max_task_duration)
            self.assertIsNone(insights.max_task_args)
            self.assertEqual(max_task_duration_last_updated, datetime(1970, 1, 1, 0, 0, 0, 0))

        insights.reset_insights(enable_insights=True)
        max_task_duration_last_updated = datetime(1970, 1, 1, 0, 0, 0, 0)
        MockDatetimeNow.CURRENT_IDX = 0

        with self.subTest(insights_enabled=True), patch('mpire.insights.datetime', new=MockDatetimeNow):
            for worker_id, max_task_duration_list in [(0, [(5, '5'), (6, '6'), (7, '7'), (8, '8'), (9, '9')]),
                                                      (1, [(0, '0'), (1, '1'), (2, '2'), (3, '3'), (4, '4')])]:
                self.assertEqual(insights.update_task_insights(
                    worker_id, max_task_duration_last_updated, max_task_duration_list, force_update=True
                ), datetime(1970, 1, 1, 0, 0, worker_id + 1, 0))
            self.assertListEqual(list(insights.max_task_duration), [5, 6, 7, 8, 9, 0, 1, 2, 3, 4])
            self.assertListEqual(list(insights.max_task_args), ['5', '6', '7', '8', '9', '0', '1', '2', '3', '4'])

    def test_get_insights(self):
        """
        Test if the insights are properly processed
        """
        insights = WorkerInsights(mp.get_context(DEFAULT_START_METHOD), n_jobs=2)

        with self.subTest(enable_insights=False):
            insights.reset_insights(enable_insights=False)
            self.assertDictEqual(insights.get_insights(), {})

        with self.subTest(enable_insights=True):
            insights.reset_insights(enable_insights=True)
            insights.worker_start_up_time[:] = [0.1, 0.2]
            insights.worker_init_time[:] = [0.11, 0.22]
            insights.worker_n_completed_tasks[:] = [2, 3]
            insights.worker_waiting_time[:] = [0.4, 0.3]
            insights.worker_working_time[:] = [42.0, 37.0]
            insights.worker_exit_time[:] = [0.33, 0.44]

            # Durations that are zero or args that are empty are skipped
            insights.max_task_duration[:] = [0.0, 0.0, 1.0, 2.0, 0.0, 6.0, 0.8, 0.0, 0.1, 0.0]
            insights.max_task_args[:] = ['', '', '1', '2', '', '3', '4', '', '5', '']
            insights_dict = insights.get_insights()

            # Test ratios separately because of rounding errors
            total_time = 0.3 + 0.33 + 0.7 + 79.0 + 0.77
            self.assertAlmostEqual(insights_dict['start_up_ratio'], 0.3 / total_time)
            self.assertAlmostEqual(insights_dict['init_ratio'], 0.33 / total_time)
            self.assertAlmostEqual(insights_dict['waiting_ratio'], 0.7 / total_time)
            self.assertAlmostEqual(insights_dict['working_ratio'], 79.0 / total_time)
            self.assertAlmostEqual(insights_dict['exit_ratio'], 0.77 / total_time)
            del (insights_dict['start_up_ratio'], insights_dict['init_ratio'], insights_dict['waiting_ratio'],
                 insights_dict['working_ratio'], insights_dict['exit_ratio'])

            self.assertDictEqual(insights_dict, {
                'n_completed_tasks': [2, 3],
                'start_up_time': ['0:00:00.100', '0:00:00.200'],
                'init_time': ['0:00:00.110', '0:00:00.220'],
                'waiting_time': ['0:00:00.400', '0:00:00.300'],
                'working_time': ['0:00:42', '0:00:37'],
                'exit_time': ['0:00:00.330', '0:00:00.440'],
                'total_start_up_time': '0:00:00.300',
                'total_init_time': '0:00:00.330',
                'total_waiting_time': '0:00:00.700',
                'total_working_time': '0:01:19',
                'total_exit_time': '0:00:00.770',
                'top_5_max_task_durations': ['0:00:06', '0:00:02', '0:00:01', '0:00:00.800', '0:00:00.100'],
                'top_5_max_task_args': ['', '', '', '', ''] if RUNNING_WINDOWS else ['3', '2', '1', '4', '5'],
                'total_time': '0:01:21.100',
                'start_up_time_mean': '0:00:00.150', 'start_up_time_std': '0:00:00.050',
                'init_time_mean': '0:00:00.165', 'init_time_std': '0:00:00.055',
                'waiting_time_mean': '0:00:00.350', 'waiting_time_std': '0:00:00.050',
                'working_time_mean': '0:00:39.500', 'working_time_std': '0:00:02.500',
                'exit_time_mean': '0:00:00.385', 'exit_time_std': '0:00:00.055'
            })

    @staticmethod
    def _get_tasks(n):
        """ Simulate that getting tasks takes some time """
        for i in range(n):
            # sleep is added for Windows compatibility, otherwise it says 0.0 time has passed
            sleep(0.001)
            yield i

    @staticmethod
    def _init():
        # sleep is added for Windows compatibility, otherwise it says 0.0 time has passed
        sleep(0.001)

    @staticmethod
    def _exit():
        # sleep is added for Windows compatibility, otherwise it says 0.0 time has passed
        sleep(0.001)
