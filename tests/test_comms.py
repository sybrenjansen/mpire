import ctypes
import multiprocessing as mp
import queue
import threading
import unittest
import warnings
from collections import deque
from datetime import datetime, timezone
from itertools import product
from unittest.mock import patch

from mpire.comms import MAIN_PROCESS, NEW_MAP_PARAMS_PILL, NON_LETHAL_POISON_PILL, POISON_PILL, WorkerComms
from mpire.context import DEFAULT_START_METHOD, FORK_AVAILABLE, MP_CONTEXTS
from mpire.params import WorkerMapParams
from tests.utils import MockDatetimeNow


def _f1():
    pass


def _f2():
    pass


class WorkerCommsTest(unittest.TestCase):

    def test_init_comms(self):
        """
        Test if initializing/resetting the comms is done properly
        """
        test_ctx = [MP_CONTEXTS['mp_dill']['spawn'], MP_CONTEXTS['threading']]
        if FORK_AVAILABLE:
            test_ctx.extend([MP_CONTEXTS['mp_dill']['fork'], MP_CONTEXTS['mp']['forkserver']])

        for ctx, n_jobs, order_tasks in product(test_ctx, [1, 2, 4], [False, True]):
            comms = WorkerComms(ctx, n_jobs, order_tasks)
            with self.subTest('__init__ called', ctx=ctx, n_jobs=n_jobs, order_tasks=order_tasks):
                condition_type = type(ctx.Condition(ctx.Lock()))
                event_type = type(ctx.Event())
                lock_type = type(ctx.Lock())
                value_type = type(ctx.Value('i', 0, lock=True))
                self.assertEqual(comms.ctx, ctx)
                self.assertEqual(comms.n_jobs, n_jobs)
                self.assertEqual(comms.order_tasks, order_tasks)
                self.assertFalse(comms.is_initialized())
                self.assertIsInstance(comms._keep_order, value_type)
                self.assertFalse(comms._keep_order.value)
                self.assertIsInstance(comms._task_queues, list)
                self.assertEqual(len(comms._task_queues), 0)
                self.assertIsNone(comms._task_idx)
                self.assertIsInstance(comms._worker_running_task_locks, list)
                self.assertEqual(len(comms._worker_running_task_locks), 0)
                self.assertIsInstance(comms._worker_running_task, list)
                self.assertEqual(len(comms._worker_running_task), 0)
                self.assertIsInstance(comms._last_completed_task_worker_id, deque)
                self.assertEqual(len(comms._last_completed_task_worker_id), 0)
                self.assertIsNone(comms._worker_working_on_job)
                self.assertIsNone(comms._results_queue)
                self.assertIsInstance(comms._results_added, list)
                self.assertEqual(len(comms._results_added), 0)
                self.assertIsNone(comms._results_received)
                self.assertIsNone(comms._worker_restart_array)
                self.assertIsInstance(comms._worker_restart_condition, condition_type)
                self.assertIsNone(comms._workers_dead)
                self.assertIsInstance(comms._workers_time_task_started, list)
                self.assertEqual(len(comms._workers_time_task_started), 0)
                self.assertIsInstance(comms.exception_lock, lock_type)
                self.assertIsInstance(comms._exception_thrown, event_type)
                self.assertIsNone(comms._exception_job_id)
                self.assertIsInstance(comms._kill_signal_received, value_type)
                self.assertFalse(comms._exception_thrown.is_set())
                self.assertFalse(comms._kill_signal_received.value)
                self.assertIsNone(comms._tasks_completed_array)
                self.assertIsNone(comms._progress_bar_last_updated)
                self.assertIsNone(comms._progress_bar_shutdown)
                self.assertIsNone(comms._progress_bar_complete)

            MockDatetimeNow.RETURN_VALUES = [datetime(1970, 1, 1, 0, 0, 0, 0)]
            MockDatetimeNow.CURRENT_IDX = 0

            with self.subTest('without initial values', ctx=ctx, n_jobs=n_jobs, order_tasks=order_tasks), \
                    patch('mpire.comms.datetime', new=MockDatetimeNow):
                comms.init_comms()
                self._check_comms_are_initialized(comms, n_jobs)

            # Set values so we can test if the containers will be properly resetted
            comms._task_idx = 4
            comms._last_completed_task_worker_id.append(2)
            comms._last_completed_task_worker_id.append(1)
            comms._last_completed_task_worker_id.append(2)
            for i in range(n_jobs):
                comms._worker_running_task[i].value = i % 2 == 0
            for i in range(n_jobs):
                comms._worker_working_on_job[i] = i + 1
            comms._results_added = [i + 1 for i in range(n_jobs)]
            for i in range(n_jobs):
                comms._results_received[i] = i + 1
            for i in range(n_jobs):
                comms._worker_restart_array[i] = i % 2 == 0
            comms._workers_dead[:] = [False] * n_jobs
            for i in range(n_jobs):
                for j in range(3):
                    comms._workers_time_task_started[i][j] = i + j + 1
            comms._exception_thrown.set()
            comms._exception_job_id = 89
            comms._kill_signal_received.value = True
            for i in range(n_jobs):
                comms._tasks_completed_array[i] = i + 1
            comms._progress_bar_last_updated = 3
            comms._progress_bar_shutdown.value = True
            comms._progress_bar_complete.set()
            comms.reset()

            MockDatetimeNow.CURRENT_IDX = 0

            with self.subTest('with initial values', ctx=ctx, n_jobs=n_jobs, order_tasks=order_tasks), \
                    patch('mpire.comms.datetime', new=MockDatetimeNow):
                comms.init_comms()
                self._check_comms_are_initialized(comms, n_jobs)

            # Reset initialized flag
            comms.reset()
            self.assertFalse(comms.is_initialized())

    def _check_comms_are_initialized(self, comms: WorkerComms, n_jobs: int) -> None:
        """
        Checks if the WorkerComms have been properly initialized

        :param comms: WorkerComms object
        :param n_jobs: Number of jobs
        """
        array_type = type(comms.ctx.Array('i', n_jobs, lock=True))
        event_type = type(comms.ctx.Event())
        joinable_queue_type = type(comms.ctx.JoinableQueue())
        rlock_type = type(comms.ctx.RLock())
        value_type = type(comms.ctx.Value('i', 0, lock=True))
        value_without_lock_type = type(comms.ctx.Value(ctypes.c_bool, 0, lock=False))

        self.assertEqual(len(comms._task_queues), n_jobs)
        for q in comms._task_queues:
            self.assertIsInstance(q, joinable_queue_type)
        self.assertIsInstance(comms._last_completed_task_worker_id, deque)
        self.assertEqual(len(comms._last_completed_task_worker_id), 0)
        self.assertEqual(len(comms._worker_running_task_locks), n_jobs)
        for l in comms._worker_running_task_locks:
            self.assertIsInstance(l, rlock_type)
        self.assertEqual(len(comms._worker_running_task), n_jobs)
        for v in comms._worker_running_task:
            self.assertIsInstance(v, value_without_lock_type)
        self.assertIsInstance(comms._worker_working_on_job, array_type)
        self.assertEqual(len(comms._worker_working_on_job), n_jobs)
        self.assertIsInstance(comms._results_queue, joinable_queue_type)
        self.assertIsInstance(comms._results_added, list)
        self.assertEqual(len(comms._results_added), n_jobs)
        self.assertIsInstance(comms._results_received, array_type)
        self.assertEqual(len(comms._results_received), n_jobs)
        self.assertIsInstance(comms._worker_restart_array, array_type)
        self.assertEqual(len(comms._worker_restart_array), n_jobs)
        self.assertIsInstance(comms._workers_dead, array_type)
        self.assertEqual(len(comms._workers_dead), n_jobs)
        self.assertEqual(len(comms._workers_time_task_started), n_jobs)
        for v in comms._workers_time_task_started:
            self.assertIsInstance(v, array_type)
        self.assertFalse(comms._exception_thrown.is_set())
        self.assertIsInstance(comms._exception_job_id, value_type)
        self.assertEqual(comms._exception_job_id.value, 0)
        self.assertFalse(comms._kill_signal_received.value)
        self.assertIsInstance(comms._tasks_completed_array, array_type)
        self.assertEqual(len(comms._tasks_completed_array), n_jobs)
        self.assertEqual(comms._progress_bar_last_updated, datetime(1970, 1, 1, 0, 0, 0, 0))
        self.assertIsInstance(comms._progress_bar_shutdown, value_type)
        self.assertFalse(comms._progress_bar_shutdown.value)
        self.assertIsInstance(comms._progress_bar_complete, event_type)
        self.assertFalse(comms._progress_bar_complete.is_set())
        self.assertTrue(comms.is_initialized())

        # Basic sanity checks for the values
        self.assertEqual(comms._task_idx, 0)
        self.assertEqual([v.value for v in comms._worker_running_task], [False] * n_jobs)
        self.assertEqual(comms._worker_working_on_job[:], [0] * n_jobs)
        self.assertEqual(comms._results_received[:], [0] * n_jobs)
        self.assertEqual(comms._worker_restart_array[:], [False] * n_jobs)
        self.assertEqual(comms._workers_dead[:], [True] * n_jobs)
        for i in range(n_jobs):
            self.assertEqual(comms._workers_time_task_started[i][:], [0.0, 0.0, 0.0])
        self.assertEqual(comms._tasks_completed_array[:], [0] * n_jobs)

    def test_progress_bar(self):
        """
        Test progress bar related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2, False)
        comms.init_comms()

        # Nothing available yet
        self.assertEqual(sum(comms._tasks_completed_array), 0)

        MockDatetimeNow.RETURN_VALUES = [datetime(1970, 1, 1, 0, 0, 0, 0),
                                         datetime(1970, 1, 1, 0, 0, 0, 0),
                                         datetime(1970, 1, 1, 0, 0, 0, 0),
                                         datetime(1970, 1, 1, 0, 0, 0, 0)]
        MockDatetimeNow.CURRENT_IDX = 0

        # 3 task done, but not enough time has passed to send the update
        last_updated = datetime(1970, 1, 1, 0, 0, 0, 0)
        n_tasks_completed = 0
        with patch('mpire.comms.datetime', new=MockDatetimeNow):
            for n in range(1, 4):
                last_updated, n_tasks_completed = comms.task_completed_progress_bar(0, last_updated, n_tasks_completed,
                                                                                    force_update=False)
                self.assertEqual(n_tasks_completed, n)
        self.assertEqual(sum(comms._tasks_completed_array), 0)

        # Not enough time has passed, but we'll force the update. Number of tasks done should still be 3
        with patch('mpire.comms.datetime', new=MockDatetimeNow):
            last_updated, n_tasks_completed = comms.task_completed_progress_bar(0, last_updated, n_tasks_completed,
                                                                                force_update=True)
        self.assertEqual(comms.get_tasks_completed_progress_bar(), 3)
        self.assertEqual(last_updated, datetime(1970, 1, 1, 0, 0, 0, 0))
        self.assertEqual(n_tasks_completed, 0)

        # 4 tasks already done and another 4 tasks done. Enough time should've passed for each update call, except the
        # second. In total we have 3 (from above) + 4 + 4 = 11 tasks done
        MockDatetimeNow.RETURN_VALUES = [datetime(1970, 1, 1, 0, 1, 0, 0),
                                         datetime(1970, 1, 1, 0, 1, 0, 0),
                                         datetime(1970, 1, 1, 0, 3, 0, 0),
                                         datetime(1970, 1, 1, 0, 4, 0, 0)]
        MockDatetimeNow.CURRENT_IDX = 0
        last_updated = datetime(1970, 1, 1, 0, 0, 0, 0)
        n_tasks_completed = 4
        with patch('mpire.comms.datetime', new=MockDatetimeNow):
            for _ in range(4):
                last_updated, n_tasks_completed = comms.task_completed_progress_bar(0, last_updated, n_tasks_completed,
                                                                                    force_update=False)
                self.assertEqual(last_updated, MockDatetimeNow.RETURN_VALUES[MockDatetimeNow.CURRENT_IDX - 1])
        self.assertEqual(comms.get_tasks_completed_progress_bar(), 11)
        self.assertEqual(last_updated, datetime(1970, 1, 1, 0, 4, 0, 0))
        self.assertEqual(n_tasks_completed, 0)

        # Signal shutdown
        comms.signal_progress_bar_shutdown()
        self.assertEqual(comms.get_tasks_completed_progress_bar(), POISON_PILL)
        comms._progress_bar_shutdown.value = False

        # Set exception
        comms.signal_exception_thrown(MAIN_PROCESS)
        self.assertEqual(comms.get_tasks_completed_progress_bar(), POISON_PILL)
        comms._exception_thrown.clear()

        # Set kill signal received
        comms.signal_kill_signal_received()
        self.assertEqual(comms.get_tasks_completed_progress_bar(), POISON_PILL)
        comms._kill_signal_received.value = False

    def test_progress_bar_shutdown(self):
        """
        Test progress bar complete related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2, False)
        comms.init_comms()

        self.assertFalse(comms._progress_bar_shutdown.value)
        comms.signal_progress_bar_shutdown()
        self.assertTrue(comms._progress_bar_shutdown.value)

    def test_progress_bar_complete(self):
        """
        Test progress bar complete related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2, False)
        comms.init_comms()

        self.assertFalse(comms._progress_bar_complete.is_set())
        comms.signal_progress_bar_complete()
        self.assertTrue(comms._progress_bar_complete.is_set())

        with patch.object(comms._progress_bar_complete, 'wait') as p:
            comms.wait_until_progress_bar_is_complete()
            self.assertEqual(p.call_count, 1)

    def test_keep_order(self):
        """
        Test keep_order related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2, False)

        self.assertFalse(comms.keep_order())
        comms.signal_keep_order()
        self.assertTrue(comms.keep_order())
        comms.clear_keep_order()
        self.assertFalse(comms.keep_order())

    def test_tasks(self):
        """
        Test task related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 3, False)
        comms.init_comms()

        # Nothing available yet
        for worker_id in range(3):
            with self.assertRaises(queue.Empty):
                comms._task_queues[worker_id].get(block=False)

        # Add a few tasks. As no worker has completed tasks yet, it should give the task to workers in order
        job_id = 0
        comms.add_task(job_id, 12)
        comms.add_task(job_id, 'hello world')
        comms.add_task(job_id, {'foo': 'bar'})
        comms.add_task(job_id, {'foo': 'baz'})
        comms.add_task(job_id, 34.43)
        comms.add_task(job_id, datetime(2000, 1, 1, 1, 2, 3))
        tasks = []
        for worker_id in [0, 1, 2, 0, 1, 2]:
            tasks.append(comms.get_task(worker_id))
            comms.task_done(worker_id)
        self.assertListEqual(tasks, [(job_id, task) for task in
                                     [12, 'hello world', {'foo': 'bar'}, {'foo': 'baz'}, 34.43,
                                      datetime(2000, 1, 1, 1, 2, 3)]])

        # When order_tasks is set it should give the tasks to the workers in the order they were added, independent of
        # workers that have completed tasks. If set to False and workers have completed tasks, it depends on the last
        # one who gets the new task. After giving a task to that worker the worker ID that last completed a task is
        # reset again. So, it should continue with the normal order
        job_id = 1
        for order_tasks, worker_order in [(False, [0, 1, 2, 2, 1, 0, 2, 0, 1]),
                                          (True, [0, 1, 2, 0, 1, 2, 0, 1, 2])]:
            with self.subTest(order_tasks=order_tasks):
                comms.order_tasks = order_tasks
                comms.init_comms()
                comms.add_task(job_id, 12)
                comms.add_task(job_id, 'hello world')
                comms.add_task(job_id, {'foo': 'bar'})
                comms._last_completed_task_worker_id.append(2)
                comms.add_task(job_id, {'foo': 'baz'})
                comms._last_completed_task_worker_id.append(1)
                comms.add_task(job_id, 34.43)
                comms._last_completed_task_worker_id.append(0)
                comms.add_task(job_id, datetime(2000, 1, 1, 1, 2, 3))
                comms._last_completed_task_worker_id.append(2)
                comms.add_task(job_id, '123')
                comms.add_task(job_id, 123)
                comms.add_task(job_id, 1337)
                tasks = []
                for worker_id in worker_order:
                    tasks.append(comms.get_task(worker_id))
                    comms.task_done(worker_id)
                self.assertListEqual(tasks, [(job_id, task) for task in
                                             [12, 'hello world', {'foo': 'bar'}, {'foo': 'baz'}, 34.43,
                                              datetime(2000, 1, 1, 1, 2, 3), '123', 123, 1337]])

        # Throw in an exception. Should return None
        comms.signal_exception_thrown(job_id)
        for worker_id in range(3):
            self.assertIsNone(comms.get_task(worker_id))

        # Should be joinable
        comms.join_task_queues()

    def test_worker_running_task(self):
        """
        Tests that the worker_running_task functions work as expected
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 3, False)
        comms.init_comms()

        comms.set_worker_running_task(0, True)
        comms.set_worker_running_task(1, False)
        comms.set_worker_running_task(2, True)

        self.assertTrue(comms.get_worker_running_task(0))
        self.assertFalse(comms.get_worker_running_task(1))
        self.assertTrue(comms.get_worker_running_task(2))

        comms.set_worker_running_task(0, False)
        comms.set_worker_running_task(1, True)

        self.assertFalse(comms.get_worker_running_task(0))
        self.assertTrue(comms.get_worker_running_task(1))
        self.assertTrue(comms.get_worker_running_task(2))

    def test_worker_working_on_job(self):
        """
        Tests that the worker_working_on_job function works as expected
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 3, False)
        comms.init_comms()

        comms.signal_worker_working_on_job(0, 1)
        comms.signal_worker_working_on_job(1, 8)
        comms.signal_worker_working_on_job(2, 19)

        self.assertEqual(comms.get_worker_working_on_job(0), 1)
        self.assertEqual(comms.get_worker_working_on_job(1), 8)
        self.assertEqual(comms.get_worker_working_on_job(2), 19)

        comms.signal_worker_working_on_job(1, 3)
        comms.signal_worker_working_on_job(2, 0)
        comms.signal_worker_working_on_job(0, -1)

        self.assertEqual(comms.get_worker_working_on_job(0), -1)
        self.assertEqual(comms.get_worker_working_on_job(1), 3)
        self.assertEqual(comms.get_worker_working_on_job(2), 0)

    def test_results(self):
        """
        Test results related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2, False)
        comms.init_comms()

        # Nothing available yet
        with self.assertRaises(queue.Empty):
            comms._results_queue.get(block=False)

        # Add a few results. Note that `get_results` calls `task_done`
        comms.add_results(0, [(0, True, 12)])
        comms.add_results(1, [(1, True, 'hello world')])
        comms.add_results(1, [(1, False, {'foo': 'bar'})])
        comms.add_results(0, [(2, True, '123')])
        self.assertEqual(comms.get_results(), [(0, True, 12)])
        self.assertEqual(comms._last_completed_task_worker_id.popleft(), 0)
        self.assertEqual(comms.get_results(), [(1, True, 'hello world')])
        self.assertEqual(comms._last_completed_task_worker_id.popleft(), 1)
        self.assertEqual(comms.get_results(), [(1, False, {'foo': 'bar'})])
        self.assertEqual(comms._last_completed_task_worker_id.popleft(), 1)
        self.assertEqual(comms.get_results(), [(2, True, '123')])
        self.assertEqual(comms._last_completed_task_worker_id.popleft(), 0)

        # Should be joinable. When using keep_alive, it should still be open
        comms.join_results_queues(keep_alive=True)
        comms.add_results(0, [(2, True, 12)])
        comms.get_results()
        comms.join_results_queues(keep_alive=False)

        # Depending on Python version it either throws AssertionError or ValueError
        with self.assertRaises((AssertionError, ValueError)):
            comms.add_results(0, [(2, True, 12)])

    def test_add_new_map_params(self):
        """
        Test new map parameters function
        """
        comms = WorkerComms(MP_CONTEXTS['mp_dill'][DEFAULT_START_METHOD], 2, False)
        comms.init_comms()

        map_params = WorkerMapParams(_f1, None, None, 1)
        comms.add_new_map_params(map_params)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for worker_id in range(2):
                self.assertEqual(comms.get_task(worker_id), NEW_MAP_PARAMS_PILL)
                self.assertEqual(comms.get_task(worker_id), map_params)
                comms.task_done(worker_id)
                comms.task_done(worker_id)

        map_params = WorkerMapParams(_f1, _f2, None, None)
        comms.add_new_map_params(map_params)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for worker_id in range(2):
                self.assertEqual(comms.get_task(worker_id), NEW_MAP_PARAMS_PILL)
                self.assertEqual(comms.get_task(worker_id), map_params)
                comms.task_done(worker_id)
                comms.task_done(worker_id)

    def test_exception_thrown(self):
        """
        Test exception thrown related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2, False)
        comms.init_comms()

        self.assertFalse(comms.exception_thrown())
        comms.signal_exception_thrown(0)
        self.assertTrue(comms.exception_thrown())
        self.assertEqual(comms.exception_thrown_by(), 0)
        comms._exception_thrown.clear()
        self.assertFalse(comms.exception_thrown())
        comms.signal_exception_thrown(13)
        self.assertTrue(comms.exception_thrown())
        self.assertEqual(comms.exception_thrown_by(), 13)

    def test_kill_signal_received(self):
        """
        Test kill signal received related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2, False)

        self.assertFalse(comms.kill_signal_received())
        comms.signal_kill_signal_received()
        self.assertTrue(comms.kill_signal_received())
        comms._kill_signal_received.value = False
        self.assertFalse(comms.kill_signal_received())

    def test_worker_poison_pill(self):
        """
        Test that a poison pill is inserted for every worker
        """
        for n_jobs in [1, 2, 4]:
            with self.subTest(n_jobs=n_jobs):
                comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], n_jobs, False)
                comms.init_comms()
                comms.insert_poison_pill()
                for worker_id in range(n_jobs):
                    self.assertEqual(comms.get_task(worker_id), POISON_PILL)
                    comms.task_done(worker_id)
                comms.join_task_queues()

    def test_worker_non_lethal_poison_pill(self):
        """
        Test that a non-lethal poison pill is inserted for every worker
        """
        # Shouldn't be the same thing
        self.assertNotEqual(POISON_PILL, NON_LETHAL_POISON_PILL)

        for n_jobs in [1, 2, 4]:
            with self.subTest(n_jobs=n_jobs):
                comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], n_jobs, False)
                comms.init_comms()
                comms.insert_non_lethal_poison_pill()
                for worker_id in range(n_jobs):
                    self.assertEqual(comms.get_task(worker_id), NON_LETHAL_POISON_PILL)
                    comms.task_done(worker_id)
                comms.join_task_queues()

    def test_worker_restart(self):
        """
        Test worker restart related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 5, False)
        comms.init_comms()

        # No restarts yet
        with patch.object(comms._worker_restart_condition, 'wait'):
            self.assertListEqual(list(comms.get_worker_restarts()), [])

        # Signal some restarts
        comms.signal_worker_restart(0)
        comms.signal_worker_restart(2)
        comms.signal_worker_restart(3)

        # Restarts available
        self.assertListEqual(list(comms.get_worker_restarts()), [0, 2, 3])

        # Reset some
        comms.reset_worker_restart(0)
        comms.reset_worker_restart(3)

        # Restarts available
        self.assertListEqual(list(comms.get_worker_restarts()), [2])

        # Reset last one
        comms.reset_worker_restart(2)
        with patch.object(comms._worker_restart_condition, 'wait'):
            self.assertListEqual(list(comms.get_worker_restarts()), [])

    def test_worker_alive(self):
        """
        Test worker alive related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 5, False)
        comms.init_comms()

        # Signal some workers are alive
        comms.signal_worker_alive(0)
        comms.signal_worker_alive(1)
        comms.signal_worker_dead(1)
        comms.signal_worker_alive(2)
        comms.signal_worker_alive(3)

        # Check alive status
        self.assertListEqual([comms.is_worker_alive(worker_id) for worker_id in range(5)],
                             [True, False, True, True, False])

        # Reset some
        comms.signal_worker_dead(0)
        comms.signal_worker_dead(3)

        # Check alive status
        self.assertListEqual([comms.is_worker_alive(worker_id) for worker_id in range(5)],
                             [False, False, True, False, False])

    def test_drain_result_queue_terminate_worker(self):
        """
        get_results should be called once, get_exit_results should be called when exit function is defined
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 5, False)
        comms.init_comms()
        dont_wait_event = threading.Event()
        dont_wait_event.set()

        with patch.object(comms, 'get_results', side_effect=comms.get_results) as p:
            comms.drain_results_queue_terminate_worker(dont_wait_event)
            self.assertEqual(p.call_count, 1)
            self.assertTrue(dont_wait_event.is_set())

    def test_drain_queues(self):
        """
        _drain_and_join_queue should be called for every queue that matters. There are as many tasks queues as workers
        and 1 results queue
        """
        for n_jobs in [1, 2, 4]:
            comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], n_jobs, False)
            comms.init_comms()
            with self.subTest(n_jobs=n_jobs), patch.object(comms, 'drain_and_join_queue') as p:
                comms.drain_queues()
                self.assertEqual(p.call_count, n_jobs + 1)

    def test__drain_and_join_queue(self):
        """
        Test draining queues
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2, False)

        # Create a custom queue with some data
        q = mp.JoinableQueue()
        q.put(1)
        q.put('hello')
        q.put('world')

        # Drain queue. It should now be closed
        comms._drain_and_join_queue(q)

        # Depending on Python version it either throws OSError or ValueError
        with self.assertRaises((OSError, ValueError)):
            q.get(block=False)

    def test_timeouts(self):
        """
        Tests timeout related function
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 5, False)
        comms.init_comms()

        MockDatetimeNow.RETURN_VALUES = [datetime(1970, 1, 2, 0, 0, 0, 0, tzinfo=timezone.utc),
                                         datetime(1970, 1, 3, 0, 0, 0, 0, tzinfo=timezone.utc),
                                         datetime(1970, 1, 4, 0, 0, 0, 0, tzinfo=timezone.utc)]
        MockDatetimeNow.CURRENT_IDX = 0

        # Signal workers started
        with patch('mpire.comms.datetime', new=MockDatetimeNow):
            for worker_id in range(5):
                with self.subTest(worker_id=worker_id):
                    MockDatetimeNow.CURRENT_IDX = 0
                    self.assertListEqual(comms._workers_time_task_started[worker_id][:], [0.0, 0.0, 0.0])
                    comms.signal_worker_init_started(worker_id)
                    comms.signal_worker_task_started(worker_id)
                    comms.signal_worker_exit_started(worker_id)
                    self.assertListEqual(comms._workers_time_task_started[worker_id][:], [86400.0, 172800.0, 259200.0])

        MockDatetimeNow.RETURN_VALUES = [datetime(1970, 1, 2, 0, 0, 10, 0, tzinfo=timezone.utc),
                                         datetime(1970, 1, 3, 0, 0, 9, 0, tzinfo=timezone.utc),
                                         datetime(1970, 1, 4, 0, 0, 8, 0, tzinfo=timezone.utc)]

        # Check timeouts
        with patch('mpire.comms.datetime', new=MockDatetimeNow):
            for worker_id in range(5):
                # worker_init, times out at > 10
                for timeout, has_timed_out in [(8, True), (9, True), (10, True), (11, False)]:
                    with self.subTest(timeout=timeout, worker_id=worker_id):
                        MockDatetimeNow.CURRENT_IDX = 0
                        self.assertEqual(comms.has_worker_init_timed_out(worker_id, timeout), has_timed_out)

                # task, times out at > 9
                for timeout, has_timed_out in [(8, True), (9, True), (10, False), (11, False)]:
                    with self.subTest(timeout=timeout, worker_id=worker_id):
                        MockDatetimeNow.CURRENT_IDX = 1
                        self.assertEqual(comms.has_worker_task_timed_out(worker_id, timeout), has_timed_out)

                # worker_exit, times out at > 8
                for timeout, has_timed_out in [(8, True), (9, False), (10, False), (11, False)]:
                    with self.subTest(timeout=timeout, worker_id=worker_id):
                        MockDatetimeNow.CURRENT_IDX = 2
                        self.assertEqual(comms.has_worker_exit_timed_out(worker_id, timeout), has_timed_out)

        # Reset
        for worker_id in range(5):
            with self.subTest(worker_id=worker_id):
                comms.signal_worker_init_completed(worker_id)
                self.assertListEqual(comms._workers_time_task_started[worker_id][:], [0.0, 172800.0, 259200.0])
                comms.signal_worker_task_completed(worker_id)
                self.assertListEqual(comms._workers_time_task_started[worker_id][:], [0.0, 0.0, 259200.0])
                comms.signal_worker_exit_completed(worker_id)
                self.assertListEqual(comms._workers_time_task_started[worker_id][:], [0.0, 0.0, 0.0])

        # Check timeouts. Should be False because nothing started
        for worker_id in range(5):
            for timeout in [0, 0.1, 3]:
                with self.subTest(worker_id=worker_id, timeout=timeout):
                    self.assertFalse(comms.has_worker_init_timed_out(worker_id, timeout))
