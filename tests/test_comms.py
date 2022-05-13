import ctypes
import multiprocessing as mp
import queue
import threading
import unittest
import warnings
from datetime import datetime, timezone
from itertools import product
from unittest.mock import patch

from mpire.comms import NEW_MAP_PARAMS_PILL, NON_LETHAL_POISON_PILL, POISON_PILL, WorkerComms
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

        for n_jobs, ctx in product([1, 2, 4], test_ctx):
            comms = WorkerComms(ctx, n_jobs)
            self.assertEqual(comms.ctx, ctx)
            self.assertEqual(comms.n_jobs, n_jobs)

            event_type = type(ctx.Event())
            lock_type = type(ctx.Lock())

            with self.subTest('__init__ called', n_jobs=n_jobs, ctx=ctx):
                self.assertIsInstance(comms._keep_order, event_type)
                self.assertFalse(comms._keep_order.is_set())
                self.assertIsNone(comms._task_queues)
                self.assertIsNone(comms._task_idx)
                self.assertIsNone(comms._last_completed_task_worker_id)
                self.assertIsNone(comms._results_queue)
                self.assertListEqual(comms._exit_results_queues, [])
                self.assertIsNone(comms._all_exit_results_obtained)
                self.assertIsNone(comms._worker_done_array)
                self.assertIsNone(comms._workers_dead)
                self.assertIsNone(comms._workers_dead_locks)
                self.assertIsNone(comms._workers_time_task_started)
                self.assertIsNone(comms._exception_queue)
                self.assertIsInstance(comms.exception_lock, lock_type)
                self.assertIsInstance(comms._exception_thrown, event_type)
                self.assertIsInstance(comms._kill_signal_received, event_type)
                self.assertFalse(comms._exception_thrown.is_set())
                self.assertFalse(comms._kill_signal_received.is_set())
                self.assertIsNone(comms._tasks_completed_array)
                self.assertIsNone(comms._tasks_completed_locks)
                self.assertIsNone(comms._progress_bar_last_updated)
                self.assertIsNone(comms._progress_bar_shutdown)
                self.assertIsNone(comms._progress_bar_complete)
                self.assertIsNone(comms._all_exit_results_obtained)

            MockDatetimeNow.RETURN_VALUES = [datetime(1970, 1, 1, 0, 0, 0, 0)]
            MockDatetimeNow.CURRENT_IDX = 0

            with self.subTest('without initial values', n_jobs=n_jobs, ctx=ctx), \
                    patch('mpire.comms.datetime', new=MockDatetimeNow):
                comms.init_comms()
                self._check_comms_are_initialized(comms, n_jobs)

            # Set values so we can test if the containers will be properly resetted
            comms._task_idx = 4
            comms._last_completed_task_worker_id = 2
            comms._all_exit_results_obtained.set()
            comms._worker_done_array[:] = [False, True, False, True][:n_jobs]
            [worker_dead.clear() for worker_dead in comms._workers_dead]
            comms._workers_time_task_started[:] = [i + 1 for i in range(n_jobs * 3)]
            comms._exception_thrown.set()
            comms._kill_signal_received.set()
            comms._tasks_completed_array[:] = [i + 1 for i in range(n_jobs)]
            comms._progress_bar_last_updated = 3
            comms._progress_bar_shutdown.set()
            comms._progress_bar_complete.set()

            MockDatetimeNow.CURRENT_IDX = 0

            with self.subTest('with initial values', n_jobs=n_jobs, ctx=ctx), \
                    patch('mpire.comms.datetime', new=MockDatetimeNow):
                comms.init_comms()
                self._check_comms_are_initialized(comms, n_jobs)

    def _check_comms_are_initialized(self, comms: WorkerComms, n_jobs: int) -> None:
        """
        Checks if the WorkerComms have been properly initialized

        :param comms: WorkerComms object
        :param n_jobs: Number of jobs
        """
        event_type = type(comms.ctx.Event())
        lock_type = type(comms.ctx.Lock())
        joinable_queue_type = type(comms.ctx.JoinableQueue())
        array_with_locks_type = type(comms.ctx.Array('i', 0))

        self.assertEqual(len(comms._task_queues), n_jobs)
        for q in comms._task_queues:
            self.assertIsInstance(q, joinable_queue_type)
        self.assertIsNone(comms._last_completed_task_worker_id)
        self.assertIsInstance(comms._results_queue, joinable_queue_type)
        self.assertEqual(len(comms._exit_results_queues), n_jobs)
        for q in comms._exit_results_queues:
            self.assertIsInstance(q, joinable_queue_type)
        self.assertIsInstance(comms._all_exit_results_obtained, event_type)
        self.assertFalse(comms._all_exit_results_obtained.is_set())
        self.assertIsInstance(comms._worker_done_array, ctypes.Array)
        self.assertEqual(len(comms._workers_dead), n_jobs)
        for worker_dead in comms._workers_dead:
            self.assertIsInstance(worker_dead, event_type)
            self.assertTrue(worker_dead.is_set())
        self.assertEqual(len(comms._workers_dead_locks), n_jobs)
        for worker_dead_lock in comms._workers_dead_locks:
            self.assertIsInstance(worker_dead_lock, lock_type)
        self.assertIsInstance(comms._workers_time_task_started, array_with_locks_type)
        self.assertIsInstance(comms._exception_queue, joinable_queue_type)
        self.assertFalse(comms._exception_thrown.is_set())
        self.assertFalse(comms._kill_signal_received.is_set())
        self.assertIsInstance(comms._tasks_completed_array, ctypes.Array)
        self.assertEqual(len(comms._tasks_completed_locks), n_jobs)
        for lock in comms._tasks_completed_locks:
            self.assertIsInstance(lock, lock_type)
        self.assertEqual(comms._progress_bar_last_updated, datetime(1970, 1, 1, 0, 0, 0, 0))
        self.assertIsInstance(comms._progress_bar_shutdown, event_type)
        self.assertFalse(comms._progress_bar_shutdown.is_set())
        self.assertIsInstance(comms._progress_bar_complete, event_type)
        self.assertFalse(comms._progress_bar_complete.is_set())

        # Basic sanity checks for the values
        self.assertEqual(comms._task_idx, 0)
        self.assertEqual(list(comms._worker_done_array), [False for _ in range(n_jobs)])
        self.assertEqual(list(comms._workers_time_task_started), [0.0 for _ in range(n_jobs * 3)])
        self.assertEqual(list(comms._tasks_completed_array), [0 for _ in range(n_jobs)])

    def test_progress_bar(self):
        """
        Test progress bar related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2)
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
        comms._progress_bar_shutdown.clear()

        # Set exception
        comms.signal_exception_thrown()
        self.assertEqual(comms.get_tasks_completed_progress_bar(), POISON_PILL)
        comms._exception_thrown.clear()

        # Set kill signal received
        comms.signal_kill_signal_received()
        self.assertEqual(comms.get_tasks_completed_progress_bar(), POISON_PILL)
        comms._kill_signal_received.clear()

    def test_progress_bar_shutdown(self):
        """
        Test progress bar complete related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2)
        comms.init_comms()

        self.assertFalse(comms._progress_bar_shutdown.is_set())
        comms.signal_progress_bar_shutdown()
        self.assertTrue(comms._progress_bar_shutdown.is_set())

    def test_progress_bar_complete(self):
        """
        Test progress bar complete related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2)
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
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2)

        self.assertFalse(comms.keep_order())
        comms.signal_keep_order()
        self.assertTrue(comms.keep_order())
        comms.clear_keep_order()
        self.assertFalse(comms.keep_order())

    def test_tasks(self):
        """
        Test task related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 3)
        comms.init_comms()

        # Nothing available yet
        for worker_id in range(3):
            with self.assertRaises(queue.Empty):
                comms._task_queues[worker_id].get(block=False)

        # Add a few tasks. As no worker has completed tasks yet, it should give the task to workers in order
        comms.add_task(12)
        comms.add_task('hello world')
        comms.add_task({'foo': 'bar'})
        comms.add_task({'foo': 'baz'})
        comms.add_task(34.43)
        comms.add_task(datetime(2000, 1, 1, 1, 2, 3))
        tasks = []
        for worker_id in [0, 1, 2, 0, 1, 2]:
            tasks.append(comms.get_task(worker_id))
            comms.task_done(worker_id)
        self.assertListEqual(tasks, [12, 'hello world', {'foo': 'bar'}, {'foo': 'baz'}, 34.43,
                                     datetime(2000, 1, 1, 1, 2, 3)])

        # When workers have completed tasks, it depends on the last one who gets the new task. After giving a task to
        # that worker the worker ID that last completed a task is reset again. So, it should continue with the normal
        # order
        comms.init_comms()
        comms.add_task(12)
        comms.add_task('hello world')
        comms.add_task({'foo': 'bar'})
        comms._last_completed_task_worker_id = 2
        comms.add_task({'foo': 'baz'})
        comms._last_completed_task_worker_id = 1
        comms.add_task(34.43)
        comms._last_completed_task_worker_id = 0
        comms.add_task(datetime(2000, 1, 1, 1, 2, 3))
        comms._last_completed_task_worker_id = 2
        comms.add_task('123')
        comms.add_task(123)
        comms.add_task(1337)
        tasks = []
        for worker_id in [0, 1, 2, 2, 1, 0, 2, 0, 1]:
            tasks.append(comms.get_task(worker_id))
            comms.task_done(worker_id)
        self.assertListEqual(tasks, [12, 'hello world', {'foo': 'bar'}, {'foo': 'baz'}, 34.43,
                                     datetime(2000, 1, 1, 1, 2, 3), '123', 123, 1337])

        # Throw in an exception. Should return None
        comms.signal_exception_thrown()
        for worker_id in range(3):
            self.assertIsNone(comms.get_task(worker_id))

        # Should be joinable
        comms.join_task_queues()

    def test_results(self):
        """
        Test results related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2)
        comms.init_comms()

        # Nothing available yet
        with self.assertRaises(queue.Empty):
            comms._results_queue.get(block=False)

        # Add a few results. Note that `get_results` calls `task_done`
        comms.add_results(0, 12)
        comms.add_results(1, 'hello world')
        comms.add_results(1, {'foo': 'bar'})
        comms.add_results(0, '123')
        self.assertEqual(comms.get_results(), 12)
        self.assertEqual(comms._last_completed_task_worker_id, 0)
        self.assertEqual(comms.get_results(), 'hello world')
        self.assertEqual(comms._last_completed_task_worker_id, 1)
        self.assertEqual(comms.get_results(), {'foo': 'bar'})
        self.assertEqual(comms._last_completed_task_worker_id, 1)
        self.assertEqual(comms.get_results(), '123')
        self.assertEqual(comms._last_completed_task_worker_id, 0)

        # Should be joinable. When using keep_alive, it should still be open
        comms.join_results_queues(keep_alive=True)
        comms.add_results(0, 12)
        comms.get_results()
        comms.join_results_queues(keep_alive=False)

        # Depending on Python version it either throws AssertionError or ValueError
        with self.assertRaises((AssertionError, ValueError)):
            comms.add_results(0, 12)

    def test_exit_results(self):
        """
        Test exit results related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 3)
        comms.init_comms()

        # Nothing available yet. Timeout related function should only be called when timeout != None
        with patch.object(comms, 'has_worker_exit_timed_out', side_effect=lambda *_: False) as p:
            for worker_id in range(3):
                with self.assertRaises(queue.Empty):
                    comms.get_exit_results(worker_id, timeout=None, block=False)
            self.assertEqual(p.call_count, 0)

            for worker_id in range(3):
                with self.assertRaises(queue.Empty):
                    comms.get_exit_results(worker_id, timeout=100, block=False)
            self.assertEqual(p.call_count, 3)

        # When timeout is used and function returns True, it should raise a TimeoutError
        with patch.object(comms, 'has_worker_exit_timed_out', side_effect=lambda *_: True):
            for worker_id in range(3):
                with self.assertRaises(TimeoutError):
                    comms.get_exit_results(worker_id, timeout=100, block=False)

        # Add a few results. Note that `get_exit_results` calls `task_done`
        for worker_id in range(3):
            comms.add_exit_results(worker_id, worker_id)
            comms.add_exit_results(worker_id, 'hello world')
            comms.add_exit_results(worker_id, {'foo': 'bar'})
        self.assertListEqual([comms.get_exit_results(worker_id=0,  timeout=None) for _ in range(3)],
                             [0, 'hello world', {'foo': 'bar'}])
        self.assertListEqual([comms.get_exit_results(worker_id=1, timeout=None) for _ in range(3)],
                             [1, 'hello world', {'foo': 'bar'}])
        self.assertListEqual([comms.get_exit_results(worker_id=2, timeout=None) for _ in range(3)],
                             [2, 'hello world', {'foo': 'bar'}])

        # When an exception is thrown it should return directly. However, when block=False it should still get it. We
        # use a while loop because we need to wait until the queue is ready. This block=False trick is used in
        # terminate().
        comms.add_exit_results(0, 'hello world')
        comms.signal_exception_thrown()
        self.assertIsNone(comms.get_exit_results(0, timeout=None))
        while True:
            try:
                self.assertEqual(comms.get_exit_results(0, timeout=None, block=False), 'hello world')
                break
            except queue.Empty:
                pass
        comms._exception_thrown.clear()

        # Should be joinable. When using keep_alive, it should still be open
        comms.join_results_queues(keep_alive=True)
        for worker_id in range(3):
            comms.add_exit_results(worker_id, 'hello world')
            comms.get_exit_results(worker_id, timeout=None)
        comms.join_results_queues(keep_alive=False)

        # Depending on Python version it either throws AssertionError or ValueError
        for worker_id in range(3):
            with self.assertRaises((AssertionError, ValueError)):
                comms.add_exit_results(worker_id, 'hello world')

    def test_all_exit_results_obtained(self):
        """
        Test all exit results obtained related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2)
        comms.init_comms()

        self.assertFalse(comms._all_exit_results_obtained.is_set())
        comms.signal_all_exit_results_obtained()
        self.assertTrue(comms._all_exit_results_obtained.is_set())

        with patch.object(comms._all_exit_results_obtained, 'wait') as p:
            comms.wait_until_all_exit_results_obtained()
            self.assertEqual(p.call_count, 1)

    def test_add_new_map_params(self):
        """
        Test new map parameters function
        """
        comms = WorkerComms(MP_CONTEXTS['mp_dill'][DEFAULT_START_METHOD], 2)
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

    def test_exceptions(self):
        """
        Test exception related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2)
        comms.init_comms()

        # Nothing available yet
        with self.assertRaises(queue.Empty):
            comms._exception_queue.get(block=False)

        # Add a few exceptions
        comms.add_exception(TypeError, 'TypeError')
        comms.add_exception(ValueError, 'ValueError')
        comms.add_exception(RuntimeError, 'RuntimeError')
        self.assertListEqual([comms.get_exception() for _ in range(3)],
                             [(TypeError, 'TypeError'), (ValueError, 'ValueError'), (RuntimeError, 'RuntimeError')])
        [comms.task_done_exception() for _ in range(3)]

        # Add poison pill
        comms.add_exception_poison_pill()
        self.assertEqual(comms.get_exception(), (POISON_PILL, POISON_PILL))
        comms.task_done_exception()

        # Should be joinable. When using keep_alive, it should still be open
        comms.join_exception_queue(keep_alive=True)
        comms.add_exception(TypeError, 'TypeError')
        comms.get_exception()
        comms.task_done_exception()
        comms.join_exception_queue(keep_alive=False)

        # Depending on Python version it either throws AssertionError or ValueError
        with self.assertRaises((AssertionError, ValueError)):
            comms.add_exception(TypeError, 'TypeError')

    def test_exception_thrown(self):
        """
        Test exception thrown related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2)

        self.assertFalse(comms.exception_thrown())
        comms.signal_exception_thrown()
        self.assertTrue(comms.exception_thrown())
        comms._exception_thrown.clear()
        self.assertFalse(comms.exception_thrown())

    def test_kill_signal_received(self):
        """
        Test kill signal received related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2)

        self.assertFalse(comms.kill_signal_received())
        comms.signal_kill_signal_received()
        self.assertTrue(comms.kill_signal_received())
        comms._kill_signal_received.clear()
        self.assertFalse(comms.kill_signal_received())

    def test_worker_poison_pill(self):
        """
        Test that a poison pill is inserted for every worker
        """
        for n_jobs in [1, 2, 4]:
            with self.subTest(n_jobs=n_jobs):
                comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], n_jobs)
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
                comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], n_jobs)
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
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 5)
        comms.init_comms()

        # No restarts yet
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
        self.assertListEqual(list(comms.get_worker_restarts()), [])

    def test_worker_alive(self):
        """
        Test worker alive related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 5)
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

        # We test wait by simply checking the call count
        for worker_id in range(5):
            with patch.object(comms._workers_dead[worker_id], 'wait') as p:
                comms.wait_for_dead_worker(worker_id)
                self.assertEqual(p.call_count, 1)

    def test_drain_queues_terminate_worker(self):
        """
        get_results should be called once, get_exit_results should be called when exit function is defined
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 5)
        dont_wait_event = threading.Event()
        dont_wait_event.set()

        comms.init_comms()
        for worker_id in range(5):
            with self.subTest(worker_id=worker_id), \
                    patch.object(comms, 'get_results', side_effect=comms.get_results) as p1, \
                    patch.object(comms, 'get_exit_results', side_effect=comms.get_exit_results) as p2:
                comms.drain_queues_terminate_worker(worker_id, dont_wait_event)
                self.assertEqual(p1.call_count, 1)
                self.assertEqual(p2.call_count, 1)
                self.assertEqual(p2.call_args_list[0][0][0], worker_id)
                self.assertTrue(dont_wait_event.is_set())

    def test_drain_queues(self):
        """
        _drain_and_join_queue should be called for every queue that matters. There are as many tasks queues as workers,
        1 results queue, if a worker_exit function has been provided as many exit results queues as workers, and if a
        progress bar has been enabled one queu for the progress bar.
        """
        for n_jobs in [1, 2, 4]:
            comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], n_jobs)
            comms.init_comms()
            with self.subTest(n_jobs=n_jobs), patch.object(comms, 'drain_and_join_queue') as p:
                comms.drain_queues()
                self.assertEqual(p.call_count, n_jobs + 1 + n_jobs)

    def test__drain_and_join_queue(self):
        """
        Test draining queues
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2)

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
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 5)
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
                    self.assertListEqual(comms._workers_time_task_started[worker_id * 3: worker_id * 3 + 3],
                                         [0.0, 0.0, 0.0])
                    comms.signal_worker_init_started(worker_id)
                    comms.signal_worker_task_started(worker_id)
                    comms.signal_worker_exit_started(worker_id)
                    self.assertListEqual(comms._workers_time_task_started[worker_id * 3: worker_id * 3 + 3],
                                         [86400.0, 172800.0, 259200.0])

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
                for timeout, has_timed_out in [ (8, True), (9, True), (10, False), (11, False)]:
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
                self.assertListEqual(comms._workers_time_task_started[worker_id * 3: worker_id * 3 + 3],
                                     [0.0, 172800.0, 259200.0])
                comms.signal_worker_task_completed(worker_id)
                self.assertListEqual(comms._workers_time_task_started[worker_id * 3: worker_id * 3 + 3],
                                     [0.0, 0.0, 259200.0])
                comms.signal_worker_exit_completed(worker_id)
                self.assertListEqual(comms._workers_time_task_started[worker_id * 3: worker_id * 3 + 3],
                                     [0.0, 0.0, 0.0])

        # Check timeouts. Should be False because nothing started
        for worker_id in range(5):
            for timeout in [0, 0.1, 3]:
                with self.subTest(worker_id=worker_id, timeout=timeout):
                    self.assertFalse(comms.has_worker_init_timed_out(worker_id, timeout))
