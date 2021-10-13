import _thread
import ctypes
import multiprocess as mp_dill
import multiprocessing as mp
import multiprocessing.synchronize
import queue
import threading
import unittest
import warnings
from datetime import datetime
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
        test_ctx = [(MP_CONTEXTS['mp_dill']['spawn'], True, False, MP_CONTEXTS['mp_dill'][DEFAULT_START_METHOD]),
                    (MP_CONTEXTS['threading'], False, True, MP_CONTEXTS['mp'][DEFAULT_START_METHOD])]
        if FORK_AVAILABLE:
            test_ctx.extend([(MP_CONTEXTS['mp_dill']['fork'], True, False, MP_CONTEXTS['mp_dill']['fork']),
                             (MP_CONTEXTS['mp']['forkserver'], False, False, MP_CONTEXTS['mp']['fork'])])

        for n_jobs, (ctx, use_dill, using_threading, expected_ctx_for_threading) in product([1, 2, 4], test_ctx):
            comms = WorkerComms(ctx, n_jobs, use_dill, using_threading)
            self.assertEqual(comms.ctx, ctx)
            self.assertEqual(comms.ctx_for_threading, expected_ctx_for_threading)
            self.assertEqual(comms.n_jobs, n_jobs)
            self.assertEqual(comms.using_threading, using_threading)

            expected_mp = mp_dill if use_dill else mp

            with self.subTest('__init__ called', n_jobs=n_jobs, using_threading=using_threading):
                self.assertIsInstance(comms._keep_order, type(ctx.Event()))
                self.assertFalse(comms._keep_order.is_set())
                self.assertIsNone(comms._task_queues)
                self.assertIsNone(comms._task_idx)
                self.assertIsNone(comms._last_completed_task_worker_id)
                self.assertIsNone(comms._results_queue)
                self.assertListEqual(comms._exit_results_queues, [])
                self.assertIsNone(comms._all_exit_results_obtained)
                self.assertIsNone(comms._worker_done_array)
                self.assertIsNone(comms._workers_dead)
                self.assertIsNone(comms._exception_queue)
                self.assertIsInstance(comms.exception_lock,
                                      _thread.LockType if using_threading else expected_mp.synchronize.Lock)
                self.assertIsInstance(comms._exception_thrown, expected_mp.synchronize.Event)
                self.assertIsInstance(comms._kill_signal_received, expected_mp.synchronize.Event)
                self.assertFalse(comms._exception_thrown.is_set())
                self.assertFalse(comms._kill_signal_received.is_set())
                self.assertIsNone(comms._task_completed_queue)
                self.assertIsNone(comms._all_exit_results_obtained)

            with self.subTest('without initial values', n_jobs=n_jobs, using_threading=using_threading,
                              has_worker_exit=False, has_progress_bar=False):
                comms.init_comms(has_worker_exit=False, has_progress_bar=False)
                self.assertFalse(comms._keep_order.is_set())
                self.assertEqual(len(comms._task_queues), n_jobs)
                for q in comms._task_queues:
                    self.assertIsInstance(q, expected_mp.queues.JoinableQueue)
                self.assertIsNone(comms._last_completed_task_worker_id)
                self.assertIsInstance(comms._results_queue, expected_mp.queues.JoinableQueue)
                self.assertListEqual(comms._exit_results_queues, [])
                self.assertIsInstance(comms._worker_done_array, ctypes.Array)
                self.assertEqual(len(comms._workers_dead), n_jobs)
                for worker_dead in comms._workers_dead:
                    self.assertIsInstance(worker_dead,
                                          threading.Event if using_threading else expected_mp.synchronize.Event)
                    self.assertTrue(worker_dead.is_set())
                self.assertIsInstance(comms._exception_queue, expected_mp.queues.JoinableQueue)
                self.assertFalse(comms._exception_thrown.is_set())
                self.assertFalse(comms._kill_signal_received.is_set())
                self.assertIsNone(comms._task_completed_queue)
                self.assertIsNone(comms._progress_bar_complete)

                # Basic sanity checks for the values
                self.assertEqual(comms._task_idx, 0)
                self.assertEqual(list(comms._worker_done_array), [False for _ in range(n_jobs)])

            with self.subTest('without initial values', n_jobs=n_jobs, using_threading=using_threading,
                              has_worker_exit=True, has_progress_bar=True):
                comms.init_comms(has_worker_exit=True, has_progress_bar=True)
                self.assertEqual(len(comms._exit_results_queues), n_jobs)
                for q in comms._exit_results_queues:
                    self.assertIsInstance(q, expected_mp.queues.JoinableQueue)
                self.assertIsInstance(comms._all_exit_results_obtained,
                                      threading.Event if using_threading else expected_mp.synchronize.Event)
                self.assertFalse(comms._all_exit_results_obtained.is_set())
                self.assertIsInstance(comms._task_completed_queue, expected_mp.queues.JoinableQueue)
                self.assertIsInstance(comms._progress_bar_complete, expected_mp.synchronize.Event)
                self.assertFalse(comms._progress_bar_complete.is_set())

            # Set some values so we can test if the containers will be properly resetted
            comms._keep_order.set()
            comms._task_idx = 4
            comms._last_completed_task_worker_id = 2
            comms._worker_done_array[:] = [False, True, False, True][:n_jobs]
            [worker_dead.clear() for worker_dead in comms._workers_dead]
            comms._exception_thrown.set()
            comms._kill_signal_received.set()

            # Note that threading doesn't have a JoinableQueue, so they're taken from multiprocessing
            with self.subTest('with initial values', n_jobs=n_jobs, using_threading=using_threading,
                              has_worker_exit=False, has_progress_bar=False):
                comms.init_comms(has_worker_exit=False, has_progress_bar=False)
                self.assertEqual(len(comms._task_queues), n_jobs)
                for q in comms._task_queues:
                    self.assertIsInstance(q, expected_mp.queues.JoinableQueue)
                self.assertIsNone(comms._last_completed_task_worker_id)
                self.assertIsInstance(comms._results_queue, expected_mp.queues.JoinableQueue)
                self.assertListEqual(comms._exit_results_queues, [])
                self.assertIsInstance(comms._worker_done_array, ctypes.Array)
                self.assertEqual(len(comms._workers_dead), n_jobs)
                for worker_dead in comms._workers_dead:
                    self.assertIsInstance(worker_dead,
                                          threading.Event if using_threading else expected_mp.synchronize.Event)
                    self.assertTrue(worker_dead.is_set())
                self.assertIsInstance(comms._exception_queue, expected_mp.queues.JoinableQueue)
                self.assertFalse(comms._exception_thrown.is_set())
                self.assertFalse(comms._kill_signal_received.is_set())
                self.assertIsNone(comms._task_completed_queue)

                # Some variables are not reset by this function, but are reset otherwise
                self.assertIsInstance(comms._keep_order,
                                      threading.Event if using_threading else expected_mp.synchronize.Event)
                self.assertTrue(comms._keep_order.is_set())

                # Basic sanity checks for the values
                self.assertEqual(comms._task_idx, 0)
                self.assertEqual(list(comms._worker_done_array), [False for _ in range(n_jobs)])

    def test_progress_bar(self):
        """
        Test progress bar related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2, False, False)

        # Has progress bar
        self.assertFalse(comms.has_progress_bar())
        comms.init_comms(False, True)
        self.assertTrue(comms.has_progress_bar())

        # Nothing available yet
        with self.assertRaises(queue.Empty):
            comms._task_completed_queue.get(block=False)

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
                last_updated, n_tasks_completed = comms.task_completed_progress_bar(last_updated, n_tasks_completed,
                                                                                    force_update=False)
                self.assertEqual(n_tasks_completed, n)
        with self.assertRaises(queue.Empty):
            comms._task_completed_queue.get(block=False)

        # Not enough time has passed, but we'll force the update. Number of tasks done should still be 3
        with patch('mpire.comms.datetime', new=MockDatetimeNow):
            last_updated, n_tasks_completed = comms.task_completed_progress_bar(last_updated, n_tasks_completed,
                                                                                force_update=True)
        self.assertEqual(comms.get_tasks_completed_progress_bar(), (3, True))
        self.assertEqual(last_updated, datetime(1970, 1, 1, 0, 0, 0, 0))
        self.assertEqual(n_tasks_completed, 0)
        comms.task_done_progress_bar()

        # 4 tasks already done and another 4 tasks done. Enough time should've passed for each update call, except the
        # second
        MockDatetimeNow.RETURN_VALUES = [datetime(1970, 1, 1, 0, 1, 0, 0),
                                         datetime(1970, 1, 1, 0, 1, 0, 0),
                                         datetime(1970, 1, 1, 0, 3, 0, 0),
                                         datetime(1970, 1, 1, 0, 4, 0, 0)]
        MockDatetimeNow.CURRENT_IDX = 0
        last_updated = datetime(1970, 1, 1, 0, 0, 0, 0)
        n_tasks_completed = 4
        with patch('mpire.comms.datetime', new=MockDatetimeNow):
            for _ in range(4):
                last_updated, n_tasks_completed = comms.task_completed_progress_bar(last_updated, n_tasks_completed,
                                                                                    force_update=False)
                self.assertEqual(last_updated, MockDatetimeNow.RETURN_VALUES[MockDatetimeNow.CURRENT_IDX - 1])
        self.assertListEqual([comms.get_tasks_completed_progress_bar() for _ in range(3)],
                             [(5, True), (2, True), (1, True)])
        self.assertEqual(last_updated, datetime(1970, 1, 1, 0, 4, 0, 0))
        self.assertEqual(n_tasks_completed, 0)
        [comms.task_done_progress_bar() for _ in range(3)]

        # Add poison pill
        comms.add_progress_bar_poison_pill()
        self.assertEqual(comms.get_tasks_completed_progress_bar(), (POISON_PILL, True))
        comms.task_done_progress_bar()

        # Set exception
        comms.set_exception_thrown()
        self.assertEqual(comms.get_tasks_completed_progress_bar(), (POISON_PILL, False))
        comms._exception_thrown.clear()

        # Set kill signal received
        comms.set_kill_signal_received()
        self.assertEqual(comms.get_tasks_completed_progress_bar(), (POISON_PILL, False))
        comms._kill_signal_received.clear()

        # Should be joinable. When using keep_alive, it should still be open
        comms.join_progress_bar_task_completed_queue(keep_alive=True)
        comms.task_completed_progress_bar(force_update=True)
        comms.get_tasks_completed_progress_bar()
        comms.task_done_progress_bar()
        comms.join_progress_bar_task_completed_queue(keep_alive=False)

        # Depending on Python version it either throws AssertionError or ValueError
        with self.assertRaises((AssertionError, ValueError)):
            comms.task_completed_progress_bar(force_update=True)

    def test_progress_bar_complete(self):
        """
        Test progress bar complete related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2, False, False)
        comms.init_comms(False, True)

        self.assertFalse(comms._progress_bar_complete.is_set())
        comms.set_progress_bar_complete()
        self.assertTrue(comms._progress_bar_complete.is_set())

        with patch.object(comms._progress_bar_complete, 'wait') as p:
            comms.wait_until_progress_bar_is_complete()
            self.assertEqual(p.call_count, 1)

    def test_keep_order(self):
        """
        Test keep_order related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2, False, False)

        self.assertFalse(comms.keep_order())
        comms.set_keep_order()
        self.assertTrue(comms.keep_order())
        comms.clear_keep_order()
        self.assertFalse(comms.keep_order())

    def test_tasks(self):
        """
        Test task related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 3, False, False)
        comms.init_comms(False, False)

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
        comms.init_comms(False, False)
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
        comms.set_exception_thrown()
        for worker_id in range(3):
            self.assertIsNone(comms.get_task(worker_id))

        # Should be joinable
        comms.join_task_queues()

    def test_results(self):
        """
        Test results related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2, False, False)
        comms.init_comms(False, False)

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
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 3, False, False)
        comms.init_comms(True, False)

        # Nothing available yet
        for worker_id in range(3):
            with self.assertRaises(queue.Empty):
                comms.get_exit_results(worker_id, block=False)

        # Add a few results. Note that `get_exit_results` calls `task_done`
        for worker_id in range(3):
            comms.add_exit_results(worker_id, worker_id)
            comms.add_exit_results(worker_id, 'hello world')
            comms.add_exit_results(worker_id, {'foo': 'bar'})
        self.assertListEqual([comms.get_exit_results(worker_id=0) for _ in range(3)],
                             [0, 'hello world', {'foo': 'bar'}])
        self.assertListEqual([comms.get_exit_results(worker_id=1) for _ in range(3)],
                             [1, 'hello world', {'foo': 'bar'}])
        self.assertListEqual([comms.get_exit_results(worker_id=2) for _ in range(3)],
                             [2, 'hello world', {'foo': 'bar'}])

        # When an exception is thrown it should return directly. However, when block=False it should still get it. We
        # use a while loop because we need to wait until the queue is ready. This block=False trick is used in
        # terminate().
        comms.add_exit_results(0, 'hello world')
        comms.set_exception_thrown()
        self.assertIsNone(comms.get_exit_results(0))
        while True:
            try:
                self.assertEqual(comms.get_exit_results(0, block=False), 'hello world')
                break
            except queue.Empty:
                pass
        comms._exception_thrown.clear()

        # Should be joinable. When using keep_alive, it should still be open
        comms.join_results_queues(keep_alive=True)
        for worker_id in range(3):
            comms.add_exit_results(worker_id, 'hello world')
            comms.get_exit_results(worker_id)
        comms.join_results_queues(keep_alive=False)

        # Depending on Python version it either throws AssertionError or ValueError
        for worker_id in range(3):
            with self.assertRaises((AssertionError, ValueError)):
                comms.add_exit_results(worker_id, 'hello world')

    def test_exit_results_all_workers(self):
        """
        Test exit results related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 4, False, False)
        comms.init_comms(True, False)

        # Add a few results. Every worker will always have a return value (even if it's the implicit None). Note that
        # `get_exit_results` calls `task_done`
        for worker_id in range(3):
            comms.add_exit_results(worker_id, worker_id)
        comms.add_exit_results(3, None)
        self.assertListEqual(comms.get_exit_results_all_workers(), [0, 1, 2, None])

        # Should be joinable
        comms.join_results_queues()

    def test_exit_results_all_workers_exception_thrown(self):
        """
        Test exit results related functions. When an exception occurred, it should return an empty list
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 3, False, False)
        comms.init_comms(True, False)

        # Add a few results.
        for worker_id in range(3):
            comms.add_exit_results(worker_id, worker_id)

        # Set exception
        comms.set_exception_thrown()

        # Should return empty list
        self.assertListEqual(comms.get_exit_results_all_workers(), [])

        # Drain and join
        comms._exception_thrown.clear()
        comms.get_exit_results_all_workers()
        comms.join_results_queues()

    def test_all_exit_results_obtained(self):
        """
        Test all exit results obtained related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2, False, False)
        comms.init_comms(True, False)

        self.assertFalse(comms._all_exit_results_obtained.is_set())
        comms.set_all_exit_results_obtained()
        self.assertTrue(comms._all_exit_results_obtained.is_set())

        with patch.object(comms._all_exit_results_obtained, 'wait') as p:
            comms.wait_until_all_exit_results_obtained()
            self.assertEqual(p.call_count, 1)

    def test_add_new_map_params(self):
        """
        Test new map parameters function
        """
        comms = WorkerComms(MP_CONTEXTS['mp_dill'][DEFAULT_START_METHOD], 2, True, False)
        comms.init_comms(False, False)

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
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2, False, False)
        comms.init_comms(False, False)

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
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2, False, False)

        self.assertFalse(comms.exception_thrown())
        comms.set_exception_thrown()
        self.assertTrue(comms.exception_thrown())
        comms._exception_thrown.clear()
        self.assertFalse(comms.exception_thrown())

    def test_kill_signal_received(self):
        """
        Test kill signal received related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2, False, False)

        self.assertFalse(comms.kill_signal_received())
        comms.set_kill_signal_received()
        self.assertTrue(comms.kill_signal_received())
        comms._kill_signal_received.clear()
        self.assertFalse(comms.kill_signal_received())

    def test_worker_poison_pill(self):
        """
        Test that a poison pill is inserted for every worker
        """
        for n_jobs in [1, 2, 4]:
            with self.subTest(n_jobs=n_jobs):
                comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], n_jobs, False, False)
                comms.init_comms(False, False)
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
                comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], n_jobs, False, False)
                comms.init_comms(False, False)
                comms.insert_non_lethal_poison_pill()
                for worker_id in range(n_jobs):
                    self.assertEqual(comms.get_task(worker_id), NON_LETHAL_POISON_PILL)
                    comms.task_done(worker_id)
                comms.join_task_queues()

    def test_worker_restart(self):
        """
        Test worker restart related functions
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 5, False, False)
        comms.init_comms(False, False)

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
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 5, False, False)
        comms.init_comms(False, False)

        # Signal some workers are alive
        comms.set_worker_alive(0)
        comms.set_worker_alive(1)
        comms.set_worker_dead(1)
        comms.set_worker_alive(2)
        comms.set_worker_alive(3)

        # Check alive status
        self.assertListEqual([comms.is_worker_alive(worker_id) for worker_id in range(5)],
                             [True, False, True, True, False])

        # Reset some
        comms.set_worker_dead(0)
        comms.set_worker_dead(3)

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
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 5, False, False)
        dont_wait_event = threading.Event()
        dont_wait_event.set()

        comms.init_comms(has_worker_exit=False, has_progress_bar=False)
        with self.subTest(has_worker_exit=False, has_progress_bar=False), \
                patch.object(comms, 'get_results', side_effect=comms.get_results) as p1, \
                patch.object(comms, 'get_exit_results') as p2:
            comms.drain_queues_terminate_worker(0, dont_wait_event)
            self.assertEqual(p1.call_count, 1)
            self.assertEqual(p2.call_count, 0)
            self.assertTrue(dont_wait_event.is_set())

        comms.init_comms(has_worker_exit=True, has_progress_bar=False)
        for worker_id in range(5):
            with self.subTest(has_worker_exit=True, has_progress_bar=False, worker_id=worker_id), \
                    patch.object(comms, 'get_results', side_effect=comms.get_results) as p1, \
                    patch.object(comms, 'get_exit_results', side_effect=comms.get_exit_results) as p2:
                comms.drain_queues_terminate_worker(worker_id, dont_wait_event)
                self.assertEqual(p1.call_count, 1)
                self.assertEqual(p2.call_count, 1)
                self.assertEqual(p2.call_args_list[0][0][0], worker_id)
                self.assertTrue(dont_wait_event.is_set())

        comms.init_comms(has_worker_exit=False, has_progress_bar=True)
        with self.subTest(has_worker_exit=False, has_progress_bar=True), \
                patch.object(comms, 'get_results', side_effect=comms.get_results) as p1, \
                patch.object(comms, 'get_exit_results') as p2, \
                patch.object(comms._task_completed_queue, 'get', side_effect=comms._task_completed_queue.get) as p3:
            comms.drain_queues_terminate_worker(0, dont_wait_event)
            self.assertEqual(p1.call_count, 1)
            self.assertEqual(p2.call_count, 0)
            self.assertEqual(p3.call_count, 1)
            self.assertTrue(dont_wait_event.is_set())

        comms.init_comms(has_worker_exit=True, has_progress_bar=True)
        for worker_id in range(5):
            with self.subTest(has_worker_exit=True, has_progress_bar=True, worker_id=worker_id), \
                    patch.object(comms, 'get_results', side_effect=comms.get_results) as p1, \
                    patch.object(comms, 'get_exit_results', side_effect=comms.get_exit_results) as p2, \
                    patch.object(comms._task_completed_queue, 'get', side_effect=comms._task_completed_queue.get) as p3:
                comms.drain_queues_terminate_worker(worker_id, dont_wait_event)
                self.assertEqual(p1.call_count, 1)
                self.assertEqual(p2.call_count, 1)
                self.assertEqual(p2.call_args_list[0][0][0], worker_id)
                self.assertEqual(p3.call_count, 1)
                self.assertTrue(dont_wait_event.is_set())

    def test_drain_queues(self):
        """
        _drain_and_join_queue should be called for every queue that matters. There are as many tasks queues as workers,
        1 results queue, if a worker_exit function has been provided as many exit results queues as workers, and if a
        progress bar has been enabled one queu for the progress bar.
        """
        for n_jobs, has_worker_exit, has_progress_bar in product([1, 2, 4], [False, True], [False, True]):
            comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], n_jobs, False, False)
            comms.init_comms(has_worker_exit=has_worker_exit, has_progress_bar=has_progress_bar)

            with self.subTest(n_jobs=n_jobs, has_worker_exit=has_worker_exit, has_progress_bar=has_progress_bar), \
                    patch.object(comms, 'drain_and_join_queue') as p:
                comms.drain_queues()
                self.assertEqual(p.call_count,
                                 n_jobs + 1 + (n_jobs if has_worker_exit else 0) + (1 if has_progress_bar else 0))

    def test__drain_and_join_queue(self):
        """
        Test draining queues
        """
        comms = WorkerComms(MP_CONTEXTS['mp'][DEFAULT_START_METHOD], 2, False, False)

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
