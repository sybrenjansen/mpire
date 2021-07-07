import ctypes
import multiprocessing as mp
import queue
import threading
import unittest
from datetime import datetime
from itertools import product
from unittest.mock import patch

from mpire.comms import POISON_PILL, WorkerComms
from tests.utils import MockDatetimeNow


class WorkerCommsTest(unittest.TestCase):

    def test_init_comms(self):
        """
        Test if initializing/resetting the comms is done properly
        """
        for n_jobs in [1, 2, 4]:
            comms = WorkerComms(mp.get_context('fork'), n_jobs)
            self.assertEqual(comms.ctx, mp.get_context('fork'))
            self.assertEqual(comms.n_jobs, n_jobs)

            with self.subTest('__init__ called', n_jobs=n_jobs):
                self.assertFalse(comms._keep_order.is_set())
                self.assertIsNone(comms._tasks_queue)
                self.assertIsNone(comms._results_queue)
                self.assertListEqual(comms._exit_results_queues, [])
                self.assertIsNone(comms._worker_done_array)
                self.assertIsNone(comms._workers_dead)
                self.assertIsNone(comms._task_completed_queue)
                self.assertIsNone(comms._exception_queue)
                self.assertIsInstance(comms.exception_lock, mp.synchronize.Lock)
                self.assertFalse(comms._exception_thrown.is_set())
                self.assertFalse(comms._exception_caught.is_set())
                self.assertIsNone(comms.worker_id)
                self.assertIsNone(comms._progress_bar_last_updated)
                self.assertIsNone(comms._progress_bar_n_tasks_completed)

            with self.subTest('without initial values', n_jobs=n_jobs, has_worker_exit=False, has_progress_bar=False):
                comms.init_comms(has_worker_exit=False, has_progress_bar=False)
                self.assertFalse(comms._keep_order.is_set())
                self.assertIsInstance(comms._tasks_queue, mp.queues.JoinableQueue)
                self.assertIsInstance(comms._results_queue, mp.queues.JoinableQueue)
                self.assertListEqual(comms._exit_results_queues, [])
                self.assertIsInstance(comms._worker_done_array, ctypes.Array)
                self.assertEqual(len(comms._workers_dead), n_jobs)
                for worker_dead in comms._workers_dead:
                    self.assertIsInstance(worker_dead, mp.synchronize.Event)
                    self.assertTrue(worker_dead.is_set())
                self.assertIsNone(comms._task_completed_queue)
                self.assertIsInstance(comms._exception_queue, mp.queues.JoinableQueue)
                self.assertFalse(comms._exception_thrown.is_set())
                self.assertFalse(comms._exception_caught.is_set())
                self.assertIsNone(comms.worker_id)
                self.assertIsNone(comms._progress_bar_last_updated)
                self.assertIsNone(comms._progress_bar_n_tasks_completed)

                # Basic sanity checks for the values
                self.assertEqual(list(comms._worker_done_array), [False for _ in range(n_jobs)])

            with self.subTest('without initial values', n_jobs=n_jobs, has_worker_exit=True, has_progress_bar=True):
                comms.init_comms(has_worker_exit=True, has_progress_bar=True)
                self.assertEqual(len(comms._exit_results_queues), n_jobs)
                for q in comms._exit_results_queues:
                    self.assertIsInstance(q, mp.queues.JoinableQueue)
                self.assertIsInstance(comms._task_completed_queue, mp.queues.JoinableQueue)

            # Set some values so we can test if the containers will be properly resetted
            comms._keep_order.set()
            comms._worker_done_array[:] = [False, True, False, True][:n_jobs]
            [worker_dead.clear() for worker_dead in comms._workers_dead]
            comms._exception_thrown.set()
            comms._exception_caught.set()
            comms.worker_id = 3
            comms._progress_bar_last_updated = datetime.now()
            comms._progress_bar_n_tasks_completed = 42

            with self.subTest('with initial values', n_jobs=n_jobs, has_worker_exit=False, has_progress_bar=False):
                comms.init_comms(has_worker_exit=False, has_progress_bar=False)
                self.assertIsInstance(comms._tasks_queue, mp.queues.JoinableQueue)
                self.assertIsInstance(comms._results_queue, mp.queues.JoinableQueue)
                self.assertListEqual(comms._exit_results_queues, [])
                self.assertIsInstance(comms._worker_done_array, ctypes.Array)
                self.assertEqual(len(comms._workers_dead), n_jobs)
                for worker_dead in comms._workers_dead:
                    self.assertIsInstance(worker_dead, mp.synchronize.Event)
                    self.assertTrue(worker_dead.is_set())
                self.assertIsNone(comms._task_completed_queue)
                self.assertIsInstance(comms._exception_queue, mp.queues.JoinableQueue)
                self.assertFalse(comms._exception_thrown.is_set())
                self.assertFalse(comms._exception_caught.is_set())
                self.assertIsNone(comms.worker_id)
                self.assertIsNone(comms._progress_bar_last_updated)
                self.assertIsNone(comms._progress_bar_n_tasks_completed)

                # Some variables are not reset by this function, but are reset otherwise
                self.assertTrue(comms._keep_order.is_set())

                # Basic sanity checks for the values
                self.assertEqual(list(comms._worker_done_array), [False for _ in range(n_jobs)])

    def test_init_worker(self):
        """
        Worker ID should be stored correctly
        """
        MockDatetimeNow.RETURN_VALUES = [datetime(1970, 1, 1, 0, 0, 0, 0),
                                         datetime(1970, 1, 1, 0, 1, 0, 0),
                                         datetime(1970, 1, 1, 0, 4, 0, 0)]
        MockDatetimeNow.CURRENT_IDX = 0

        comms = WorkerComms(mp.get_context('fork'), 5)

        self.assertIsNone(comms.worker_id)
        for worker_id in [0, 1, 4]:
            with self.subTest(worker_id=worker_id, has_progress_bar=False):
                comms.init_comms(False, False)
                comms.init_worker(worker_id)
                self.assertEqual(comms.worker_id, worker_id)
                self.assertIsNone(comms._progress_bar_last_updated)
                self.assertIsNone(comms._progress_bar_n_tasks_completed)

            with self.subTest(worker_id=worker_id, has_progress_bar=True), \
                    patch('mpire.comms.datetime', new=MockDatetimeNow):
                comms.init_comms(False, True)
                comms.init_worker(worker_id)
                self.assertEqual(comms.worker_id, worker_id)
                self.assertEqual(comms._progress_bar_last_updated, datetime(1970, 1, 1, 0, worker_id, 0, 0))
                self.assertEqual(comms._progress_bar_n_tasks_completed, 0)

    def test_progress_bar(self):
        """
        Test progress bar related functions
        """
        comms = WorkerComms(mp.get_context('fork'), 2)

        # Has progress bar
        self.assertFalse(comms.has_progress_bar())
        comms.init_comms(False, True)
        self.assertTrue(comms.has_progress_bar())

        # Initialize worker
        MockDatetimeNow.RETURN_VALUES = [datetime(1970, 1, 1, 0, 0, 0, 0),
                                         datetime(1970, 1, 1, 0, 0, 0, 0),
                                         datetime(1970, 1, 1, 0, 0, 0, 0),
                                         datetime(1970, 1, 1, 0, 0, 0, 0),
                                         datetime(1970, 1, 1, 0, 0, 0, 0)]
        MockDatetimeNow.CURRENT_IDX = 0
        with patch('mpire.comms.datetime', new=MockDatetimeNow):
            comms.init_worker(0)

        # Nothing available yet
        with self.assertRaises(queue.Empty):
            comms._task_completed_queue.get(block=False)

        # 3 task done, but not enough time has passed
        with patch('mpire.comms.datetime', new=MockDatetimeNow):
            [comms.task_completed_progress_bar() for _ in range(3)]
        with self.assertRaises(queue.Empty):
            comms._task_completed_queue.get(block=False)

        # 1 more task done. Not enough time has passed, but we'll force the update. Number of tasks done should be
        # aggregated to 4
        with patch('mpire.comms.datetime', new=MockDatetimeNow):
            comms.task_completed_progress_bar(force_update=True)
        self.assertEqual(comms.get_tasks_completed_progress_bar(), (4, True))
        comms.task_done_progress_bar()

        # 3 tasks done. Enough time should've passed for each update call
        MockDatetimeNow.RETURN_VALUES = [datetime(1970, 1, 1, 0, 1, 0, 0),
                                         datetime(1970, 1, 1, 0, 2, 0, 0),
                                         datetime(1970, 1, 1, 0, 3, 0, 0)]
        MockDatetimeNow.CURRENT_IDX = 0
        with patch('mpire.comms.datetime', new=MockDatetimeNow):
            [comms.task_completed_progress_bar() for _ in range(3)]
        self.assertListEqual([comms.get_tasks_completed_progress_bar() for _ in range(3)],
                             [(1, True), (1, True), (1, True)])
        [comms.task_done_progress_bar() for _ in range(3)]

        # Add poison pill
        comms.add_progress_bar_poison_pill()
        self.assertEqual(comms.get_tasks_completed_progress_bar(), (POISON_PILL, True))
        comms.task_done_progress_bar()

        # Set exception
        comms.set_exception_caught()
        self.assertEqual(comms.get_tasks_completed_progress_bar(), (POISON_PILL, False))

        # Should be joinable now
        comms.join_progress_bar_task_completed_queue()

    def test_keep_order(self):
        """
        Test keep_order related functions
        """
        comms = WorkerComms(mp.get_context('fork'), 2)

        self.assertFalse(comms.keep_order())
        comms.set_keep_order()
        self.assertTrue(comms.keep_order())
        comms.clear_keep_order()
        self.assertFalse(comms.keep_order())

    def test_tasks(self):
        """
        Test task related functions
        """
        comms = WorkerComms(mp.get_context('fork'), 2)
        comms.init_comms(False, False)

        # Nothing available yet
        with self.assertRaises(queue.Empty):
            comms._tasks_queue.get(block=False)

        # Add a few tasks
        comms.add_task(12)
        comms.add_task('hello world')
        comms.add_task({'foo': 'bar'})
        self.assertListEqual([comms.get_task() for _ in range(3)], [12, 'hello world', {'foo': 'bar'}])
        [comms.task_done() for _ in range(3)]

        # Throw in an exception. Should return None
        comms.set_exception()
        self.assertIsNone(comms.get_task())

        # Should be joinable
        comms.join_tasks_queue()

    def test_results(self):
        """
        Test results related functions
        """
        comms = WorkerComms(mp.get_context('fork'), 2)
        comms.init_comms(False, False)

        # Nothing available yet
        with self.assertRaises(queue.Empty):
            comms._results_queue.get(block=False)

        # Add a few results. Note that `get_results` calls `task_done`
        comms.add_results(12)
        comms.add_results('hello world')
        comms.add_results({'foo': 'bar'})
        self.assertListEqual([comms.get_results() for _ in range(3)], [12, 'hello world', {'foo': 'bar'}])

        # Should be joinable
        comms.join_results_queues()

    def test_exit_results(self):
        """
        Test exit results related functions
        """
        comms = WorkerComms(mp.get_context('fork'), 3)
        comms.init_comms(True, False)

        # Nothing available yet
        for worker_id in range(3):
            with self.assertRaises(queue.Empty):
                comms.get_exit_results(worker_id, timeout=0)

        # Add a few results. Note that `get_exit_results` calls `task_done`
        for worker_id in range(3):
            comms.init_worker(worker_id)
            comms.add_exit_results(worker_id)
            comms.add_exit_results('hello world')
            comms.add_exit_results({'foo': 'bar'})
        self.assertListEqual([comms.get_exit_results(worker_id=0) for _ in range(3)],
                             [0, 'hello world', {'foo': 'bar'}])
        self.assertListEqual([comms.get_exit_results(worker_id=1) for _ in range(3)],
                             [1, 'hello world', {'foo': 'bar'}])
        self.assertListEqual([comms.get_exit_results(worker_id=2) for _ in range(3)],
                             [2, 'hello world', {'foo': 'bar'}])

        # Should be joinable
        comms.join_results_queues()

    def test_exit_results_all_workers(self):
        """
        Test exit results related functions
        """
        comms = WorkerComms(mp.get_context('fork'), 4)
        comms.init_comms(True, False)

        # Add a few results. Every worker will always have a return value (even if it's the implicit None). Note that
        # `get_exit_results` calls `task_done`
        for worker_id in range(3):
            comms.init_worker(worker_id)
            comms.add_exit_results(worker_id)
        comms.init_worker(3)
        comms.add_exit_results(None)
        self.assertListEqual(comms.get_exit_results_all_workers(), [0, 1, 2, None])

        # Should be joinable
        comms.join_results_queues()

    def test_exit_results_all_workers_exception_thrown(self):
        """
        Test exit results related functions. When an exception occurred, it should return an empty list
        """
        comms = WorkerComms(mp.get_context('fork'), 3)
        comms.init_comms(True, False)

        # Add a few results.
        for worker_id in range(3):
            comms.init_worker(worker_id)
            comms.add_exit_results(worker_id)

        # Set exception
        comms.set_exception()

        # Should return empty list
        self.assertListEqual(comms.get_exit_results_all_workers(), [])

        # Drain and join
        comms._exception_thrown.clear()
        comms.get_exit_results_all_workers()
        comms.join_results_queues()

    def test_exceptions(self):
        """
        Test exception related functions
        """
        comms = WorkerComms(mp.get_context('fork'), 2)
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

        # Should be joinable now
        comms.join_exception_queue()

    def test_exception_thrown(self):
        """
        Test exception thrown related functions
        """
        comms = WorkerComms(mp.get_context('fork'), 2)

        self.assertFalse(comms.exception_thrown())
        comms.set_exception()
        self.assertTrue(comms.exception_thrown())
        comms._exception_thrown.clear()
        self.assertFalse(comms.exception_thrown())

    def test_exception_caught(self):
        """
        Test exception thrown related functions
        """
        comms = WorkerComms(mp.get_context('fork'), 2)

        self.assertFalse(comms.exception_caught())
        comms.set_exception_caught()
        self.assertTrue(comms.exception_caught())
        comms._exception_caught.clear()
        self.assertFalse(comms.exception_caught())

        # We test wait by simply checking the call count
        with patch.object(comms._exception_caught, 'wait') as p:
            comms.wait_until_exception_is_caught()
            self.assertEqual(p.call_count, 1)

    def test_worker_poison_pill(self):
        """
        Test that a poison pill is inserted for every worker
        """
        for n_jobs in [1, 2, 4]:
            with self.subTest(n_jobs=n_jobs):
                comms = WorkerComms(mp.get_context('fork'), n_jobs)
                comms.init_comms(False, False)
                comms.insert_poison_pill()
                self.assertListEqual([comms.get_task() for _ in range(n_jobs)], [POISON_PILL for _ in range(n_jobs)])
                [comms.task_done() for _ in range(n_jobs)]
                comms.join_tasks_queue()

    def test_worker_restart(self):
        """
        Test worker restart related functions
        """
        comms = WorkerComms(mp.get_context('fork'), 5)
        comms.init_comms(False, False)

        # No restarts yet
        self.assertListEqual(list(comms.get_worker_restarts()), [])

        # Signal some restarts
        comms.init_worker(0)
        comms.signal_worker_restart()
        comms.init_worker(2)
        comms.signal_worker_restart()
        comms.init_worker(3)
        comms.signal_worker_restart()

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
        comms = WorkerComms(mp.get_context('fork'), 5)
        comms.init_comms(False, False)

        # Signal some workers are alive
        comms.init_worker(0)
        comms.set_worker_alive()
        comms.init_worker(1)
        comms.set_worker_alive()
        comms.set_worker_dead()
        comms.init_worker(2)
        comms.set_worker_alive()
        comms.init_worker(3)
        comms.set_worker_alive()

        # Check alive status
        self.assertListEqual([comms.is_worker_alive(worker_id) for worker_id in range(5)],
                             [True, False, True, True, False])

        # Reset some
        comms.init_worker(0)
        comms.set_worker_dead()
        comms.init_worker(3)
        comms.set_worker_dead()

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
        comms = WorkerComms(mp.get_context('fork'), 5)
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
        _drain_and_join_queue should be called for every queue that matters
        """
        for n_jobs, has_worker_exit, has_progress_bar in product([1, 2, 4], [False, True], [False, True]):
            comms = WorkerComms(mp.get_context('fork'), n_jobs)
            comms.init_comms(has_worker_exit=has_worker_exit, has_progress_bar=has_progress_bar)

            with self.subTest(n_jobs=n_jobs, has_worker_exit=has_worker_exit, has_progress_bar=has_progress_bar), \
                    patch.object(comms, '_drain_and_join_queue') as p:
                comms.drain_queues()
                self.assertEqual(p.call_count, 2 + (n_jobs if has_worker_exit else 0) + (1 if has_progress_bar else 0))

    def test__drain_and_join_queue(self):
        """
        Test draining queues
        """
        comms = WorkerComms(mp.get_context('fork'), 2)

        # Create a custom queue with some data
        q = mp.JoinableQueue()
        q.put(1)
        q.put('hello')
        q.put('world')

        # Drain queue. It should now be empty
        comms._drain_and_join_queue(q)
        with self.assertRaises(queue.Empty):
            q.get(block=False)

        # Even though it's joined we test it here again, to be sure it's actually joinable. It isn't joinable when
        # task_done isn't called as many times as there are items in the queue.
        q.join()
