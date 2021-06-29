import ctypes
import multiprocessing as mp
import queue
import unittest
from itertools import product
from unittest.mock import patch

from mpire.comms import POISON_PILL, WorkerComms


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
                self.assertIsNone(comms._task_completed_queue)
                self.assertIsNone(comms._exception_queue)
                self.assertIsInstance(comms.exception_lock, mp.synchronize.Lock)
                self.assertFalse(comms._exception_thrown.is_set())
                self.assertFalse(comms._exception_caught.is_set())
                self.assertIsNone(comms.worker_id)

            with self.subTest('without initial values', n_jobs=n_jobs, has_worker_exit=False, has_progress_bar=False):
                comms.init_comms(has_worker_exit=False, has_progress_bar=False)
                self.assertFalse(comms._keep_order.is_set())
                self.assertIsInstance(comms._tasks_queue, mp.queues.JoinableQueue)
                self.assertIsInstance(comms._results_queue, mp.queues.JoinableQueue)
                self.assertListEqual(comms._exit_results_queues, [])
                self.assertIsInstance(comms._worker_done_array, ctypes.Array)
                self.assertIsNone(comms._task_completed_queue)
                self.assertIsInstance(comms._exception_queue, mp.queues.JoinableQueue)
                self.assertFalse(comms._exception_thrown.is_set())
                self.assertFalse(comms._exception_caught.is_set())
                self.assertIsNone(comms.worker_id)

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
            comms._exception_thrown.set()
            comms._exception_caught.set()
            comms.worker_id = 3

            with self.subTest('with initial values', n_jobs=n_jobs, has_worker_exit=False, has_progress_bar=False):
                comms.init_comms(has_worker_exit=False, has_progress_bar=False)
                self.assertIsInstance(comms._tasks_queue, mp.queues.JoinableQueue)
                self.assertIsInstance(comms._results_queue, mp.queues.JoinableQueue)
                self.assertListEqual(comms._exit_results_queues, [])
                self.assertIsInstance(comms._worker_done_array, ctypes.Array)
                self.assertIsNone(comms._task_completed_queue)
                self.assertIsInstance(comms._exception_queue, mp.queues.JoinableQueue)
                self.assertFalse(comms._exception_thrown.is_set())
                self.assertFalse(comms._exception_caught.is_set())
                self.assertIsNone(comms.worker_id)

                # Some variables are not reset by this function, but are reset otherwise
                self.assertTrue(comms._keep_order.is_set())

                # Basic sanity checks for the values
                self.assertEqual(list(comms._worker_done_array), [False for _ in range(n_jobs)])

    def test_init_worker(self):
        """
        Worker ID should be stored correctly
        """
        comms = WorkerComms(mp.get_context('fork'), 5)

        self.assertIsNone(comms.worker_id)
        for worker_id in [0, 1, 4]:
            with self.subTest(worker_id=worker_id):
                comms.init_worker(worker_id)
                self.assertEqual(comms.worker_id, worker_id)

    def test_progress_bar(self):
        """
        Test progress bar related functions
        """
        comms = WorkerComms(mp.get_context('fork'), 2)

        # Has progress bar
        self.assertFalse(comms.has_progress_bar())
        comms.init_comms(False, True)
        self.assertTrue(comms.has_progress_bar())

        # Nothing available yet
        with self.assertRaises(queue.Empty):
            comms._task_completed_queue.get(block=False)

        # 3 tasks done
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

    def test_drain_queues(self):
        """
        _drain_and_join_queue should be called for every queue that matters
        """
        for n_jobs, has_worker_exit in product([1, 2, 4], [False, True]):
            comms = WorkerComms(mp.get_context('fork'), n_jobs)
            comms.init_comms(has_worker_exit=has_worker_exit, has_progress_bar=False)

            with self.subTest(n_jobs=n_jobs, has_worker_exit=has_worker_exit), \
                    patch.object(comms, '_drain_and_join_queue') as p:
                comms.drain_queues()
                self.assertEqual(p.call_count, 2 + (n_jobs if has_worker_exit else 0))

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

        # We do a get one time, such that after we obtain the remaining 2 items the task_done calls doesn't add up to
        # 3. Therefore it should go into the second while loop
        q.get()

        # Drain queue. It should now be empty
        comms._drain_and_join_queue(q)
        with self.assertRaises(queue.Empty):
            q.get(block=False)

        # Even though it's joined we test it here again, to be sure it's actually joinable. It isn't joinable when
        # task_done isn't called as many times as there are items in the queue.
        q.join()
