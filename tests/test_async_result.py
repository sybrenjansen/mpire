import itertools
import queue
import unittest
from unittest.mock import patch, Mock

from mpire.async_result import (AsyncResult, AsyncResultWithExceptionGetter, UnorderedAsyncExitResultIterator,
                                UnorderedAsyncResultIterator)
from mpire.comms import INIT_FUNC, EXIT_FUNC, MAIN_PROCESS


class AsyncResultTest(unittest.TestCase):

    def setUp(self) -> None:
        self.cache = {}

    def _reset_cache(self) -> None:
        self.cache = {}

    def test_init(self):
        """
        Test that the job_id is set correctly and that the cache is updated
        """
        with self.subTest("job_counter=0", job_id=None), \
                patch('mpire.async_result.job_counter', itertools.count(start=0)):
            r = AsyncResult(self.cache, None, None, None, True)
            self.assertEqual(r.job_id, 0)
            self.assertEqual(self.cache, {0: r})

        self._reset_cache()

        with self.subTest("job_counter=10", job_id=None), \
                patch('mpire.async_result.job_counter', itertools.count(start=10)):
            r = AsyncResult(self.cache, None, None, None, True)
            self.assertEqual(r.job_id, 10)
            self.assertEqual(self.cache, {10: r})

        self._reset_cache()

        with self.subTest("job_counter=0", job_id=42), \
                patch('mpire.async_result.job_counter', itertools.count(start=0)):
            r = AsyncResult(self.cache, None, None, 42, True)
            self.assertEqual(r.job_id, 42)
            self.assertEqual(self.cache, {42: r})

        with self.subTest("job_id already exists in cache"), \
                patch('mpire.async_result.job_counter', itertools.count(start=0)):
            with self.assertRaises(AssertionError):
                AsyncResult(self.cache, None, None, 42, True)

    def test_ready(self):
        """
        Test that the ready method returns the correct value
        """
        r = AsyncResult(self.cache, None, None, None, True)
        self.assertFalse(r.ready())
        r._ready_event.set()
        self.assertTrue(r.ready())

    def test_successful(self):
        """
        Test that the successful method returns the correct value
        """
        r = AsyncResult(self.cache, None, None, None, True)

        # Test that the method raises a ValueError if the task is not finished yet
        with self.assertRaises(ValueError):
            r.successful()

        r._success = True
        r._ready_event.set()
        self.assertTrue(r.successful())

        r._success = False
        self.assertFalse(r.successful())

    def test_get_value(self):
        """
        Test that the get method returns the correct value
        """
        r = AsyncResult(self.cache, None, None, None, True)
        r._value = 42
        r._success = True
        r._ready_event.set()
        self.assertEqual(r.get(), 42)

        r._value = 1337
        self.assertEqual(r.get(), 1337)

    def test_get_timeout(self):
        """
        Test that the get method raises a TimeoutError if the timeout is exceeded
        """
        r = AsyncResult(self.cache, None, None, None, True)
        with self.assertRaises(TimeoutError):
            r.get(timeout=0.001)

    def test_get_error(self):
        """
        Test that the get method raises the error if the task has failed
        """
        r = AsyncResult(self.cache, None, None, None, True)
        r._value = ValueError('test')
        r._success = False
        r._ready_event.set()
        with self.assertRaises(ValueError):
            r.get()

    def test_set_success(self):
        """
        Test that the _set method sets the correct values if the task has succeeded
        """
        r = AsyncResult(self.cache, None, None, None, True)
        r._set(True, 42)
        self.assertTrue(r._success)
        self.assertEqual(r._value, 42)
        self.assertTrue(r._ready_event.is_set())

    def test_set_exception(self):
        """
        Test that the _set method sets the correct values if the task has failed
        """
        value_error = ValueError('test')
        r = AsyncResult(self.cache, None, None, None, True)
        r._set(False, value_error)
        self.assertFalse(r._success)
        self.assertEqual(r._value, value_error)
        self.assertTrue(r._ready_event.is_set())

    def test_set_delete_from_cache(self):
        """
        Test that the _set method deletes the result from the cache if the task has finished
        """
        with self.subTest("delete"):
            r = AsyncResult(self.cache, None, None, None, True)
            r._set(True, 42)
            self.assertNotIn(r.job_id, self.cache)

        with self.subTest("no_delete"):
            r = AsyncResult(self.cache, None, None, None, False)
            r._set(True, 42)
            self.assertIn(r.job_id, self.cache)

    def test_callback_success(self):
        """
        Test that the callback is called if the task has succeeded
        """
        callback = Mock()
        r = AsyncResult(self.cache, callback, None, None, True)
        r._set(True, 42)
        callback.assert_called_once_with(42)

    def test_callback_error(self):
        """
        Test that the callback is called if the task has failed
        """
        callback = Mock()
        value_error = ValueError('test')
        r = AsyncResult(self.cache, None, callback, None, True)
        r._set(False, value_error)
        callback.assert_called_once_with(value_error)


class UnorderedAsyncResultIteratorTest(unittest.TestCase):

    def setUp(self) -> None:
        self.cache = {}

    def _reset_cache(self) -> None:
        self.cache = {}

    def test_init(self):
        """
        Test that the job_id is set correctly and that the cache is updated
        """
        with self.subTest("job_counter=0", job_id=None), \
                patch('mpire.async_result.job_counter', itertools.count(start=0)):
            r = UnorderedAsyncResultIterator(self.cache, None, None)
            self.assertEqual(r.job_id, 0)
            self.assertEqual(self.cache, {0: r})

        self._reset_cache()

        with self.subTest("job_counter=10", job_id=None), \
                patch('mpire.async_result.job_counter', itertools.count(start=10)):
            r = UnorderedAsyncResultIterator(self.cache, None, None)
            self.assertEqual(r.job_id, 10)
            self.assertEqual(self.cache, {10: r})

        self._reset_cache()

        with self.subTest("job_counter=0", job_id=42), \
                patch('mpire.async_result.job_counter', itertools.count(start=0)):
            r = UnorderedAsyncResultIterator(self.cache, None, 42)
            self.assertEqual(r.job_id, 42)
            self.assertEqual(self.cache, {42: r})

        with self.subTest("job_id already exists in cache"), \
                patch('mpire.async_result.job_counter', itertools.count(start=0)):
            with self.assertRaises(AssertionError):
                UnorderedAsyncResultIterator(self.cache, None, 42)

    def test_iter(self):
        """
        Test that the iterator is returned
        """
        r = UnorderedAsyncResultIterator(self.cache, None, None)
        self.assertEqual(r, iter(r))

    def test_next(self):
        """
        Test that the next method returns the correct value
        """
        r = UnorderedAsyncResultIterator(self.cache, None, None)
        r._set(True, 42)
        r._set(True, 1337)
        self.assertEqual(next(r), 42)
        self.assertEqual(r._n_returned, 1)
        self.assertEqual(next(r), 1337)
        self.assertEqual(r._n_returned, 2)

    def test_next_timeout(self):
        """
        Test that the next method raises a queue.Empty if the timeout is exceeded
        """
        r = UnorderedAsyncResultIterator(self.cache, None, None)
        with self.assertRaises(queue.Empty):
            r.next(block=True, timeout=0.001)
        with self.assertRaises(queue.Empty):
            r.next(block=False)

    def test_next_all_returned(self):
        """
        Test that the next method raises a StopIteration if all values have been returned
        """
        r = UnorderedAsyncResultIterator(self.cache, 2, None)
        r._set(True, 42)
        r._set(True, 1337)
        next(r)
        next(r)
        with self.assertRaises(StopIteration):
            next(r)

    def test_set_success(self):
        """
        Test that the _set method sets the correct values if the task has succeeded
        """
        r = UnorderedAsyncResultIterator(self.cache, None, None)
        r._set(True, 42)
        self.assertIsNone(r._exception)
        self.assertFalse(r._got_exception.is_set())
        self.assertEqual(list(r._items), [42])

        r._set(True, 1337)
        r._set(True, 0)
        self.assertEqual(list(r._items), [42, 1337, 0])

    def test_set_exception(self):
        """
        Test that the _set method sets the exception if the task has failed
        """
        r = UnorderedAsyncResultIterator(self.cache, None, None)
        value_error = ValueError('test')
        r._set(False, value_error)
        self.assertEqual(r._exception, value_error)
        self.assertTrue(r._got_exception.is_set())
        self.assertEqual(list(r._items), [])

    def test_set_length(self):
        """
        Test that the _set method sets the correct length if it is given
        """
        r = UnorderedAsyncResultIterator(self.cache, None, None)
        self.assertIsNone(r._n_tasks)
        r.set_length(2)
        self.assertEqual(r._n_tasks, 2)

    def test_set_length_already_set(self):
        """
        Test that the _set method raises an ValueError if the length is already set. Setting the length to the same
        value should not raise an error.
        """
        r = UnorderedAsyncResultIterator(self.cache, 2, None)
        r.set_length(2)
        with self.assertRaises(ValueError):
            r.set_length(1)

    def test_get_exception(self):
        """
        Test that the get_exception method returns the correct exception
        """
        r = UnorderedAsyncResultIterator(self.cache, None, None)
        value_error = ValueError('test')
        r._set(False, value_error)
        self.assertEqual(r.get_exception(), value_error)

    def test_remove_from_cache(self):
        """
        Test that the remove_from_cache method removes the result from the cache
        """
        r = UnorderedAsyncResultIterator(self.cache, None, None)
        self.assertIn(r.job_id, self.cache)
        r.remove_from_cache()
        self.assertNotIn(r.job_id, self.cache)


class AsyncResultWithExceptionGetterTest(unittest.TestCase):

    def setUp(self) -> None:
        self.cache = {}

    def test_init(self):
        """
        Test that the result is initialized correctly
        """
        r = AsyncResultWithExceptionGetter(self.cache, INIT_FUNC)
        self.assertEqual(r.job_id, INIT_FUNC)
        self.assertFalse(r._delete_from_cache)

        r = AsyncResultWithExceptionGetter(self.cache, MAIN_PROCESS)
        self.assertEqual(r.job_id, MAIN_PROCESS)
        self.assertFalse(r._delete_from_cache)

    def test_get_exception(self):
        """
        Test that the get_exception method returns the correct exception
        """
        r = AsyncResultWithExceptionGetter(self.cache, INIT_FUNC)
        value_error = ValueError('test')
        r._set(False, value_error)
        self.assertEqual(r.get_exception(), value_error)

    def test_reset(self):
        """
        Test that the reset method resets the result
        """
        value_error = ValueError('test')
        r = AsyncResultWithExceptionGetter(self.cache, INIT_FUNC)
        r._set(False, value_error)
        self.assertFalse(r._success)
        self.assertEqual(r._value, value_error)
        self.assertTrue(r._ready_event.is_set())
        r.reset()
        self.assertIsNone(r._success)
        self.assertIsNone(r._value)
        self.assertFalse(r._ready_event.is_set())


class UnorderedAsyncExitResultIteratorTest(unittest.TestCase):

    def setUp(self) -> None:
        self.cache = {}

    def test_init(self):
        """
        Test that the result is initialized correctly
        """
        r = UnorderedAsyncExitResultIterator(self.cache)
        self.assertEqual(r.job_id, EXIT_FUNC)
        self.assertIsNone(r._n_tasks)

    def test_get_results(self):
        """
        Test that the get_results method returns the correct results
        """
        value_error = ValueError('test')
        r = UnorderedAsyncExitResultIterator(self.cache)
        r._set(True, 42)
        r._set(True, 1337)
        r._set(False, value_error)
        self.assertEqual(r.get_results(), [42, 1337])

    def test_reset(self):
        """
        Test that the reset method resets the result
        """
        r = UnorderedAsyncExitResultIterator(self.cache)
        r._set(True, 42)
        r._set(False, ValueError('test'))
        r.set_length(2)
        next(r)
        self.assertEqual(r._n_tasks, 2)
        self.assertEqual(len(r._items), 0)  # Not 2, because 1 has alraedy been returned and 1 is an exception
        self.assertEqual(r._n_received, 1)
        self.assertEqual(r._n_returned, 1)
        self.assertIsNotNone(r._exception)
        self.assertTrue(r._got_exception.is_set())
        r.reset()
        self.assertEqual(r._n_tasks, None)
        self.assertEqual(len(r._items), 0)
        self.assertEqual(r._n_received, 0)
        self.assertEqual(r._n_returned, 0)
        self.assertIsNone(r._exception)
        self.assertFalse(r._got_exception.is_set())
