import types
import unittest
from datetime import datetime
from itertools import chain, product
from multiprocessing import cpu_count
from unittest.mock import patch

import numpy as np

from mpire.utils import apply_numpy_chunking, chunk_tasks, format_seconds, get_n_chunks, make_single_arguments, TimeIt
from tests.utils import MockDatetimeNow


class ChunkTasksTest(unittest.TestCase):

    def test_no_chunk_size_no_n_splits_provided(self):
        """
        Test that a ValueError is raised when no chunk_size and n_splits are provided
        """
        with self.assertRaises(ValueError):
            next(chunk_tasks([]))

    def test_generator_without_iterable_len(self):
        """
        Test that a ValueError is raised when a generator is provided without iterable_len
        """
        with self.assertRaises(ValueError):
            next(chunk_tasks(iter([]), n_splits=1))

    def test_chunk_size_has_priority_over_n_splits(self):
        """
        Test that chunk_size is prioritized over n_splits
        """
        chunks = list(chunk_tasks(range(4), chunk_size=4, n_splits=4))
        self.assertEqual(len(chunks), 1)
        self.assertEqual(len(chunks[0]), 4)
        self.assertEqual(list(range(4)), list(chain.from_iterable(chunks)))

    def test_empty_input(self):
        """
        Test that the chunker is an empty generator for an empty input iterable
        """
        with self.subTest('list input'):
            chunks = list(chunk_tasks([], n_splits=5))
            self.assertEqual(len(chunks), 0)

        with self.subTest('generator/iterator input'):
            chunks = list(chunk_tasks(iter([]), iterable_len=0, n_splits=5))
            self.assertEqual(len(chunks), 0)

    def test_iterable_len_doesnt_match_input_size(self):
        """
        Test for cases where iterable_len does and does not match the number of arguments (it should work fine)
        """
        num_args = 10
        for iter_len in [5, 10, 20]:
            expected_args_sum = min(iter_len, num_args)

            # Test for normal list (range is considered a normal list as it implements __len__ and such)
            with self.subTest(iter_len=iter_len, input='list'):
                chunks = list(chunk_tasks(range(num_args), iterable_len=iter_len, n_splits=1))
                total_args = sum(map(len, chunks))
                self.assertEqual(total_args, expected_args_sum)
                self.assertEqual(list(range(expected_args_sum)), list(chain.from_iterable(chunks)))

            # Test for an actual generator (range does not really behave like one)
            with self.subTest(iter_len=iter_len, input='generator/iterator'):
                chunks = list(chunk_tasks(iter(range(num_args)), iterable_len=iter_len, n_splits=1))
                total_args = sum(map(len, chunks))
                self.assertEqual(total_args, expected_args_sum)
                self.assertEqual(list(range(expected_args_sum)), list(chain.from_iterable(chunks)))

    def test_n_splits(self):
        """
        Test different values of n_splits: len(args) {<, ==, >} n_splits
        """
        n_splits = 5
        for num_args in [n_splits - 1, n_splits, n_splits + 1]:
            expected_n_chunks = min(n_splits, num_args)

            # Test for normal list (range is considered a normal list as it implements __len__ and such)
            with self.subTest(num_args=num_args, input='list'):
                chunks = list(chunk_tasks(range(num_args), n_splits=n_splits))
                self.assertEqual(len(chunks), expected_n_chunks)
                self.assertEqual(list(range(num_args)), list(chain.from_iterable(chunks)))

            # Test for an actual generator (range does not really behave like one)
            with self.subTest(num_args=num_args, input='generator/iterator'):
                chunks = list(chunk_tasks(iter(range(num_args)), iterable_len=num_args, n_splits=n_splits))
                self.assertEqual(len(chunks), expected_n_chunks)
                self.assertEqual(list(range(num_args)), list(chain.from_iterable(chunks)))

    def test_chunk_size(self):
        """
        Test that chunks are of the right size if chunk_size is provided
        """
        chunk_size = 3
        for num_args in [chunk_size - 1, chunk_size, chunk_size + 1]:

            # Test for normal list (range is considered a normal list as it implements __len__ and such)
            with self.subTest(num_args=num_args, input='list'):
                chunks = list(chunk_tasks(range(num_args), chunk_size=chunk_size))
                for chunk in chunks[:-1]:
                    self.assertEqual(len(chunk), chunk_size)
                self.assertLessEqual(len(chunks[-1]), chunk_size)
                self.assertEqual(list(range(num_args)), list(chain.from_iterable(chunks)))

            # Test for an actual generator (range does not really behave like one)
            with self.subTest(num_args=num_args, input='generator/iterator'):
                chunks = list(chunk_tasks(iter(range(num_args)), chunk_size=chunk_size))
                for chunk in chunks[:-1]:
                    self.assertEqual(len(chunk), chunk_size)
                self.assertLessEqual(len(chunks[-1]), chunk_size)
                self.assertEqual(list(range(num_args)), list(chain.from_iterable(chunks)))


class ApplyNumpyChunkingTest(unittest.TestCase):

    """
    This function simply calls other, already tested, functions in succession. We do test the individual parameter
    influence, but interactions between them are skipped
    """

    def setUp(self):
        self.test_data_numpy = np.random.rand(100, 2)

    def test_iterable_len(self):
        """
        Test that iterable_len is adhered to. When iterable_len < len(input) it should reduce the input size. If higher
        or None it should take the entire input
        """
        for iterable_len, expected_size in [(5, 5), (150, 100), (None, 100)]:
            with self.subTest(iterable_len=iterable_len):
                iterable_of_args, iterable_len_, chunk_size, n_splits = apply_numpy_chunking(
                    self.test_data_numpy, iterable_len=iterable_len, n_splits=1
                )

                # Materialize generator and test contents
                iterable_of_args = list(iterable_of_args)
                self.assertEqual(len(iterable_of_args), 1)
                self.assertIsInstance(iterable_of_args[0][0], np.ndarray)
                np.testing.assert_array_equal(iterable_of_args[0][0], self.test_data_numpy[:expected_size])

                # Test other output
                self.assertEqual(iterable_len_, 1)
                self.assertEqual(chunk_size, 1)
                self.assertIsNone(n_splits)

    def test_chunk_size(self):
        """
        Test that chunk_size works as expected. Note that chunk_size trumps n_splits
        """
        for chunk_size, expected_n_chunks in [(1, 100), (3, 34), (200, 1), (None, 1)]:
            with self.subTest(chunk_size=chunk_size):
                iterable_of_args, iterable_len, chunk_size_, n_splits = apply_numpy_chunking(
                    self.test_data_numpy, chunk_size=chunk_size, n_splits=1
                )

                # Materialize generator and test contents. The chunks should be of size chunk_size (expect for the last
                # chunk which can be smaller)
                iterable_of_args = list(iterable_of_args)
                self.assertEqual(len(iterable_of_args), expected_n_chunks)
                chunk_size = chunk_size or 100
                for chunk_idx, chunk in enumerate(iterable_of_args):
                    self.assertIsInstance(chunk[0], np.ndarray)
                    np.testing.assert_array_equal(chunk[0], self.test_data_numpy[chunk_idx * chunk_size:
                                                                                 (chunk_idx + 1) * chunk_size])

                # Test other output
                self.assertEqual(iterable_len, expected_n_chunks)
                self.assertEqual(chunk_size_, 1)
                self.assertIsNone(n_splits)

    def test_n_splits(self):
        """
        Test that n_splits works as expected.
        """
        for n_splits, expected_n_chunks in [(1, 1), (3, 3), (150, 100)]:
            with self.subTest(n_splits=n_splits):
                iterable_of_args, iterable_len, chunk_size, n_splits_ = apply_numpy_chunking(
                    self.test_data_numpy, n_splits=n_splits
                )

                # Materialize generator and test contents. We simply test if every row of the original input occurs in
                # the chunks
                iterable_of_args = list(iterable_of_args)
                self.assertEqual(len(iterable_of_args), expected_n_chunks)
                offset = 0
                for chunk in iterable_of_args:
                    self.assertIsInstance(chunk[0], np.ndarray)
                    np.testing.assert_array_equal(chunk[0], self.test_data_numpy[offset:offset + len(chunk[0])])
                    offset += len(chunk[0])
                self.assertEqual(offset, 100)

                # Test other output
                self.assertEqual(iterable_len, expected_n_chunks)
                self.assertEqual(chunk_size, 1)
                self.assertIsNone(n_splits_)

        # chunk_size and n_splits can't be both None
        with self.subTest(n_splits=None), self.assertRaises(ValueError):
            iterable_of_args, *_ = apply_numpy_chunking(self.test_data_numpy, n_splits=None)
            list(iterable_of_args)

    def test_n_jobs(self):
        """
        Test that n_jobs works as expected. When chunk_size and n_splits are both None, n_jobs * 4 is passed on as
        n_splits
        """
        for n_jobs, expected_n_chunks in [(1, 4), (3, 12), (40, 100), (150, 100)]:
            with self.subTest(n_jobs=n_jobs):
                iterable_of_args, iterable_len, chunk_size, n_splits_ = apply_numpy_chunking(
                    self.test_data_numpy, n_jobs=n_jobs
                )

                # Materialize generator and test contents. We simply test if every row of the original input occurs in
                # the chunks
                iterable_of_args = list(iterable_of_args)
                self.assertEqual(len(iterable_of_args), expected_n_chunks)
                offset = 0
                for chunk in iterable_of_args:
                    self.assertIsInstance(chunk[0], np.ndarray)
                    np.testing.assert_array_equal(chunk[0], self.test_data_numpy[offset:offset + len(chunk[0])])
                    offset += len(chunk[0])
                self.assertEqual(offset, 100)

                # Test other output
                self.assertEqual(iterable_len, expected_n_chunks)
                self.assertEqual(chunk_size, 1)
                self.assertIsNone(n_splits_)


class GetNChunksTest(unittest.TestCase):

    def setUp(self):
        self.test_data = [1, 2, 3, 5, 6, 9, 37, 42, 1337, 0, 3, 5, 0]
        self.test_data_numpy = np.random.rand(100, 2)

    def test_everything_none(self):
        """
        When everything is None we should use cpu_count * 4 as number of splits. We have to take the number of tasks
        into account
        """
        with self.subTest(input='list'):
            self.assertEqual(get_n_chunks(self.test_data, iterable_len=None, chunk_size=None, n_splits=None,
                                          n_jobs=None), min(13, cpu_count() * 4))
        with self.subTest(input='numpy'):
            self.assertEqual(get_n_chunks(self.test_data_numpy, iterable_len=None, chunk_size=None, n_splits=None,
                                          n_jobs=None), min(100, cpu_count() * 4))

    def test_smaller_iterable_len(self):
        """
        Test iterable_len, where iterable_len < len(input)
        """
        with self.subTest(input='list'):
            self.assertEqual(get_n_chunks(self.test_data, iterable_len=5, chunk_size=None, n_splits=None, n_jobs=None),
                             min(5, cpu_count() * 4))
        with self.subTest(input='numpy'):
            self.assertEqual(get_n_chunks(self.test_data_numpy, iterable_len=5, chunk_size=None, n_splits=None,
                                          n_jobs=None), min(5, cpu_count() * 4))
        with self.subTest(input='generator/iterator'):
            self.assertEqual(get_n_chunks(iter(self.test_data), iterable_len=5, chunk_size=None, n_splits=None,
                                          n_jobs=None), min(5, cpu_count() * 4))

    def test_larger_iterable_len(self):
        """
        Test iterable_len, where iterable_len > len(input). Should ignores iterable_len when actual number of tasks is
        less, except when we use the data_generator function, in which case we cannot determine the actual number of
        elements.
        """
        with self.subTest(input='list'):
            self.assertEqual(get_n_chunks(self.test_data, iterable_len=25, chunk_size=None, n_splits=None, n_jobs=None),
                             min(13, cpu_count() * 4))
        with self.subTest(input='numpy'):
            self.assertEqual(get_n_chunks(self.test_data_numpy, iterable_len=125, chunk_size=None, n_splits=None,
                                          n_jobs=None), min(100, cpu_count() * 4))
        with self.subTest(input='generator/iterator'):
            self.assertEqual(get_n_chunks(iter(self.test_data), iterable_len=25, chunk_size=None, n_splits=None,
                                          n_jobs=None), min(25, cpu_count() * 4))

    def test_chunk_size(self):
        """
        Test chunk_size
        """
        for chunk_size, expected_n_chunks in [(1, 13), (3, 5)]:
            with self.subTest(input='list', chunk_size=chunk_size, expected_n_chunks=expected_n_chunks):
                self.assertEqual(get_n_chunks(self.test_data, iterable_len=None, chunk_size=chunk_size, n_splits=None,
                                              n_jobs=None), expected_n_chunks)

        for chunk_size, expected_n_chunks in [(1, 100), (3, 34)]:
            with self.subTest(input='list', chunk_size=chunk_size, expected_n_chunks=expected_n_chunks):
                self.assertEqual(get_n_chunks(self.test_data_numpy, iterable_len=None, chunk_size=chunk_size,
                                              n_splits=None, n_jobs=None), expected_n_chunks)

    def test_n_splits(self):
        """
        Test n_splits. n_jobs shouldn't have any influence
        """
        for n_splits, n_jobs in product([1, 6], [None, 2, 8]):
            with self.subTest(input='list', n_splits=n_splits, n_jobs=n_jobs):
                self.assertEqual(get_n_chunks(self.test_data, iterable_len=None, chunk_size=None, n_splits=n_splits,
                                              n_jobs=n_jobs), n_splits)

            with self.subTest(input='numpy', n_splits=n_splits, n_jobs=n_jobs):
                self.assertEqual(get_n_chunks(self.test_data_numpy, iterable_len=None, chunk_size=None,
                                              n_splits=n_splits, n_jobs=n_jobs), n_splits)

    def test_n_jobs(self):
        """
        When everything is None except n_jobs we should use n_jobs * 4 as number of splits. Again, taking into account
        the number of tasks
        """
        for n_jobs in [1, 6]:
            with self.subTest(input='list', n_jobs=n_jobs):
                self.assertEqual(get_n_chunks(self.test_data, iterable_len=None, chunk_size=None, n_splits=None,
                                              n_jobs=n_jobs), min(4 * n_jobs, len(self.test_data)))

            with self.subTest(input='numpy', n_jobs=n_jobs):
                self.assertEqual(get_n_chunks(self.test_data_numpy, iterable_len=None, chunk_size=None, n_splits=None,
                                              n_jobs=n_jobs), min(4 * n_jobs, len(self.test_data_numpy)))

    def test_chunk_size_priority_over_n_splits(self):
        """
        chunk_size should have priority over n_splits
        """
        with self.subTest(input='list', chunk_size=1, n_splits=6):
            self.assertEqual(get_n_chunks(self.test_data, iterable_len=None, chunk_size=1, n_splits=6, n_jobs=None), 13)
        with self.subTest(input='numpy', chunk_size=1, n_splits=6):
            self.assertEqual(get_n_chunks(self.test_data_numpy, iterable_len=None, chunk_size=1, n_splits=6,
                                          n_jobs=None), 100)

        with self.subTest(input='list', chunk_size=3, n_splits=3):
            self.assertEqual(get_n_chunks(self.test_data, iterable_len=None, chunk_size=3, n_splits=3, n_jobs=None), 5)
        with self.subTest(input='numpy', chunk_size=3, n_splits=3):
            self.assertEqual(get_n_chunks(self.test_data_numpy, iterable_len=None, chunk_size=3, n_splits=3,
                                          n_jobs=None), 34)

    def test_generator_input_with_no_iterable_len_raises(self):
        """
        When working with generators the iterable_len should be provided (the working examples are already tested above)
        """
        for chunk_size, n_splits, n_jobs in product([None, 1, 3], [None, 1, 3], [None, 1, 3]):
            with self.subTest(chunk_size=chunk_size, n_splits=n_splits, n_jobs=n_jobs), self.assertRaises(ValueError):
                get_n_chunks(iter(self.test_data), iterable_len=None, chunk_size=chunk_size, n_splits=n_splits,
                             n_jobs=n_jobs)


class MakeSingleArgumentsTest(unittest.TestCase):

    def test_make_single_arguments(self):
        """
        Tests the make_single_arguments function for different inputs
        """
        # Test for some different inputs
        for (args_in, args_out), generator in product(
                [(['a', 'c', 'b', 'd'], [('a',), ('c',), ('b',), ('d',)]),
                 ([1, 2, 3, 4, 5], [(1,), (2,), (3,), (4,), (5,)]),
                 ([(True,), (False,), (None,)], [((True,),), ((False,),), ((None,),)])],
                [False, True]
        ):
            # Transform
            args_transformed = make_single_arguments((arg for arg in args_in) if generator else args_in,
                                                     generator=generator)

            # Check type
            self.assertTrue(isinstance(args_transformed, types.GeneratorType if generator else list))

            # Check contents
            self.assertEqual(list(args_transformed), args_out)


class FormatSecondsTest(unittest.TestCase):

    def test_none_input(self):
        """
        When the input is None it should return an empty string
        """
        for with_milliseconds in [False, True]:
            with self.subTest(with_milliseconds=with_milliseconds):
                self.assertEqual(format_seconds(None, with_milliseconds=with_milliseconds), '')

    def test_without_milliseconds(self):
        """
        Test output without milliseconds
        """
        for seconds, expected_output in [(0, '0:00:00'), (1, '0:00:01'), (1.337, '0:00:01'), (2.9, '0:00:02'),
                                         (123456.78901234, '1 day, 10:17:36')]:
            with self.subTest(seconds=seconds):
                self.assertEqual(format_seconds(seconds, with_milliseconds=False), expected_output)

    def test_with_milliseconds(self):
        """
        Test output with milliseconds. Only shows them when they're actually needed.
        """
        for seconds, expected_output in [(0, '0:00:00'), (1, '0:00:01'), (1.337, '0:00:01.337'), (2.9, '0:00:02.900'),
                                         (123456.78901234, '1 day, 10:17:36.789')]:
            with self.subTest(seconds=seconds):
                self.assertEqual(format_seconds(seconds, with_milliseconds=True), expected_output)


class TimeItTest(unittest.TestCase):

    def test_array_storage(self):
        """
        TimeIt should write to the correct idx in the cum_time_array container. The max_time_array is a min-heap
        container, so the lowest value is stored at index 0. The single highest value in this case is stored at index 2
        """
        for array_idx in range(5):
            cum_time_array = [0, 0, 0, 0, 0]
            max_time_array = [(0, ''), (0, ''), (0, ''), (0, ''), (0, '')]
            MockDatetimeNow.RETURN_VALUES = [datetime(1970, 1, 1, 0, 0, 0, 0),
                                             datetime(1970, 1, 1, 0, 0, 4, 200000)]
            MockDatetimeNow.CURRENT_IDX = 0
            with self.subTest(array_idx=array_idx), patch('mpire.utils.datetime', new=MockDatetimeNow), \
                    TimeIt(cum_time_array, array_idx, max_time_array):
                pass
            self.assertListEqual([t for idx, t in enumerate(cum_time_array) if idx != array_idx], [0, 0, 0, 0])
            self.assertListEqual([t for idx, t in enumerate(max_time_array) if idx != 2],
                                 [(0, ''), (0, ''), (0, ''), (0, '')])
            self.assertEqual(cum_time_array[array_idx], 4.2)
            self.assertGreaterEqual(max_time_array[2], (4.2, None))

    def test_cum_time(self):
        """
        Using TimeIt multiple times should increase the cum_time_array
        """
        # These return values are used by TimeIt in order: start, end, start, end, ... So the first time the duration
        # will be 1 second, then 2 seconds, and 3 seconds.
        MockDatetimeNow.RETURN_VALUES = [datetime(1970, 1, 1, 0, 0, 0, 0),
                                         datetime(1970, 1, 1, 0, 0, 1, 0),
                                         datetime(1970, 1, 1, 0, 0, 0, 0),
                                         datetime(1970, 1, 1, 0, 0, 2, 0),
                                         datetime(1970, 1, 1, 0, 0, 0, 0),
                                         datetime(1970, 1, 1, 0, 0, 3, 0)]
        MockDatetimeNow.CURRENT_IDX = 0
        cum_time_array = [0]
        with patch('mpire.utils.datetime', new=MockDatetimeNow):
            with TimeIt(cum_time_array, 0):
                pass
            self.assertEqual(cum_time_array[0], 1.0)

            with TimeIt(cum_time_array, 0):
                pass
            self.assertEqual(cum_time_array[0], 3.0)

            with TimeIt(cum_time_array, 0):
                pass
            self.assertEqual(cum_time_array[0], 6.0)

    def test_max_time(self):
        """
        Using TimeIt multiple times should store the max duration value in the max_time_array using heapq. There's only
        room for the highest 5 values, while it is called 6 times. The smallest duration shouldn't be present.
        """
        # These return values are used by TimeIt in order: start, end, start, end, ... So the first time the duration
        # will be 1 second, then 2 seconds, 3 seconds, 3 seconds again, 0.5 seconds, and 10 seconds.
        MockDatetimeNow.RETURN_VALUES = [datetime(1970, 1, 1, 0, 0, 0, 0),
                                         datetime(1970, 1, 1, 0, 0, 1, 0),
                                         datetime(1970, 1, 1, 0, 0, 0, 0),
                                         datetime(1970, 1, 1, 0, 0, 2, 0),
                                         datetime(1970, 1, 1, 0, 0, 0, 0),
                                         datetime(1970, 1, 1, 0, 0, 3, 0),
                                         datetime(1970, 1, 1, 0, 0, 0, 0),
                                         datetime(1970, 1, 1, 0, 0, 3, 0),
                                         datetime(1970, 1, 1, 0, 0, 0, 0),
                                         datetime(1970, 1, 1, 0, 0, 0, 500000),
                                         datetime(1970, 1, 1, 0, 0, 0, 0),
                                         datetime(1970, 1, 1, 0, 0, 10, 0)]
        MockDatetimeNow.CURRENT_IDX = 0
        cum_time_array = [0]
        max_time_array = [(0, ''), (0, ''), (0, ''), (0, ''), (0, '')]
        with patch('mpire.utils.datetime', new=MockDatetimeNow):
            for _ in range(6):
                with TimeIt(cum_time_array, 0, max_time_array):
                    pass
            self.assertListEqual(max_time_array, [(1.0, None), (2.0, None), (10.0, None), (3.0, None), (3.0, None)])

    def test_format_args(self):
        """
        The format args func should be called when provided
        """
        for format_func, formatted in [(lambda: "1", "1"), (lambda: 2, 2), (lambda: "foo", "foo")]:
            # These return values are used by TimeIt in order: start, end, start, end, ... So the first time the
            # duration will be 1 second, then 2 seconds, and 3 seconds.
            MockDatetimeNow.RETURN_VALUES = [datetime(1970, 1, 1, 0, 0, 0, 0),
                                             datetime(1970, 1, 1, 0, 0, 1, 0),
                                             datetime(1970, 1, 1, 0, 0, 0, 0),
                                             datetime(1970, 1, 1, 0, 0, 2, 0),
                                             datetime(1970, 1, 1, 0, 0, 0, 0),
                                             datetime(1970, 1, 1, 0, 0, 3, 0)]
            MockDatetimeNow.CURRENT_IDX = 0
            with self.subTest(format_func=format_func), patch('mpire.utils.datetime', new=MockDatetimeNow):
                cum_time_array = [0]
                max_time_array = [(0, ''), (0, '')]
                for _ in range(3):
                    with TimeIt(cum_time_array, 0, max_time_array, format_func):
                        pass

                # The heapq only had room for two entries. The highest durations should be kept
                self.assertListEqual(max_time_array, [(2.0, formatted), (3.0, formatted)])
