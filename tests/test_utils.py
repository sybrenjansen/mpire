import unittest
from itertools import chain


class UtilsTest(unittest.TestCase):

    def test_chunk_tasks(self):
        """
        Tests the chunk_tasks function
        """
        from mpire.utils import chunk_tasks

        # Test that a ValueError is raised when no chunk_size and n_splits are provided
        with self.assertRaises(ValueError):
            next(chunk_tasks([]))

        # Test that a ValueError is raised when a generator is provided without iterable_len
        with self.assertRaises(ValueError):
            next(chunk_tasks(iter([]), n_splits=1))

        # Test that chunk_size is prioritized over n_splits
        chunks = list(chunk_tasks(range(4), chunk_size=4, n_splits=4))
        self.assertEqual(len(chunks), 1)
        self.assertEqual(len(chunks[0]), 4)
        self.assertEqual(list(range(4)), list(chain.from_iterable(chunks)))

        # Test that the chunker is an empty generator for an empty input iterable
        chunks = list(chunk_tasks([], n_splits=5))
        self.assertEqual(len(chunks), 0)

        chunks = list(chunk_tasks(iter([]), iterable_len=0, n_splits=5))
        self.assertEqual(len(chunks), 0)

        # Test for cases where iterable_len does and does not match the number of arguments
        num_args = 10
        for iter_len in [5, 10, 20]:
            expected_args_sum = min(iter_len, num_args)

            # Test for normal list (range is considered a normal list as it implements __len__ and such)
            chunks = list(chunk_tasks(range(num_args), iterable_len=iter_len, n_splits=1))
            total_args = sum(map(len, chunks))
            self.assertEqual(total_args, expected_args_sum)
            self.assertEqual(list(range(expected_args_sum)), list(chain.from_iterable(chunks)))

            # Test for an actual generator (range does not really behave like one)
            chunks = list(chunk_tasks(iter(range(num_args)), iterable_len=iter_len, n_splits=1))
            total_args = sum(map(len, chunks))
            self.assertEqual(total_args, expected_args_sum)
            self.assertEqual(list(range(expected_args_sum)), list(chain.from_iterable(chunks)))

        # Test for len(args) {<, ==, >} n_splits
        n_splits = 5
        for num_args in [n_splits - 1, n_splits, n_splits + 1]:
            expected_n_chunks = min(n_splits, num_args)

            # Test for normal list (range is considered a normal list as it implements __len__ and such)
            chunks = list(chunk_tasks(range(num_args), n_splits=n_splits))
            self.assertEqual(len(chunks), expected_n_chunks)
            self.assertEqual(list(range(num_args)), list(chain.from_iterable(chunks)))

            # Test for an actual generator (range does not really behave like one)
            chunks = list(chunk_tasks(iter(range(num_args)), iterable_len=num_args, n_splits=n_splits))
            self.assertEqual(len(chunks), expected_n_chunks)
            self.assertEqual(list(range(num_args)), list(chain.from_iterable(chunks)))

        # Test that chunks are of the right size if chunk_size is provided:
        chunk_size = 3
        for num_args in [chunk_size - 1, chunk_size, chunk_size + 1]:
            # Test for normal list (range is considered a normal list as it implements __len__ and such)
            chunks = list(chunk_tasks(range(num_args), chunk_size=chunk_size))
            for chunk in chunks[:-1]:
                self.assertEqual(len(chunk), chunk_size)
            self.assertLessEqual(len(chunks[-1]), chunk_size)
            self.assertEqual(list(range(num_args)), list(chain.from_iterable(chunks)))

            # Test for an actual generator (range does not really behave like one)
            chunks = list(chunk_tasks(iter(range(num_args)), chunk_size=chunk_size))
            for chunk in chunks[:-1]:
                self.assertEqual(len(chunk), chunk_size)
            self.assertLessEqual(len(chunks[-1]), chunk_size)
            self.assertEqual(list(range(num_args)), list(chain.from_iterable(chunks)))

    def test_make_single_arguments(self):
        """
        Tests the make_single_arguments function
        """
        from mpire.utils import make_single_arguments
        from itertools import product
        import types

        # Test for some different inputs
        for (args_in, args_out), gen in product([(['a', 'c', 'b', 'd'], [('a',), ('c',), ('b',), ('d',)]),
                                                 ([1, 2, 3, 4, 5], [(1,), (2,), (3,), (4,), (5,)]),
                                                 ([(True,), (False,), (None,)], [((True,),), ((False,),), ((None,),)])],
                                                [False, True]):
            # Transform
            args_transformed = make_single_arguments((arg for arg in args_in) if gen else args_in, generator=gen)

            # Check type
            self.assertTrue(isinstance(args_transformed, types.GeneratorType if gen else list))

            # Check contents
            self.assertEqual(list(args_transformed), args_out)

