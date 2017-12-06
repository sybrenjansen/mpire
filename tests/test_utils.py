import unittest


class UtilsTest(unittest.TestCase):

    def test_chunk_tasks(self):
        """
        Tests the chunk_tasks function
        """
        from mpire.utils import chunk_tasks

        # Test that a ValueError is raised when no chunk_size and n_splits are provided
        with self.assertRaises(ValueError):
            chunk_tasks([]).__next__()

        # Test that a valueError is raised when a generator is provided without iterable_len
        with self.assertRaises(ValueError):
            chunk_tasks(iter([]), n_splits=1).__next__()

        # Test that chunk_size is prioritized over n_splits
        chunks = list(chunk_tasks(range(4), chunk_size=4, n_splits=4))
        self.assertEqual(len(chunks), 1)
        self.assertEqual(len(chunks[0]), 4)

        # Test that the chunker is an empty generator for an empty input iterable
        chunks = list(chunk_tasks([], n_splits=5))
        self.assertEqual(len(chunks), 0)

        chunks = list(chunk_tasks(iter([]), iterable_len=0, n_splits=5))
        self.assertEqual(len(chunks), 0)

        # Test for cases where iterable_len does and does not match the number of arguments
        num_args = 10
        for iter_len in [5, 10, 20]:
            expected_args_sum = min(iter_len, num_args)
            chunks = list(chunk_tasks(range(num_args), iterable_len=iter_len, n_splits=1))
            self.assertEqual(len(chunks[0]), expected_args_sum)

            list(chunk_tasks(iter(range(num_args)), iterable_len=iter_len, n_splits=1))
            self.assertEqual(len(chunks[0]), expected_args_sum)

        # Test for len(args) {<, ==, >} n_splits
        n_splits = 5
        for num_args in [n_splits - 1, n_splits, n_splits + 1]:
            expected_n_chunks = n_splits if n_splits <= num_args else num_args

            # Test for normal list
            chunks = list(chunk_tasks(range(num_args), n_splits=n_splits))
            self.assertEqual(len(chunks), expected_n_chunks)

            # Test for generator
            chunks = list(chunk_tasks(iter(range(num_args)), iterable_len=num_args, n_splits=n_splits))
            self.assertEqual(len(chunks), expected_n_chunks)

        # Test that chunks are of the right size if chunk_size is provided:
        chunk_size = 3
        for num_args in [chunk_size - 1, chunk_size, chunk_size + 1]:
            chunks = list(chunk_tasks(range(num_args), chunk_size=chunk_size))
            for chunk in chunks[:-1]:
                self.assertEqual(len(chunk), chunk_size)
            self.assertTrue(len(chunks[-1]) <= chunk_size)

            chunks = list(chunk_tasks(iter(range(num_args)), chunk_size=chunk_size))
            for chunk in chunks[:-1]:
                self.assertEqual(len(chunk), chunk_size)
            self.assertTrue(len(chunks[-1]) <= chunk_size)
