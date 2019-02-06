import types
import unittest
from itertools import product, repeat

import numpy as np

from mpire import cpu_count, tqdm, WorkerPool
from mpire.utils import get_n_chunks


def square(idx, x):
    return idx, x * x


def square_numpy(x):
    return x * x


def subtract(x, y):
    return x - y


def square_daemon(X):
    with WorkerPool(n_jobs=4) as pool:
        return pool.map(square, X, chunk_size=1)


def square_raises(_, x):
    raise ValueError(x)


def square_raises_on_idx(idx, x):
    if idx == 5:
        raise ValueError(x)
    else:
        return idx, x * x


def get_generator(iterable):
    yield from iterable


class MPIRETest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Create some test data. Note that the regular map reads the inputs as a list of single tuples (one argument),
        # whereas parallel.map sees it as a list of argument lists. Therefore we give the regular map a lambda function
        # which mimics the parallel.map behavior.
        self.test_data = list(enumerate([1, 2, 3, 5, 6, 9, 37, 42, 1337, 0, 3, 5, 0]))
        self.test_desired_output = list(map(lambda _args: square(*_args), self.test_data))
        self.test_data_len = len(self.test_data)

        # Numpy test data
        self.test_data_numpy = np.random.rand(100, 2)
        self.test_desired_output_numpy = square_numpy(self.test_data_numpy)
        self.test_data_len_numpy = len(self.test_data_numpy)

    def test_map(self):
        """
        Tests the map related function of the worker pool class
        """
        # Test results for different number of jobs to run in parallel and the maximum number of active tasks in the
        # queue
        for n_jobs, n_tasks_max_active, worker_lifespan, chunk_size, n_splits in \
                product([1, 2, None], [None, 2], [None, 2], [None, 3], [None, 3]):
            with WorkerPool(n_jobs=n_jobs) as pool:
                for map_func, sort, result_type in ((pool.map, False, list), (pool.map_unordered, True, list),
                                                    (pool.imap, False, types.GeneratorType),
                                                    (pool.imap_unordered, True, types.GeneratorType)):
                    # Test if parallel map results in the same as ordinary map function. Should work both for generators
                    # and iterators. Also check if an empty list works as desired.
                    results_list = map_func(square, self.test_data, max_tasks_active=n_tasks_max_active,
                                            worker_lifespan=worker_lifespan)
                    self.assertTrue(isinstance(results_list, result_type))
                    self.assertEqual(self.test_desired_output,
                                     sorted(results_list, key=lambda tup: tup[0]) if sort else list(results_list))

                    results_list = map_func(square, get_generator(self.test_data), iterable_len=self.test_data_len,
                                            max_tasks_active=n_tasks_max_active, worker_lifespan=worker_lifespan)
                    self.assertTrue(isinstance(results_list, result_type))
                    self.assertEqual(self.test_desired_output,
                                     sorted(results_list, key=lambda tup: tup[0]) if sort else list(results_list))

                    results_list = map_func(square, [], max_tasks_active=n_tasks_max_active,
                                            worker_lifespan=worker_lifespan)
                    self.assertTrue(isinstance(results_list, result_type))
                    self.assertEqual([], sorted(results_list, key=lambda tup: tup[0]) if sort else list(results_list))

                # Test numpy input. map should concatenate chunks of numpy output to a single output array if we
                # instruct it to
                results = pool.map(square_numpy, self.test_data_numpy, max_tasks_active=n_tasks_max_active,
                                   worker_lifespan=worker_lifespan, concatenate_numpy_output=True)
                self.assertTrue(isinstance(results, np.ndarray))
                np.testing.assert_array_equal(results, self.test_desired_output_numpy)

                # If we disable it we should get back chunks of the original array
                results = pool.map(square_numpy, self.test_data_numpy, max_tasks_active=n_tasks_max_active,
                                   worker_lifespan=worker_lifespan, concatenate_numpy_output=False)
                self.assertTrue(isinstance(results, list))
                np.testing.assert_array_equal(np.concatenate(results), self.test_desired_output_numpy)

                # Numpy concatenation doesn't exist for the other functions
                results = pool.imap(square_numpy, self.test_data_numpy, max_tasks_active=n_tasks_max_active,
                                    worker_lifespan=worker_lifespan)
                self.assertTrue(isinstance(results, types.GeneratorType))
                np.testing.assert_array_equal(np.concatenate(list(results)), self.test_desired_output_numpy)

                # map_unordered and imap_unordered cannot be checked for correctness as we don't know the order of the
                # returned results, except when n_jobs=1. In the other cases we could, however, check if all the values
                # (numpy rows) that are returned are present (albeit being in a different order)
                for map_func, result_type in ((pool.map_unordered, list), (pool.imap_unordered, types.GeneratorType)):
                    results = map_func(square_numpy, self.test_data_numpy, max_tasks_active=n_tasks_max_active,
                                       worker_lifespan=worker_lifespan)
                    self.assertTrue(isinstance(results, result_type))
                    concattenated_results = np.concatenate(list(results))
                    if n_jobs == 1:
                        np.testing.assert_array_equal(concattenated_results, self.test_desired_output_numpy)
                    else:
                        # We sort the expected and actual results using lexsort, which sorts using a sequence of keys.
                        # We transpose the array to sort on columns instead of rows.
                        np.testing.assert_array_equal(
                            concattenated_results[np.lexsort(concattenated_results.T)],
                            self.test_desired_output_numpy[np.lexsort(self.test_desired_output_numpy.T)]
                        )

        # Check if dictionary inputs behave in a correct way
        with WorkerPool(n_jobs=1) as pool:
            # Should work
            results_list = pool.map(subtract, [{'x': 5, 'y': 2}, {'y': 5, 'x': 2}])
            self.assertEqual(results_list, [3, -3])
        with WorkerPool(n_jobs=1) as pool:
            # Should throw
            with self.assertRaises(TypeError):
                pool.map(subtract, [{'x': 5, 'z': 2}])
        with WorkerPool(n_jobs=1) as pool:
            # Should throw
            with self.assertRaises(TypeError):
                pool.map(subtract, [{'x': 5, 'y': 2, 'z': 2}])

        # Zero (or a negative number of) active tasks/lifespan should result in a value error
        for n in [-3, -1, 0, 3.14]:
            with self.assertRaises(ValueError if isinstance(n, int) else TypeError):
                with WorkerPool(n_jobs=4) as pool:
                    pool.map(square, self.test_data, max_tasks_active=n)

            with self.assertRaises(ValueError if isinstance(n, int) else TypeError):
                with WorkerPool(n_jobs=4) as pool:
                    pool.map_unordered(square, self.test_data, max_tasks_active=n)

            with self.assertRaises(ValueError if isinstance(n, int) else TypeError):
                with WorkerPool(n_jobs=4) as pool:
                    for _ in pool.imap(square, self.test_data, max_tasks_active=n):
                        pass

            with self.assertRaises(ValueError if isinstance(n, int) else TypeError):
                with WorkerPool(n_jobs=4) as pool:
                    for _ in pool.imap_unordered(square, self.test_data, max_tasks_active=n):
                        pass

            with self.assertRaises(ValueError if isinstance(n, int) else TypeError):
                with WorkerPool(n_jobs=4) as pool:
                    pool.map(square, self.test_data, worker_lifespan=n)

            with self.assertRaises(ValueError if isinstance(n, int) else TypeError):
                with WorkerPool(n_jobs=4) as pool:
                    pool.map_unordered(square, self.test_data, worker_lifespan=n)

            with self.assertRaises(ValueError if isinstance(n, int) else TypeError):
                with WorkerPool(n_jobs=4) as pool:
                    for _ in pool.imap(square, self.test_data, worker_lifespan=n):
                        pass

            with self.assertRaises(ValueError if isinstance(n, int) else TypeError):
                with WorkerPool(n_jobs=4) as pool:
                    for _ in pool.imap_unordered(square, self.test_data, worker_lifespan=n):
                        pass

        # chunk_size should be an integer or None
        with self.assertRaises(TypeError):
            with WorkerPool(n_jobs=4) as pool:
                for _ in pool.imap(square, self.test_data, chunk_size='3'):
                    pass

        # chunk_size should be a positive integer
        with self.assertRaises(ValueError):
            with WorkerPool(n_jobs=4) as pool:
                for _ in pool.imap(square, self.test_data, chunk_size=-5):
                    pass

        # n_splits should be an integer or None
        with self.assertRaises(TypeError):
            with WorkerPool(n_jobs=4) as pool:
                for _ in pool.imap(square, self.test_data, n_splits='3'):
                    pass

        # n_splits should be a positive integer
        with self.assertRaises(ValueError):
            with WorkerPool(n_jobs=4) as pool:
                for _ in pool.imap(square, self.test_data, n_splits=-5):
                    pass

    def test_worker_id_shared_objects(self):
        """
        Tests passing the worker ID and shared objects
        """
        # Function with worker ID and shared objects
        def f1(_wid, _sobjects, _args, _n_jobs):
            self.assertTrue(isinstance(_wid, int))
            self.assertGreaterEqual(_wid, 0)
            self.assertLessEqual(_wid, _n_jobs)
            self.assertEqual(_sobjects, _args)

        # Function with worker ID
        def f2(_wid, _, _n_jobs):
            self.assertTrue(isinstance(_wid, int))
            self.assertGreaterEqual(_wid, 0)
            self.assertLessEqual(_wid, _n_jobs)

        # Function with shared objects
        def f3(_sobjects, _args, _n_jobs):
            self.assertEqual(_sobjects, _args, _n_jobs)

        # Function without worker ID and shared objects
        def f4(*_):
            pass

        for n_jobs, pass_worker_id, shared_objects in product([1, 2, 4], [False, True],
                                                              [None, (37, 42), ({'1', '2', '3'})]):

            with WorkerPool(n_jobs=n_jobs) as pool:
                # Configure pool
                pool.pass_on_worker_id(pass_worker_id)
                pool.set_shared_objects(shared_objects)

                # Tests should fail when number of arguments in function is incorrect, worker ID is not within range,
                # or when the shared objects are not equal to the given arguments
                f = f1 if pass_worker_id and shared_objects else (f2 if pass_worker_id else (f3 if shared_objects else
                                                                                             f4))
                pool.map(f, ((shared_objects, n_jobs) for _ in range(10)), iterable_len=10)

            # Pass on arguments using the constructor instead
            with WorkerPool(n_jobs=n_jobs, pass_worker_id=pass_worker_id, shared_objects=shared_objects) as pool:
                # Tests should fail when number of arguments in function is incorrect, worker ID is not within range,
                # or when the shared objects are not equal to the given arguments
                f = f1 if pass_worker_id and shared_objects else (f2 if pass_worker_id else (f3 if shared_objects else
                                                                                             f4))
                pool.map(f, ((shared_objects, n_jobs) for _ in range(10)), iterable_len=10)

    def test_worker_state(self):
        """
        Tests worker state
        """
        # Function with worker ID and worker state
        def f1(_wid, _wstate, _arg):
            self.assertTrue(isinstance(_wstate, dict))

            # Worker id should always be the same
            _wstate.setdefault('worker_id', set()).add(_wid)
            self.assertEqual(_wstate['worker_id'], {_wid})

            # Should contain previous args
            _wstate.setdefault('args', []).append(_arg)
            return _wid, len(_wstate['args'])

        # Function with worker ID, shared objects and worker state
        def f2(_wid, _sobjects, _wstate, _arg):
            # Check shared objects
            self.assertEqual(_sobjects, 1337)

            return f1(_wid, _wstate, _arg)

        # Function with worker ID (simply tests if WorkerPool correctly handles use_worker_state=False)
        def f3(_wid, _):
            pass

        for n_jobs, use_worker_state, shared_objects in product([1, 2, 4], [False, True], [None, 1337]):

            # Do not test use_worker_state=False and shared_objects=1337, that is already tested in
            # test_worker_id_shared_objects
            if not use_worker_state and shared_objects:
                continue

            for n_tasks in [0, 1, 3, 150]:
                with WorkerPool(n_jobs=n_jobs, pass_worker_id=True) as pool:
                    # Configure pool
                    pool.set_shared_objects(shared_objects)
                    pool.set_use_worker_state(use_worker_state)

                    # When use_worker_state is set, the final (worker_id, n_args) of each worker should add up to the
                    # number of given tasks
                    f = f1 if use_worker_state and not shared_objects else (f2 if use_worker_state and shared_objects
                                                                            else f3)
                    results = pool.map(f, range(n_tasks), chunk_size=2)
                    if use_worker_state:
                        n_processed_per_worker = [0] * n_jobs
                        for wid, n_processed in results:
                            n_processed_per_worker[wid] = n_processed
                        self.assertEqual(sum(n_processed_per_worker), n_tasks)

                # Pass on arguments using the constructor instead
                with WorkerPool(n_jobs=n_jobs, pass_worker_id=True, shared_objects=shared_objects,
                                use_worker_state=use_worker_state) as pool:
                    # When use_worker_state is set, the final (worker_id, n_args) of each worker should add up to the
                    # number of given tasks
                    f = f1 if use_worker_state and not shared_objects else (f2 if use_worker_state and shared_objects
                                                                            else f3)
                    results = pool.map(f, range(n_tasks), chunk_size=2)
                    if use_worker_state:
                        n_processed_per_worker = [0] * n_jobs
                        for wid, n_processed in results:
                            n_processed_per_worker[wid] = n_processed
                        self.assertEqual(sum(n_processed_per_worker), n_tasks)

    def test_daemon(self):
        """
        Tests nested WorkerPools
        """
        with WorkerPool(n_jobs=4, daemon=False) as pool:
            # Obtain results using nested WorkerPools
            results = pool.map(square_daemon, ((X,) for X in repeat(self.test_data, 4)), chunk_size=1)

            # Each of the results should match
            for results_list in results:
                self.assertTrue(isinstance(results_list, list))
                self.assertEqual(self.test_desired_output, results_list)

        # Daemon processes are not allowed to spawn children
        with self.assertRaises(AssertionError):
            with WorkerPool(n_jobs=4, daemon=True) as pool:
                # Obtain results using nested WorkerPools
                pool.map(square_daemon, ((X,) for X in repeat(self.test_data, 4)), chunk_size=1)

    def test_cpu_pinning(self):
        """
        Tests CPU pinning
        """
        for n_jobs, cpu_ids in product([None, 1, 2, 4], [None, [0], [0, 1], [0, 1, 2, 3], [[0, 3]], [[0, 1], [0, 1]]]):
            # Things should work fine when cpu_ids is None or number of cpu_ids given is one or equals the number of
            # jobs,
            if cpu_ids is None or len(cpu_ids) == 1 or len(cpu_ids) == (n_jobs or cpu_count()):
                with WorkerPool(n_jobs=n_jobs, cpu_ids=cpu_ids) as pool:
                    results_list = pool.map(square, self.test_data)
                    self.assertTrue(isinstance(results_list, list))
                    self.assertEqual(self.test_desired_output, results_list)
            # Should raise
            else:
                with self.assertRaises(ValueError):
                    WorkerPool(n_jobs=n_jobs, cpu_ids=cpu_ids)

        # Should raise when CPU IDs are out of scope
        with self.assertRaises(ValueError):
            WorkerPool(n_jobs=1, cpu_ids=[-1])
        with self.assertRaises(ValueError):
            WorkerPool(n_jobs=1, cpu_ids=[cpu_count()])

    def test_progress_bar(self):
        """
        Tests the progress bar
        """
        # This print statement is intentional as it will print multiple progress bars
        print()
        for n_jobs, progress_bar in product([None, 1, 2], [True, tqdm(total=len(self.test_data)),
                                                           tqdm(total=len(self.test_data), ascii=True), tqdm()]):
            # Should work just fine
            if progress_bar is True or progress_bar.total == len(self.test_data):
                with WorkerPool(n_jobs=n_jobs) as pool:
                    results_list = pool.map(square, self.test_data, progress_bar=progress_bar)
                    self.assertTrue(isinstance(results_list, list))
                    self.assertEqual(self.test_desired_output, results_list)

            # Should raise
            else:
                with self.assertRaises(ValueError):
                    with WorkerPool(n_jobs=n_jobs) as pool:
                        _ = pool.map(square, self.test_data, progress_bar=progress_bar)

        # Test with numpy, as that will change the number of tasks
        for n_jobs, progress_bar in product([None, 1, 2], [True, 1, 2, 3]):

            # Obtain progress bar. We create it here as it's dependent on n_jobs
            progress_bar = {True: lambda: True,
                            1: lambda: tqdm(total=get_n_chunks(self.test_data_numpy, n_jobs=n_jobs)),
                            2: lambda: tqdm(total=get_n_chunks(self.test_data_numpy, n_jobs=n_jobs), ascii=True),
                            3: lambda: tqdm()}.get(progress_bar)()

            # Should work just fine
            if progress_bar is True or progress_bar.total == get_n_chunks(self.test_data_numpy, n_jobs=n_jobs):
                with WorkerPool(n_jobs=n_jobs) as pool:
                    results = pool.map(square_numpy, self.test_data_numpy, progress_bar=progress_bar)
                    self.assertTrue(isinstance(results, np.ndarray))
                    np.testing.assert_array_equal(results, self.test_desired_output_numpy)

            # Should raise
            else:
                with self.assertRaises(ValueError):
                    with WorkerPool(n_jobs=n_jobs) as pool:
                        _ = pool.map(square_numpy, self.test_data_numpy, progress_bar=progress_bar)

        # Test with empty iterable (this failed before)
        with WorkerPool() as pool:
            self.assertListEqual(pool.map(square, [], progress_bar=True), [])

        print()

    def test_exceptions(self):
        """
        Tests if MPIRE can handle exceptions well
        """
        # This print statement is intentional as it will print multiple progress bars
        print()
        for n_jobs, n_tasks_max_active, worker_lifespan, progress_bar in product([1, 20], [None, 1], [None, 1],
                                                                                 [False, True]):
            # Should work for map like functions
            with self.assertRaises(ValueError):
                with WorkerPool(n_jobs=n_jobs) as pool:
                    _ = pool.map(square_raises, self.test_data, max_tasks_active=n_tasks_max_active,
                                 worker_lifespan=worker_lifespan, progress_bar=progress_bar)

            # Should work for imap like functions
            with self.assertRaises(ValueError):
                with WorkerPool(n_jobs=n_jobs) as pool:
                    list(pool.imap_unordered(square_raises, self.test_data, max_tasks_active=n_tasks_max_active,
                                             worker_lifespan=worker_lifespan, progress_bar=progress_bar))

            # Should work for map like functions
            with self.assertRaises(ValueError):
                with WorkerPool(n_jobs=n_jobs) as pool:
                    _ = pool.map(square_raises_on_idx, self.test_data, max_tasks_active=n_tasks_max_active,
                                 worker_lifespan=worker_lifespan, progress_bar=progress_bar)

            # Should work for imap like functions
            with self.assertRaises(ValueError):
                with WorkerPool(n_jobs=n_jobs) as pool:
                    list(pool.imap_unordered(square_raises_on_idx, self.test_data, max_tasks_active=n_tasks_max_active,
                                             worker_lifespan=worker_lifespan, progress_bar=progress_bar))
