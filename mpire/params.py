import itertools
import multiprocessing as mp
import warnings
from typing import Any, Callable, Iterable, List, Optional, Sized, Tuple, Union

from dataclasses import dataclass, field
from tqdm import tqdm

from mpire.context import RUNNING_WINDOWS, DEFAULT_START_METHOD

# Typedefs
CPUList = List[Union[int, List[int]]]


@dataclass(init=True, frozen=False)
class WorkerPoolParams:
    """
    Data class for all :obj:`mpire.WorkerPool` parameters.
    """
    n_jobs: Optional[int]
    _n_jobs: int = field(init=False, repr=False)
    cpu_ids: Optional[CPUList]
    _cpu_ids: CPUList = field(init=False, repr=False)
    daemon: bool = True
    shared_objects: Any = None
    pass_worker_id: bool = False
    use_worker_state: bool = False
    start_method: str = DEFAULT_START_METHOD
    keep_alive: bool = False
    use_dill: bool = False
    enable_insights: bool = False

    @property
    def n_jobs(self) -> Optional[int]:
        return self._n_jobs

    @n_jobs.setter
    def n_jobs(self, n_jobs: Optional[int]) -> None:
        self._n_jobs = n_jobs or mp.cpu_count()

    @property
    def cpu_ids(self) -> CPUList:
        return self._cpu_ids

    @cpu_ids.setter
    def cpu_ids(self, cpu_ids: Optional[CPUList]) -> None:
        self._cpu_ids = self._check_cpu_ids(cpu_ids)

    def _check_cpu_ids(self, cpu_ids: Optional[CPUList]) -> CPUList:
        """
        Checks the cpu_ids parameter for correctness

        :param cpu_ids: List of CPU IDs to use for pinning child processes to specific CPUs. The list must be as long as
            the number of jobs used (if ``n_jobs`` equals ``None`` it must be equal to ``mpire.cpu_count()``), or the
            list must have exactly one element. In the former case, element x specifies the CPU ID(s) to use for child
            process x. In the latter case the single element specifies the CPU ID(s) for all child  processes to use. A
            single element can be either a single integer specifying a single CPU ID, or a list of integers specifying
            that a single child process can make use of multiple CPU IDs. If ``None``, CPU pinning will be disabled.
            Note that CPU pinning may only work on Linux based systems
        :return: cpu_ids
        """
        # Check CPU IDs
        converted_cpu_ids = []
        if cpu_ids:
            # Check number of arguments
            if len(cpu_ids) != 1 and len(cpu_ids) != self.n_jobs:
                raise ValueError("Number of CPU IDs (%d) does not match number of jobs (%d)" %
                                 (len(cpu_ids), self.n_jobs))

            # Convert CPU IDs to proper format and find the max and min CPU ID
            max_cpu_id = 0
            min_cpu_id = 0
            for cpu_id in cpu_ids:
                if isinstance(cpu_id, list):
                    converted_cpu_ids.append(cpu_id)
                    max_cpu_id = max(max_cpu_id, max(cpu for cpu in cpu_id))
                    min_cpu_id = min(min_cpu_id, min(cpu for cpu in cpu_id))
                elif isinstance(cpu_id, int):
                    converted_cpu_ids.append([cpu_id])
                    max_cpu_id = max(max_cpu_id, cpu_id)
                    min_cpu_id = min(min_cpu_id, cpu_id)
                else:
                    raise TypeError("CPU ID(s) must be either a list or a single integer")

            # Check max CPU ID
            if max_cpu_id >= mp.cpu_count():
                raise ValueError("CPU ID %d exceeds the maximum CPU ID available on your system: %d" %
                                 (max_cpu_id, mp.cpu_count() - 1))

            # Check min CPU ID
            if min_cpu_id < 0:
                raise ValueError("CPU IDs cannot be negative")

        # If only one item is given, use this item for all child processes
        if len(converted_cpu_ids) == 1:
            converted_cpu_ids = list(itertools.repeat(converted_cpu_ids[0], self.n_jobs))

        return converted_cpu_ids


@dataclass(init=True, frozen=True)
class WorkerMapParams:
    """
    Data class for all :meth:`mpire.WorkerPool.map` parameters that need to be passed on to a worker.
    """
    # User provided functions to call, provided to a map function
    func: Callable
    worker_init: Optional[Callable] = None
    worker_exit: Optional[Callable] = None

    # Number of (chunks of) jobs a child process can process before requesting a restart
    worker_lifespan: Optional[int] = None

    # Progress bar
    progress_bar: bool = False

    # Timeout in seconds for a single task, worker_init, and worker_exit
    task_timeout: Optional[float] = None
    worker_init_timeout: Optional[float] = None
    worker_exit_timeout: Optional[float] = None

    def __eq__(self, other: 'WorkerMapParams') -> bool:
        """
        :param other: Other WorkerMapConfig
        :return: Whether the configs are the same
        """
        if other.worker_init != self.worker_init or other.worker_exit != self.worker_exit:
            warnings.warn("You're changing either the worker_init and/or worker_exit function while keep_alive is "
                          "enabled. Be aware this can have undesired side-effects as worker_init functions are only "
                          "executed when a worker is started and worker_exit functions when a worker is terminated.",
                          RuntimeWarning, stacklevel=2)

        return (other.func == self.func and
                other.worker_init == self.worker_init and
                other.worker_exit == self.worker_exit and
                other.worker_lifespan == self.worker_lifespan and
                other.progress_bar == self.progress_bar and
                other.task_timeout == self.task_timeout and
                other.worker_init_timeout == self.worker_init_timeout and
                other.worker_exit_timeout == self.worker_exit_timeout)


def check_map_parameters(pool_params: WorkerPoolParams, iterable_of_args: Union[Sized, Iterable],
                         iterable_len: Optional[int], max_tasks_active: Optional[int],
                         chunk_size: Optional[Union[int, float]], n_splits: Optional[int],
                         worker_lifespan: Optional[int], progress_bar: bool, progress_bar_position: int,
                         task_timeout: Optional[float], worker_init_timeout: Optional[float],
                         worker_exit_timeout: Optional[float]) \
        -> Tuple[Optional[int], int, Optional[int], Union[bool, tqdm]]:
    """
    Check the parameters provided to any (i)map function. Also extracts the number of tasks and can modify the
    ``chunk_size`` and ``progress_bar`` parameters.

    :param pool_params: WorkerPool config
    :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker, which
        passes it to the function
    :param iterable_len: Number of elements in the ``iterable_of_args``. When chunk_size is set to ``None`` it needs
        to know the number of tasks. This can either be provided by implementing the ``__len__`` function on the
        iterable object, or by specifying the number of tasks
    :param max_tasks_active: Maximum number of active tasks in the queue. Use ``None`` to not limit the queue
    :param chunk_size: Number of simultaneous tasks to give to a worker. If ``None`` it will generate ``n_jobs * 4``
        number of chunks
    :param n_splits: Number of splits to use when ``chunk_size`` is ``None``
    :param worker_lifespan: Number of chunks a worker can handle before it is restarted. If ``None``, workers will
        stay alive the entire time. Use this when workers use up too much memory over the course of time
    :param progress_bar: When ``True`` it will display a progress bar
    :param progress_bar_position: Denotes the position (line nr) of the progress bar. This is useful wel using
        multiple progress bars at the same time
    :param task_timeout: Timeout in seconds for a single task
    :param worker_init_timeout: Timeout in seconds for the ``worker_init`` function
    :param worker_exit_timeout: Timeout in seconds for the ``worker_exit`` function
    :return: Number of tasks, max tasks active, chunk size, progress bar
    """
    # Get number of tasks
    n_tasks = None
    if iterable_len is not None:
        n_tasks = iterable_len
    elif hasattr(iterable_of_args, '__len__'):
        n_tasks = len(iterable_of_args)
    elif chunk_size is None or progress_bar:
        warnings.warn('Failed to obtain length of iterable when chunk size or number of splits is None and/or a '
                      'progress bar is requested. Chunk size is set to 1 and no progress bar will be shown. '
                      'Remedy: either provide an iterable with a len() function or specify iterable_len in the '
                      'function call', RuntimeWarning, stacklevel=2)
        chunk_size = 1
        progress_bar = False

    # Check chunk_size parameter
    if chunk_size is not None:
        if not isinstance(chunk_size, (int, float)):
            raise TypeError('chunk_size should be either None or a positive integer/float (> 0)')
        elif chunk_size <= 0:
            raise ValueError('chunk_size should be either None or a positive integer/float (> 0)')

    # Check n_splits parameter (only when chunk_size is None)
    else:
        if not (isinstance(n_splits, int) or n_splits is None):
            raise TypeError('n_splits should be either None or a positive integer (> 0)')
        if isinstance(n_splits, int) and n_splits <= 0:
            raise ValueError('n_splits should be either None or a positive integer (> 0)')

    # Check max_tasks_active parameter
    if max_tasks_active is None:
        max_tasks_active = pool_params.n_jobs * 2
    elif isinstance(max_tasks_active, int):
        if max_tasks_active <= 0:
            raise ValueError('max_tasks_active should be either None or a positive integer (> 0)')
    else:
        raise TypeError('max_tasks_active should be a either None or a positive integer (> 0)')

    # If worker lifespan is not None or not a positive integer, raise
    if isinstance(worker_lifespan, int):
        if worker_lifespan <= 0:
            raise ValueError('worker_lifespan should be either None or a positive integer (> 0)')
    elif worker_lifespan is not None:
        raise TypeError('worker_lifespan should be either None or a positive integer (> 0)')

    # Progress bar is currently not supported on Windows when using threading as start method. For reasons still
    # unknown we get a TypeError: cannot pickle '_thread.Lock' object.
    if RUNNING_WINDOWS and progress_bar and pool_params.start_method == "threading":
        raise ValueError("Progress bar is currently not supported on Windows when using start_method='threading'")

    # Progress bar position should be a positive integer
    if not isinstance(progress_bar_position, int):
        raise TypeError('progress_bar_position should be a positive integer (>= 0)')
    if progress_bar_position < 0:
        raise ValueError('progress_bar_position should be a positive integer (>= 0)')

    # Timeout parameters can't be negative
    for timeout_var, timeout_var_name in [(task_timeout, 'task_timeout'),
                                          (worker_init_timeout, 'worker_init_timeout'),
                                          (worker_exit_timeout, 'worker_exit_timeout')]:
        if timeout_var is not None:
            if not isinstance(timeout_var, (int, float)):
                raise TypeError(f'{timeout_var_name} should be either None or a positive integer/float (> 0)')
            if timeout_var <= 0:
                raise ValueError(f"{timeout_var_name} should be either None or a positive integer/float (> 0)")

    return n_tasks, max_tasks_active, chunk_size, progress_bar
