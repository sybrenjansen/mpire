import itertools
import math
import multiprocessing as mp
import warnings
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Sized, Tuple, Type, Union

from tqdm import TqdmKeyError

from mpire.context import DEFAULT_START_METHOD, RUNNING_MACOS
from mpire.tqdm_utils import get_tqdm

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
    order_tasks: bool = False

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
            if RUNNING_MACOS:
                warnings.warn("Setting CPU affinity is not supported on MacOS. Ignoring cpu_ids parameter", 
                              RuntimeWarning)
            
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
                         worker_lifespan: Optional[int], progress_bar: bool, 
                         progress_bar_options: Optional[Dict[str, Any]], progress_bar_style: Optional[str],
                         task_timeout: Optional[float], worker_init_timeout: Optional[float],
                         worker_exit_timeout: Optional[float]) \
        -> Tuple[Optional[int], int, Optional[int], bool, Dict[str, Any]]:
    """
    Check the parameters provided to any (i)map function. Also extracts the number of tasks and can modify the
    ``chunk_size`` and ``progress_bar`` parameters.

    :param pool_params: WorkerPool config
    :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker
    :param iterable_len: Number of elements in the ``iterable_of_args``
    :param max_tasks_active: Maximum number of active tasks in the queue. Use ``None`` to not limit the queue
    :param chunk_size: Number of simultaneous tasks to give to a worker. If ``None`` it will generate ``n_jobs * 4``
        number of chunks
    :param n_splits: Number of splits to use when ``chunk_size`` is ``None``
    :param worker_lifespan: Number of chunks a worker can handle before it is restarted. If ``None``, workers will
        stay alive the entire time. Use this when workers use up too much memory over the course of time
    :param progress_bar: When ``True`` it will display a progress bar
    :param progress_bar_options: Dictionary containing keyword arguments to pass to the ``tqdm`` progress bar. See
         ``tqdm.tqdm()`` for details. The arguments ``total`` and ``leave`` will be overwritten by MPIRE.
    :param progress_bar_style: Style to use for the progress bar
    :param task_timeout: Timeout in seconds for a single task
    :param worker_init_timeout: Timeout in seconds for the ``worker_init`` function
    :param worker_exit_timeout: Timeout in seconds for the ``worker_exit`` function
    :return: Number of tasks, max tasks active, chunk size, progress bar, progress bar options
    """
    # Get number of tasks and check chunk_size and n_splits parameters
    n_tasks = get_number_of_tasks(iterable_of_args, iterable_len)
    check_number(chunk_size, 'chunk_size', allowed_types=(int, float), none_allowed=True, min_=1)
    check_number(n_splits, 'n_splits', allowed_types=(int,), none_allowed=True, min_=1)

    # If chunk_size and n_splits are not provided, use 64 * n_jobs chunks in total
    if chunk_size is None:
        if n_splits is not None and n_tasks is not None:
            chunk_size = n_tasks / n_splits
        else:
            if n_tasks is None:
                warnings.warn('Failed to obtain length of iterable when chunk size or number of splits is None. Chunk '
                              'size is set to 4. Remedy: either provide an iterable with a len() function or specify '
                              'iterable_len in the function call', RuntimeWarning, stacklevel=2)
                chunk_size = 4
            else:
                chunk_size = n_tasks / (pool_params.n_jobs * 64)

    # Check max_tasks_active parameter. If it is None, we set it to n_jobs * chunk_size * 2
    if max_tasks_active is None:
        max_tasks_active = pool_params.n_jobs * int(math.ceil(chunk_size)) * 2
    else:
        check_number(max_tasks_active, 'max_tasks_active', allowed_types=(int,), none_allowed=True, min_=1)

    # If worker lifespan is not None or not a positive integer, raise
    check_number(worker_lifespan, 'worker_lifespan', allowed_types=(int,), none_allowed=True, min_=1)

    # Check progress bar parameters and set default values
    progress_bar_options = check_progress_bar_options(progress_bar_options, n_tasks, progress_bar_style)

    # Timeout parameters can't be negative
    for timeout_var, timeout_var_name in [(task_timeout, 'task_timeout'),
                                          (worker_init_timeout, 'worker_init_timeout'),
                                          (worker_exit_timeout, 'worker_exit_timeout')]:
        check_number(timeout_var, timeout_var_name, allowed_types=(int, float), none_allowed=True, min_=1e-8)

    return n_tasks, max_tasks_active, chunk_size, progress_bar, progress_bar_options


def get_number_of_tasks(iterable_of_args: Union[Sized, Iterable], iterable_len: Optional[int]) -> Optional[int]:
    """
    Get the number of tasks to process. If iterable_len is provided, it will be used. Otherwise, if iterable_of_args
    is a Sized object, len(iterable_of_args) will be used. Otherwise, None will be returned.

    :param iterable_of_args: A numpy array or an iterable containing tuples of arguments to pass to a worker
    :param iterable_len: Number of elements in the ``iterable_of_args``
    :return: Number of tasks to process
    """
    if iterable_len is not None:
        return iterable_len

    if hasattr(iterable_of_args, '__len__'):
        return len(iterable_of_args)

    return None


def check_number(var: Any, var_name: str, allowed_types: Tuple[Type, ...], none_allowed: bool,
                 min_: Optional[float] = None) -> None:
    """
    Check that a variable is of the correct type and within the allowed range

    :param var: Variable to check
    :param var_name: Name of the variable
    :param allowed_types: Allowed types for the variable
    :param none_allowed: Whether None is allowed for the variable
    :param min_: Minimum value for the variable. If None, no minimum value is checked
    """
    if none_allowed and var is None:
        return
    if not isinstance(var, allowed_types):
        raise TypeError(f"{var_name} should be of type {allowed_types}")
    if min_ is not None and var < min_:  # type: ignore
        raise ValueError(f"{var_name} should be >= {min_}")


def check_progress_bar_options(progress_bar_options: Optional[Dict[str, Any]], n_tasks: Optional[int], 
                               progress_bar_style: Optional[str]) -> Dict[str, Any]:
    """
    Check that the progress bar options are properly formatted and set some defaults

    :param progress_bar_options: Dictionary containing keyword arguments to pass to the ``tqdm`` progress bar. See
         ``tqdm.tqdm()`` for details. The arguments ``total`` and ``leave`` will be overwritten by MPIRE.
    :param n_tasks: Number of tasks to process
    :param progress_bar_style: Progress bar style to use
    :return: Dictionary containing the progress bar options
    """
    # Progress bar options should be a dictionary. Issue a warning for "total" and "leave".
    progress_bar_options = progress_bar_options or {}
    if not isinstance(progress_bar_options, dict):
        raise TypeError("progress_bar_options should be a dictionary")
    if "total" in progress_bar_options:
        warnings.warn("The 'total' keyword argument is overwritten by MPIRE. Set the total number of tasks to process "
                      "using the iterable_len parameter", RuntimeWarning, stacklevel=2)
    if "leave" in progress_bar_options:
        warnings.warn("The 'leave' keyword argument will be overwritten by MPIRE", RuntimeWarning, stacklevel=2)

    # We currently do not support the position parameter for rich progress bars. Although this can be implemented by
    # using a single rich progress bar for all workers and using `add_task`, but this is not trivial to implement.
    if progress_bar_style == "rich" and "position" in progress_bar_options:
        raise NotImplementedError("The 'position' parameter is currently not supported for rich progress bars")

    # Set some defaults and overwrite others
    # NB We make a copy of the progress bar options, so that if the original dict is reused, redoing this check doesn't
    # raise warnings due to the "total" and "leave" being added.
    progress_bar_options = {
        # defaults
        "position": 0,
        "dynamic_ncols": True,
        "mininterval": 0.1,
        "maxinterval": 0.5,
        **progress_bar_options,
        # overrides
        "total": n_tasks,
        "leave": True
    }

    # Check if the tqdm progress bar style is valid
    tqdm = get_tqdm(progress_bar_style)

    # Check that all progress bar options are properly formatted. We need to do that here, because when an error occurs
    # within the progress bar handler it will deadlock (it's not technically impossible to do it there, but might as
    # well do it here)
    try:
        tqdm.check_options(progress_bar_options)
    except (TqdmKeyError, TypeError) as e:
        raise e from ValueError("There's an error in progress_bar_options. Either one of the parameters doesn't exist "
                                "or it's not properly formatted. See tqdm.tqdm() for details.")

    return progress_bar_options
