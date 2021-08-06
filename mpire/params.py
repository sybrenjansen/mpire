import itertools
import multiprocessing as mp
import warnings
from typing import Any, Callable, Iterable, List, Optional, Sized, Tuple, Union

from tqdm import tqdm

# Typedefs
CPUList = Optional[List[Union[int, List[int]]]]


class WorkerPoolParams:

    """
    Data class, with some parameter verification, for all :obj:`mpire.WorkerPool` parameters.
    """

    def __init__(self, n_jobs: Optional[int] = None, daemon: bool = True, cpu_ids: CPUList = None,
                 shared_objects: Any = None, pass_worker_id: bool = False, use_worker_state: bool = False,
                 start_method: str = 'fork', keep_alive: bool = False, use_dill: bool = False) -> None:
        """
        See ``WorkerPool.__init__`` docstring.
        """
        self.n_jobs = n_jobs or mp.cpu_count()
        self.daemon = daemon
        self.cpu_ids = self._check_cpu_ids(cpu_ids)
        self.shared_objects = shared_objects
        self.pass_worker_id = pass_worker_id
        self.use_worker_state = use_worker_state
        self.start_method = start_method
        self.keep_alive = keep_alive
        self.use_dill = use_dill

        # Number of (chunks of) jobs a child process can process before requesting a restart
        self.worker_lifespan = None

        # User provided functions to call
        self.func = None
        self.worker_init = None
        self.worker_exit = None
        self.enable_insights = None

    def _check_cpu_ids(self, cpu_ids: CPUList) -> List[List[int]]:
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

    def check_map_parameters(self, iterable_of_args: Union[Sized, Iterable], iterable_len: Optional[int],
                             max_tasks_active: Optional[int], chunk_size: Optional[Union[int, float]],
                             n_splits: Optional[int], worker_lifespan: Optional[int], progress_bar: bool,
                             progress_bar_position: int) \
            -> Tuple[Optional[int], int, Optional[int], Union[bool, tqdm]]:
        """
        Check the parameters provided to any (i)map function. Also extracts the number of tasks and can modify the
        ``chunk_size`` and ``progress_bar`` parameters.

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
                raise TypeError('chunk_size should be either None or an integer value')
            elif chunk_size <= 0:
                raise ValueError('chunk_size should be a positive integer > 0')

        # Check n_splits parameter (only when chunk_size is None)
        else:
            if not (isinstance(n_splits, int) or n_splits is None):
                raise TypeError('n_splits should be either None or an integer value')
            if isinstance(n_splits, int) and n_splits <= 0:
                raise ValueError('n_splits should be a positive integer > 0')

        # Check max_tasks_active parameter
        if max_tasks_active is None:
            max_tasks_active = self.n_jobs * 2
        elif isinstance(max_tasks_active, int):
            if max_tasks_active <= 0:
                raise ValueError('max_tasks_active should be a positive integer or None')
        else:
            raise TypeError('max_tasks_active should be a positive integer or None')

        # If worker lifespan is not None or not a positive integer, raise
        if isinstance(worker_lifespan, int):
            if worker_lifespan <= 0:
                raise ValueError('worker_lifespan should be either None or a positive integer (> 0)')
        elif worker_lifespan is not None:
            raise TypeError('worker_lifespan should be either None or a positive integer (> 0)')

        # Progress bar position should be a positive integer
        if not isinstance(progress_bar_position, int):
            raise TypeError('progress_bar_position should be a positive integer (>= 0)')
        if progress_bar_position < 0:
            raise ValueError('progress_bar_position should be a positive integer (>= 0)')

        return n_tasks, max_tasks_active, chunk_size, progress_bar

    def set_map_params(self, func: Callable, worker_init: Optional[Callable], worker_exit: Optional[Callable],
                       worker_lifespan: int, enable_insights: bool) -> None:
        """
        Set map specific parameters

        :param func: Function to call each time new task arguments become available
        :param worker_init: Function to call each time a new worker starts
        :param worker_exit: Function to call each time a worker exits
        :param worker_lifespan: Number of chunks a worker can handle before it is restarted
        :param enable_insights: Whether to enable worker insights
        """
        self.func = func
        self.worker_init = worker_init
        self.worker_exit = worker_exit
        self.worker_lifespan = worker_lifespan
        self.enable_insights = enable_insights

    def workers_need_restart(self, func: Callable, worker_init: Optional[Callable], worker_exit: Optional[Callable],
                             worker_lifespan: int, enable_insights: bool) -> bool:
        """
        Checks if workers need to be restarted based on some key parameters

        :param func: Function to call each time new task arguments become available
        :param worker_init: Function to call each time a new worker starts
        :param worker_exit: Function to call each time a worker exits
        :param worker_lifespan: Number of chunks a worker can handle before it is restarted
        :param enable_insights: Whether to enable worker insights
        :return: Whether the workers need to be restarted
        """
        return (func != self.func or
                worker_init != self.worker_init or
                worker_exit != self.worker_exit or
                worker_lifespan != self.worker_lifespan or
                enable_insights != self.enable_insights)
