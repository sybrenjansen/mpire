from __future__ import annotations

import heapq
import math
import multiprocessing as mp
import time
from enum import Enum, auto
from functools import partial
from multiprocessing import shared_memory
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np

from mpire.utils import format_seconds

# TODO: use shared memory for insights. Then we don't need the additional queue and the progress bar can make use of 
# it!
# TODO: but then we do still need the max duration tasks to be communicated somehow. For that we will need to use the 
#   queue I guess. But only when insights are enabled!
# TODO: make use of this! Is this mpire 3?
# TODO: this still requires locking!!! Which we don't have
    # Well, we only need a single lock per worker I guess :O, which is also used by the comms class


class WorkerStatsType(Enum):
    START_UP_TIME = auto()
    INIT_TIME = auto()
    WAITING_TIME = auto()
    WORKING_TIME = auto()
    EXIT_TIME = auto()


class TaskWorkerComms:

    """
    Task and worker specific class for keeping track of number of completed tasks per worker and profiling the worker 
    start up time, waiting time and working time. When worker init and exit functions are provided it will time those 
    as well. Finally, it will track the top 5 tasks with the highest duration.
    """

    def __init__(self, job_id: int, lock: mp.Lock = None, shm_name: Optional[str] = None) -> None:
        """
        Parameter class for worker insights.

        :param job_id: Job ID
        :param shm_name: Shared memory name
        :param lock: Lock instance
        """
        self.job_id = job_id
        self.lock = lock
        
        # Stats array: [n_completed_tasks, start_up_time, init_time, waiting_time, working_time, exit_time]
        # 0: start_up_time: The total number of seconds the worker took to start up
        # 1: init_time: The total number of seconds the worker took to run the init function
        # 2: waiting_time: The total number of seconds the worker has been idle
        # 3: working_time: The total number of seconds the worker is executing the task function
        # 4: exit_time: The total number of seconds the worker took to run the exit function
        # 5: n_completed_tasks: The total number of completed tasks for this worker
        # 6: progress_bar_complete: Whether the progress bar is complete
        self.stats = np.array([0.0 for _ in range(7)], dtype=np.float64)
        
        # Create shared memory if shm_name is None, otherwise connect to existing shared memory
        self.shm = shared_memory.SharedMemory(name=shm_name, create=shm_name is None, size=self.stats.nbytes)    
        self.stats_shm = np.ndarray(self.stats.shape, dtype=self.stats.dtype, buffer=self.shm.buf)

        # The top 5 tasks with the highest duration for this worker. Each tuple consists of the duration in seconds and
        # the formatted task args (string). This list is local to the worker and will be communicated to the pool 
        # through a queue
        self.max_duration_tasks: List[Tuple[float, Optional[str]]] = [(0.0, None) for _ in range(5)]
        
        # Local variables needed to update the progress bar and the max duration tasks on set intervals. Updating them
        # too often will slow down the worker too much
        self.progress_bar_last_updated = 0.0
        self.progress_bar_n_tasks_completed = self.worker_n_completed_tasks
        self.max_duration_tasks_last_updated = 0.0
        
    def task_completed(self) -> None:
        """
        Increment the number of completed tasks
        """
        self.progress_bar_n_tasks_completed += 1
        
    def send_progress_bar_update(self, time_interval: float, force_update: bool = False) -> None:
        """
        Send the progress bar update to the shared memory
        
        :param time_interval: Time interval since the last update
        :param force_update: Whether to force an update
        """
        # Check if we need to update
        now = time.time()
        if force_update or (now - self.progress_bar_last_updated) > time_interval:
            with self.lock:
                self.stats_shm[5] = self.progress_bar_n_tasks_completed
            self.progress_bar_last_updated = now
            
    def signal_progress_bar_complete(self) -> None:
        """
        Signal that the progress bar is complete
        """
        with self.lock:
            self.stats_shm[6] = 1.0
            
    def wait_for_progress_bar_complete(self) -> None:
        """
        Wait until the progress bar is complete
        """
        while not self.progress_bar_complete:
            time.sleep(0.01)
        
    def add_duration(self, stats_type: WorkerStatsType, time: float) -> None:
        """
        Add duration to the worker insights
        
        :param stats_type: Worker stats type
        :param time: Time in seconds
        """
        with self.lock:
            if stats_type == WorkerStatsType.START_UP_TIME:
                self.stats_shm[0] += time
            elif stats_type == WorkerStatsType.INIT_TIME:
                self.stats_shm[1] += time
            elif stats_type == WorkerStatsType.WAITING_TIME:
                self.stats_shm[2] += time
            elif stats_type == WorkerStatsType.WORKING_TIME:
                self.stats_shm[3] += time
            elif stats_type == WorkerStatsType.EXIT_TIME:
                self.stats_shm[4] += time

    def add_max_duration_task(self, duration: float, formatted_args: str) -> None:
        """
        When a task takes longer than the current top 5 tasks, it will be added to the list
        
        :param duration: Duration in seconds
        :param formatted_args: Formatted task args
        """
        if duration > self.max_duration_tasks[0][0]:
            heapq.heappushpop(self.max_duration_tasks, (duration, formatted_args))
            
    @property
    def worker_start_up_time(self) -> float:
        return self.get_value(0)
    
    @property
    def worker_init_time(self) -> float:
        return self.get_value(1)
    
    @property
    def worker_waiting_time(self) -> float:
        return self.get_value(2)
    
    @property
    def worker_working_time(self) -> float:
        return self.get_value(3)
    
    @property
    def worker_exit_time(self) -> float:
        return self.get_value(4)
    
    @property
    def worker_n_completed_tasks(self) -> int:
        return int(self.get_value(5))
    
    @property
    def progress_bar_complete(self) -> bool:
        return bool(self.get_value(6))
        
    def get_value(self, idx: int) -> float:
        """
        Get the value at the given index either from the shared memory or the local stats
        
        :param idx: Index
        :return: Value
        """
        if self.stats_shm is not None:
            with self.lock:
                return float(self.stats_shm[idx])
        else:
            return float(self.stats[idx])
        
    def close_shm(self) -> None:
        """
        Close access to the shared memory, but don't destroy it
        """
        self.stats[:] = self.stats_shm[:]
        self.stats_shm = None
        self.shm.close()
            
    def destroy_shm(self) -> None:
        """
        Clean up the shared memory
        """
        self.close_shm()
        self.shm.unlink()


class TaskComms:
    
    """
    Task specific class that contains all the inter-process communication objects (locks, events, queues, etc.) and 
    functionality to interact with them, for a specific task.
    
    This contains of profiling the worker start up time, waiting time and working time. When worker init and exit 
    functions are provided it will time those as well.
    """

    def __init__(self, n_jobs: int, job_id: int, worker_locks: List[mp.Lock]) -> None:
        """
        Parameter class for worker insights.

        :param n_jobs: Number of workers
        :param job_id: Job ID
        :param worker_locks: List of worker locks
        """
        self.n_jobs = n_jobs
        self.job_id = job_id
        
        # List of TaskWorkerComms instances
        self.task_worker_comms = [TaskWorkerComms(job_id, lock) for _, lock in zip(range(n_jobs), worker_locks)]

        # Overall top 5 tasks with the highest duration. Each tuple consists of the duration in seconds and the 
        # formatted task args (string)
        self.max_duration_tasks = [(0.0, None) for _ in range(5)]
        
        # Local variables needed to update the progress bar on set intervals
        self.progress_bar_last_updated = 0.0
        
    def get_shm_names(self) -> List[str]:
        """
        Get the shared memory names for each worker
        
        :return: List of shared memory names
        """
        return [worker_comms.shm.name for worker_comms in self.task_worker_comms]
        
    def update_max_duration_tasks(self, max_duration_tasks: List[Tuple[float, Optional[str]]]) -> None:
        """
        Update max duration tasks

        :param max_duration_tasks: List of max duration tasks
        """
        for duration, args in max_duration_tasks:
            if duration > self.max_duration_tasks[0][0]:
                heapq.heappushpop(self.max_duration_tasks, (duration, args))
                
    def get_n_tasks_completed(self) -> int:
        """
        Get the total number of tasks completed
        
        :return: Total number of tasks completed
        """
        return sum(worker_comms.worker_n_completed_tasks for worker_comms in self.task_worker_comms)
    
    def signal_progress_bar_complete(self) -> None:
        """
        Signal that the progress bar is complete
        """
        [worker_comms.signal_progress_bar_complete() for worker_comms in self.task_worker_comms]
        
    def wait_for_progress_bar_complete(self) -> None:
        """
        Wait until the progress bar is complete
        """
        [worker_comms.wait_for_progress_bar_complete() for worker_comms in self.task_worker_comms]

    def get_insights(self) -> Dict:
        """
        Creates insights from the raw insight data

        :return: Dictionary containing worker insights
        """

        def argsort(seq):
            """
            argsort, as to not be dependent on numpy, by
            https://stackoverflow.com/questions/3382352/equivalent-of-numpy-argsort-in-basic-python/3382369#3382369
            """
            return sorted(range(len(seq)), key=seq.__getitem__)

        def mean_std(seq):
            """
            Calculates mean and standard deviation, as to not be dependent on numpy
            """
            _mean = sum(seq) / len(seq)
            _var = sum(pow(x - _mean, 2) for x in seq) / len(seq)
            _std = math.sqrt(_var)
            return _mean, _std

        format_seconds_func = partial(format_seconds, with_milliseconds=True)

        # Exclude zero values and args that haven't been synced yet (empty str)
        sorted_idx = argsort(self.max_duration_tasks)[::-1]
        top_5_max_task_durations, top_5_max_task_args = [], []
        for idx in sorted_idx:
            duration, args = self.max_duration_tasks[idx]
            if duration == 0:
                break
            top_5_max_task_durations.append(format_seconds_func(duration))
            top_5_max_task_args.append(args)

        # Populate
        total_start_up_time = sum(comms.worker_start_up_time for comms in self.task_worker_comms)
        total_init_time = sum(comms.worker_init_time for comms in self.task_worker_comms)
        total_waiting_time = sum(comms.worker_waiting_time for comms in self.task_worker_comms)
        total_working_time = sum(comms.worker_working_time for comms in self.task_worker_comms)
        total_exit_time = sum(comms.worker_exit_time for comms in self.task_worker_comms)
        total_time = total_start_up_time + total_init_time + total_waiting_time + total_working_time + total_exit_time
        insights = dict(
            job_id=self.job_id,
            n_completed_tasks=[comms.worker_n_completed_tasks for comms in self.task_worker_comms],
            start_up_time=[format_seconds_func(comms.worker_start_up_time) for comms in self.task_worker_comms],
            init_time=[format_seconds_func(comms.worker_init_time) for comms in self.task_worker_comms],
            waiting_time=[format_seconds_func(comms.worker_waiting_time) for comms in self.task_worker_comms],
            working_time=[format_seconds_func(comms.worker_working_time) for comms in self.task_worker_comms],
            exit_time=[format_seconds_func(comms.worker_exit_time) for comms in self.task_worker_comms],
            total_start_up_time=format_seconds_func(total_start_up_time),
            total_init_time=format_seconds_func(total_init_time),
            total_waiting_time=format_seconds_func(total_waiting_time),
            total_working_time=format_seconds_func(total_working_time),
            total_exit_time=format_seconds_func(total_exit_time),
            total_time=format_seconds_func(total_time),
            top_5_max_task_durations=top_5_max_task_durations,
            top_5_max_task_args=top_5_max_task_args
        )

        # Calculate ratio, mean and standard deviation of different parts of the worker lifespan
        for part, total in (('start_up', total_start_up_time),
                            ('init', total_init_time),
                            ('waiting', total_waiting_time),
                            ('working', total_working_time),
                            ('exit', total_exit_time)):
            mean, std = mean_std(getattr(self, f'worker_{part}_time'))
            insights[f'{part}_ratio'] = total / (total_time + 1e-8)
            insights[f'{part}_time_mean'] = format_seconds_func(mean)
            insights[f'{part}_time_std'] = format_seconds_func(std)

        return insights

    def get_insights_string(self) -> str:
        """
        Formats the worker insights_str and returns a string

        :return: worker insights_str string
        """
        insights = self.get_insights()
        title_str = f"WorkerPool insights (job ID: {self.job_id})"
        insights_str = [f"{title_str}",
                        f"{'-' * len(title_str)}",
                        f"Total number of tasks completed: {sum(insights['n_completed_tasks'])}"]

        # Format string for parts of the worker lifespan
        for part in ('start_up', 'init', 'waiting', 'working', 'exit'):
            insights_str.append(f"Total {part.replace('_', ' ')} time: {insights[f'total_{part}_time']}s ("
                                f"mean: {insights[f'{part}_time_mean']}, std: {insights[f'{part}_time_std']}, "
                                f"ratio: {insights[f'{part}_ratio'] * 100.:.2f}%)")

        # Add warning when working ratio is below 80%
        if insights['working_ratio'] < 0.8:
            insights_str.extend(["",
                                 "Efficiency warning: working ratio is < 80%!"])

        # Add stats per worker
        insights_str.extend(["",
                             "Stats per worker",
                             "----------------"])
        for worker_id in range(self.n_jobs):
            worker_str = [f"Worker {worker_id}",
                          f"Tasks completed: {insights['n_completed_tasks'][worker_id]}"]
            for part in ('start_up', 'init', 'waiting', 'working', 'exit'):
                worker_str.append(f"{part.replace('_', ' ')}: {insights[f'{part}_time'][worker_id]}s")
            insights_str.append(' - '.join(worker_str))

        # Add task stats
        insights_str.extend(["",
                             "Top 5 longest tasks",
                             "-------------------"])
        for task_idx, (duration, args) in enumerate(zip(insights['top_5_max_task_durations'],
                                                        insights['top_5_max_task_args']), start=1):
            insights_str.append(f"{task_idx}. Time: {duration} - {args}")

        return "\n".join(insights_str)
    
    def clean_up_shm(self) -> None:
        """
        Clean up the shared memory
        """
        for worker_insights in self.task_worker_comms:
            worker_insights.destroy_shm()
    
    
class TimeIt:

    """ Simple class that provides a context manager for keeping track of task duration """

    def __init__(self, worker_comms: Optional[TaskWorkerComms] = None, stats_type: Optional[WorkerStatsType] = None,
                 track_max_duration: bool = False, format_args_func: Optional[Callable] = None) -> None:
        """
        :param worker_comms: TaskWorkerComms instance
        :param stats_type: Worker stats type
        :param track_max_duration: Whether to track the maximum duration of the task
        :param format_args_func: Optional function which should return the formatted args corresponding to the function
            called within this context manager
        """
        self.worker_comms = worker_comms
        self.stats_type = stats_type
        self.track_max_duration = track_max_duration
        self.format_args_func = format_args_func
        self.start_time = None
        self.duration = None

    def __enter__(self) -> TimeIt:
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.duration = time.time() - self.start_time
        if self.worker_comms is not None:
            self.worker_comms.add_duration(self.stats_type, self.duration)
            if self.track_max_duration:
                self.worker_comms.add_max_duration_task(
                    self.duration,
                    self.format_args_func() if self.format_args_func is not None else None
                )
