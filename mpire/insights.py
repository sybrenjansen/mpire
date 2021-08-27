import ctypes
import math
import multiprocessing.context
import multiprocessing.managers
from datetime import datetime
from functools import partial
from typing import Dict, Optional, List, Tuple

from mpire.context import RUNNING_WINDOWS
from mpire.signal import ignore_keyboard_interrupt
from mpire.utils import format_seconds


class WorkerInsights:

    """
    Worker insights class for profiling the worker start up time, waiting time and working time. When worker init and
    exit functions are provided it will time those as well.
    """

    def __init__(self, ctx: multiprocessing.context.BaseContext, n_jobs: int) -> None:
        """
        Parameter class for worker insights.

        :param ctx: Multiprocessing context
        :param n_jobs: Number of workers
        """
        self.ctx = ctx
        self.n_jobs = n_jobs

        # Whether insights have been enabled or not
        self.insights_enabled = False

        # Multiprocessing Manager for storing the max_task_args in. A lock is needed to ensure no race conditions occur
        self.insights_manager = None
        self.insights_manager_lock = None

        # `datetime` object indicating at what time the Worker instance was created and started
        self.worker_start_up_time = None

        # Array object which holds the total number of seconds the workers take to start up
        self.worker_init_time = None

        # Array object which holds the total number of completed tasks per worker
        self.worker_n_completed_tasks = None

        # Array object which holds the total number of seconds the workers have been idle
        self.worker_waiting_time = None

        # Array object which holds the total number of seconds the workers are executing the task function
        self.worker_working_time = None

        # Array object which holds the total number of seconds the workers take to run the exit function
        self.worker_exit_time = None

        # Array object which holds the top 5 max task durations in seconds per worker
        self.max_task_duration = None

        # Manager.List object which holds the top 5 task arguments (string) for the longest task per worker
        self.max_task_args = None

    def reset_insights(self, enable_insights: bool) -> None:
        """
        Resets the insights containers

        :param enable_insights: Whether to enable worker insights
        """
        if enable_insights:
            # When we're on Windows, we don't use a Manager as it's giving authentication errors. The max_task_args
            # information is therefore not available on Windows systems. This needs to be fixed at some point in time
            if not RUNNING_WINDOWS:
                # We need to ignore the KeyboardInterrupt signal for the manager to avoid BrokenPipeErrors
                self.insights_manager = multiprocessing.managers.SyncManager(ctx=self.ctx)
                self.insights_manager.start(ignore_keyboard_interrupt)
            self.insights_manager_lock = self.ctx.Lock()
            self.worker_start_up_time = self.ctx.Array(ctypes.c_double, self.n_jobs, lock=False)
            self.worker_init_time = self.ctx.Array(ctypes.c_double, self.n_jobs, lock=False)
            self.worker_n_completed_tasks = self.ctx.Array(ctypes.c_int, self.n_jobs, lock=False)
            self.worker_waiting_time = self.ctx.Array(ctypes.c_double, self.n_jobs, lock=False)
            self.worker_working_time = self.ctx.Array(ctypes.c_double, self.n_jobs, lock=False)
            self.worker_exit_time = self.ctx.Array(ctypes.c_double, self.n_jobs, lock=False)
            self.max_task_duration = self.ctx.Array(ctypes.c_double, self.n_jobs * 5, lock=False)
            if RUNNING_WINDOWS:
                # Doesn't actually do anything, but reduces the amount of if/else statements in other code parts
                self.max_task_args = [""] * self.n_jobs * 5
            else:
                self.max_task_args = self.insights_manager.list([""] * self.n_jobs * 5)
        else:
            self.insights_manager = None
            self.insights_manager_lock = None
            self.worker_start_up_time = None
            self.worker_init_time = None
            self.worker_n_completed_tasks = None
            self.worker_waiting_time = None
            self.worker_working_time = None
            self.worker_exit_time = None
            self.max_task_duration = None
            self.max_task_args = None

        self.insights_enabled = enable_insights

    def get_max_task_duration_list(self, worker_id: int) -> Optional[List[Tuple[float, str]]]:
        """
        Initialize insights for a specific worker

        :param worker_id: worker ID
        """
        if self.insights_enabled:
            # Local worker insights container that holds (task duration, task args) tuples, sorted for heapq. We use a
            # local container for each worker as to not put too big of a burden on interprocess communication
            with self.insights_manager_lock:
                return (list(zip(self.max_task_duration[worker_id * 5:(worker_id + 1) * 5],
                                 self.max_task_args[worker_id * 5:(worker_id + 1) * 5]))
                        if self.max_task_duration is not None else None)

    def update_start_up_time(self, worker_id: int, start_time: datetime) -> None:
        """
        Update start up time

        :param worker_id: Worker ID
        :param start_time: datetime
        """
        if self.insights_enabled:
            self.worker_start_up_time[worker_id] = (datetime.now() - start_time).total_seconds()

    def update_n_completed_tasks(self, worker_id: int) -> None:
        """
        Increment the number of completed tasks for this worker

        :param worker_id: Worker ID
        """
        if self.insights_enabled:
            self.worker_n_completed_tasks[worker_id] += 1

    def update_task_insights(self, worker_id: int, max_task_duration_last_updated: datetime,
                             max_task_duration_list: Optional[List[Tuple[float, str]]],
                             force_update: bool = False) -> datetime:
        """
        Update synced containers with new top 5 max task duration + args. Updates every 2 seconds.

        :param worker_id: Worker ID
        :param max_task_duration_last_updated: Last updated datetime
        :param max_task_duration_list: Local worker insights container that holds (task duration, task args) tuples,
            sorted for heapq
        :param force_update: Whether to force the update
        :return: Last updated datetime
        """
        now = datetime.now()
        if self.insights_enabled and (force_update or (now - max_task_duration_last_updated).total_seconds() > 2):
            task_durations, task_args = zip(*max_task_duration_list)
            self.max_task_duration[worker_id * 5:(worker_id + 1) * 5] = task_durations
            with self.insights_manager_lock:
                self.max_task_args[worker_id * 5:(worker_id + 1) * 5] = task_args
            max_task_duration_last_updated = now

        return max_task_duration_last_updated

    def get_insights(self) -> Dict:
        """
        Creates insights from the raw insight data

        :return: dictionary containing worker insights
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

        if not self.insights_enabled:
            return {}

        format_seconds_func = partial(format_seconds, with_milliseconds=True)

        # Determine max 5 tasks based on duration, exclude zero values and args that haven't been synced yet (empty str)
        sorted_idx = argsort(self.max_task_duration)[-5:][::-1]
        top_5_max_task_durations, top_5_max_task_args = [], []
        for idx in sorted_idx:
            if self.max_task_duration[idx] == 0:
                break
            if self.max_task_args[idx] == "" and not RUNNING_WINDOWS:
                continue
            top_5_max_task_durations.append(format_seconds_func(self.max_task_duration[idx]))
            top_5_max_task_args.append("" if RUNNING_WINDOWS else self.max_task_args[idx])

        # Populate
        total_start_up_time = sum(self.worker_start_up_time)
        total_init_time = sum(self.worker_init_time)
        total_waiting_time = sum(self.worker_waiting_time)
        total_working_time = sum(self.worker_working_time)
        total_exit_time = sum(self.worker_exit_time)
        total_time = total_start_up_time + total_init_time + total_waiting_time + total_working_time + total_exit_time
        insights = dict(n_completed_tasks=list(self.worker_n_completed_tasks),
                        start_up_time=list(map(format_seconds_func, self.worker_start_up_time)),
                        init_time=list(map(format_seconds_func, self.worker_init_time)),
                        waiting_time=list(map(format_seconds_func, self.worker_waiting_time)),
                        working_time=list(map(format_seconds_func, self.worker_working_time)),
                        exit_time=list(map(format_seconds_func, self.worker_exit_time)),
                        total_start_up_time=format_seconds_func(total_start_up_time),
                        total_init_time=format_seconds_func(total_init_time),
                        total_waiting_time=format_seconds_func(total_waiting_time),
                        total_working_time=format_seconds_func(total_working_time),
                        total_exit_time=format_seconds_func(total_exit_time),
                        top_5_max_task_durations=top_5_max_task_durations,
                        top_5_max_task_args=top_5_max_task_args)

        insights['total_time'] = format_seconds_func(total_time)

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
        if not self.insights_enabled:
            return "No profiling stats available. Try to run a function first with insights enabled ..."

        insights = self.get_insights()
        insights_str = ["WorkerPool insights",
                        "-------------------",
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
        max_task_duration_args_separator = "" if RUNNING_WINDOWS else " - "
        for task_idx, (duration, args) in enumerate(zip(insights['top_5_max_task_durations'],
                                                        insights['top_5_max_task_args']), start=1):
            insights_str.append(f"{task_idx}. Time: {duration}{max_task_duration_args_separator}{args}")

        return "\n".join(insights_str)
