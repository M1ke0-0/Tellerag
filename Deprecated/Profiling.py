import time, asyncio, threading, tracemalloc, multiprocessing
from functools import wraps
from typing import Callable, Any, Optional, Dict, Coroutine, List
import psutil


class Profiler:
    """
    A generic profiler to monitor execution metrics for sycnhronous and asynchronous functions,
    and periodic system-wide stats (CPU, memory, I/O).
    """

    _instance: Optional['Profiler'] = None
    @classmethod
    def get_instance(cls) -> 'Profiler':
        if cls._instance is None:
            raise ValueError("Profiler instance not created. Create one.")
        return cls._instance

    def __init__(self, interval: float = 1.0, snapshots: int = 10):
        self.processes_to_profile = []
        self.snapshots = snapshots
        self.interval = interval
        self._task_stats: Dict[str, Dict[str, Any]] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None
        tracemalloc.start()
        self.cpu_usage_snapshots = []
        self.load_average = 0.0
        self.processes_stats = {}

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._collect_loop, daemon=True)
        self._thread.start()

    def put_processes(self, pids: List[int]):
        """
        Adds a list of processes to the list of processes to profile.
        """
        self.processes_to_profile = pids
    def update_process(self):
        """
        Updates the process to profile.
        """
        for key in self.processes_stats.keys():
            if key not in self.processes_to_profile:
                del self.processes_stats[key]


    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join()
        tracemalloc.stop()

    def _collect_loop(self):
        process = psutil.Process()
        while self._running:
            cpu = process.cpu_percent(interval=self.interval)
            memory = process.memory_info().rss
            current, peak = tracemalloc.get_traced_memory()
            self.cpu_usage_snapshots.append(cpu)
            if len(self.cpu_usage_snapshots) == self.snapshots:
                self.load_average = sum(self.cpu_usage_snapshots) / len(self.cpu_usage_snapshots)

            self._system_stats = {
                "cpu": cpu,
                "memory": memory,
                "current_malloc": current,
                "peak_malloc": peak,
                "time": time.time(),
            }
            tracemalloc.reset_peak()
            self.update_process()
            for pid in self.processes_to_profile:
                self.profile_process(pid)
            time.sleep(self.interval)

    def profile_process(self, pid: int):
        """
        Profiles a process by its PID.
        """
        try:
            process = psutil.Process(pid)
            self.processes_stats[pid] = {
                "cpu": process.cpu_percent(interval=self.interval),
                "memory": process.memory_info().rss,
                "io_counters": process.io_counters(),
                "threads": process.threads(),
                "open_files": process.open_files(),
            }
        except psutil.NoSuchProcess:
            if pid in self.processes_to_profile:
                del self.processes_stats[pid]
                self.processes_to_profile.remove(pid)

    def profile_func(self, name: Optional[str] = None) -> Callable:
        """
        Decorator to profile a function or coroutine.
        """
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            key = name or func.__qualname__
            stats = {
                'count' : 0,
                'total_time' : 0.0,
                'errors': 0
            }
            self._task_stats[key] = stats
            if asyncio.iscoroutinefunction(func):
                @wraps(func)
                async def wrapper(*args, **kwargs) -> Any:
                    start_time = time.monotonic()
                    try:
                        result = await func(*args, **kwargs)
                        return result
                    except Exception:
                        stats['errors'] += 1
                        raise
                    finally:
                        elapsed_time = time.monotonic() - start_time
                        stats['count'] += 1
                        stats['total_time'] += elapsed_time
                return wrapper
            else:
                @wraps(func)
                def wrapper(*args, **kwargs) -> Any:
                    start_time = time.monotonic()
                    try:
                        result = func(*args, **kwargs)
                        return result
                    except Exception:
                        stats['errors'] += 1
                        raise
                    finally:
                        elapsed_time = time.monotonic() - start_time
                        stats['count'] += 1
                        stats['total_time'] += elapsed_time
                return wrapper
        return decorator

    def get_task_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Returns the current task stats.
        """
        return self._task_stats

    def get_system_stats(self):
        return getattr(self, '_system_stats', {})

    def get_process_stats(self):
        return self.processes_stats

    def get_load_average(self):
        return self.load_average




