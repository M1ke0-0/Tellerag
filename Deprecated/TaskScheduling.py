"""
TaskScheduling Module Documentation
===================================

Overview:
---------
This module implements a robust task scheduling and execution framework that combines
multiprocessing with asynchronous programming. It enables efficient execution of both
synchronous and asynchronous tasks with support for task dependencies, chaining,
priority-based scheduling, and resource management.

Key Components:
--------------
1. Task
   - Represents an individual unit of work with its own execution context
   - Supports both synchronous and asynchronous function execution
   - Features automatic dependency resolution between tasks
   - Manages result propagation and exception handling
   - Implements priority-based scheduling with dynamic priority adjustment

2. TaskChain
   - Connects multiple tasks in a sequential execution chain
   - Automatically handles result passing between linked tasks
   - Manages task relationships (predecessors and successors)
   - Enforces chain integrity with a maximum length of 15 tasks

3. Worker (Process Management)
   - Extends multiprocessing.Process to handle task execution
   - Maintains a state lifecycle (IDLE, BUSY, STOPPED, TERMINATED)
   - Implements asynchronous logging for operational transparency
   - Manages task execution timeouts and graceful termination
   - Supports both synchronous and asynchronous task execution

4. ProcessCompositor
   - Orchestrates worker pools for different task types
   - Dynamically creates and terminates workers based on system load
   - Implements CPU usage monitoring to prevent system overload
   - Handles worker dispatch and result collection
   - Converts human-readable timeout specifications to seconds

5. TaskScheduler
   - Maintains a priority queue for task execution
   - Implements a singleton pattern for global access
   - Manages task submission and dispatch to workers
   - Coordinates with ProcessCompositor for actual task execution

6. CoreMultiprocessing
   - Provides a simplified interface for task submission
   - Handles both individual tasks and task chains
   - Abstracts away scheduler details for client code

Execution Flow:
--------------
1. Tasks are created with functions, arguments, and optional dependencies
2. Tasks can be chained together for sequential execution
3. Tasks or chains are submitted to the TaskScheduler
4. The scheduler prioritizes and dispatches tasks to the ProcessCompositor
5. The compositor assigns tasks to available workers or creates new workers
6. Workers execute tasks and return results to the compositor
7. Results propagate back to the original tasks, resolving dependencies

Performance Features:
-------------------
- Dynamic worker creation and termination based on system load
- CPU usage monitoring to prevent system overload
- Priority-based scheduling with aging mechanism
- Separate worker pools for synchronous and asynchronous tasks
- Efficient worker idle timeout management

Error Handling:
--------------
- Comprehensive exception capture and propagation
- Task-level exception containment
- Detailed logging of task execution status
- Graceful worker termination on errors

Usage Examples:
--------------
1. Creating and submitting a simple task:
   ```
   task = Task("example_task", my_function, args=(arg1, arg2), kwargs={"param": value})
   CoreMultiprocessing.push_task(task)
   result = task.get()  # Blocks until task completes
   ```

2. Creating a task chain:
   ```
   task1 = Task("first_task", step_one)
   task2 = Task("second_task", step_two, arg_deps=["result"])
   chain = TaskChain(task1, task2)
   CoreMultiprocessing.push_chain(chain)
   final_result = chain.get()  # Blocks until all tasks complete
   ```

3. Working with async functions:
   ```
   async_task = Task("async_example", my_async_function)
   CoreMultiprocessing.push_task(async_task)
   result = async_task.get()
   ```
"""
import enum, multiprocessing as mp, time, heapq, asyncio, inspect, psutil
from typing import Callable, Optional, Any, Tuple, List, Dict, Iterable
from source.Logging import Logger, LogLevel
from dataclasses import dataclass
from Deprecated.CoreUtils import time_type_dict
from Deprecated.Profiling import Profiler

task_scheduler_logger = Logger("TaskScheduler", "runtime.log")
process_logger = Logger("Process", "runtime.log")
compositor_logger = Logger("Compositor", "runtime.log")

process_state_dict = MappingProxyType({
    ProcessState.IDLE: "Idle",
    ProcessState.BUSY: "Busy",
    ProcessState.STOPPED: "Stopped"
})

class Task:
    def __init__(
            self,
            name: str,
            func: Callable[..., Any],
            args: tuple = (),
            kwargs: Optional[dict] = None,
            base_priority: float = 0.0,
            arg_deps: Optional[list[str]] = None,
    ) -> None:
        self.name = name
        self.func = func
        self.args = args
        self.kwargs = kwargs if kwargs is not None else {}
        self.base_priority = base_priority
        self._priority = base_priority
        self._enqueue_time = time.monotonic()
        self.is_async = inspect.iscoroutinefunction(func)
        self.result = None # Placeholder for result
        self.exception = None # placeholder for exception handling
        self.result_event = mp.Event()
        self._next = None
        self._past = None
        self._arg_deps = arg_deps or None
        self._prev_result = None
        if not self._arg_deps:
            self.resolved = True
        else:
            self.resolved = False

    def resolve(self):
        if self._prev_result is None:
            self._prev_result = self._past.get()
        if self._prev_result is Iterable:
            if self._prev_result is Dict:
                for key in self._arg_deps:
                    if key not in self._prev_result:
                        raise RuntimeError(f"Dependency {key} not found in previous task result")
                    self.kwargs[key] = self._prev_result[key]
            else:
                for i, key in enumerate(self._arg_deps):
                    if i >= len(self._prev_result):
                        raise RuntimeError(f"Dependency {key} not found in previous task result")
                    self.kwargs[key] = self._prev_result[i]
        self.resolved = True

    def put_result(self, result: Any) -> None:
        if self.result is None:
            self.result = result
            self.result_event.set()

    def set_prev(self, prev_task: Optional["Task"]) -> None:
            self._past = prev_task

    def set_next(self, next_task: Optional["Task"]) -> None:
            self._next = next_task

    def put_exception(self, exception: Exception) -> None:
        if self.exception is None:
            self.exception = exception
            self.result_event.set()
    @property
    def next(self) -> Optional["Task"]:
        return self._next

    @property
    def past(self) -> Optional["Task"]:
        return self._past

    @property
    def priority(self) -> float:
        wait = time.monotonic() - self._enqueue_time
        return self._priority + wait

    async def _run_async(self) -> Any:
        if not self.resolved:
            self.resolve()
        return await self.func(*self.args, **self.kwargs)

    def _run_sync(self):
        if not self.resolved:
            self.resolve()
        return self.func(*self.args, **self.kwargs)

    def run(self) -> Any:
        if self.is_async:
            return RuntimeError("Use scheduler to run coroutine tasks")
        return self._run_async()

    def get(self):
        self.result_event.wait()
        if self.exception:
            raise self.exception
        return self.result

    def __repr__(self):
        return f"Task(func={self.func.__name__}, priority={self.priority:.2f})"


class TaskChain:
    def __init__(self, *args: Task):
        self.tasks = args
        self.head = None
        self.tail = None
        if len(self.tasks) > 15:
            raise RuntimeError("Task chain too long. Maximum length is 15 tasks.")
        for task in self.tasks:
            if self.head is None:
                self.head = task
                self.tail = task
            else:
                self.tail.set_next(task)
                task.set_prev(self.tail)
                self.tail = task

    def __delete__(self, instance):
        current = self.head
        while current is not None:
            next_task = current.next
            current.set_next(None)
            current.set_prev(None)
            current = next_task
        self.head = None
        self.tail = None

    def unzip(self):
        tasks = []
        current = self.head
        while current is not None:
            tasks.append(current)
            current = current.next
        return tasks

    def get(self):
        return self.tail.get()

class ProcessState(enum.Enum):
    IDLE = 0
    BUSY = 1
    STOPPED = 2
    TERMINATED = 3

class PSig(enum.Enum):
    STOP = 0
    RESUME = 1
    TERMINATE = 2
    KILL = 3

class Worker(mp.Process):
    def __init__(
            self,
            is_async: bool,
            task_queue: mp.Queue,
            result_queue: mp.Queue,
            name: str = f"Worker",
            timeout: int = 10,
    ) -> None:
        super().__init__(daemon=True)
        self.name = name + "_" +str(self.pid) + "_async" if is_async else "_sync"
        self.is_async = is_async
        self.task_queue = task_queue
        self.result_queue = result_queue
        self._idle_timeout = timeout
        self._logger_task = None
        self.log_queue = asyncio.Queue()
        self._state = ProcessState.IDLE
        self._stop_event = mp.Event()

    def run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(self._consumer(loop))
        loop.create_task(self._logging_execution(loop))
        loop.run_forever()

    async def _log(self, level: LogLevel, msg: str):
        await self.log_queue.put((level, msg))

    async def _consumer(self, loop) -> None:
        await loop.run_in_executor(None, self._log(LogLevel.INFO, "Worker started"))
        task_buffer = None # Only for chained tasks
        while True:
            start_time = time.monotonic()
            if self._stop_event.is_set():
                self._stop_event.clear()
                await loop.run_in_executor(None, self._log(LogLevel.INFO, f"Worker {self.name} stopped."))
                self._state = ProcessState.STOPPED
                await asyncio.to_thread(self._stop_event.wait)
                self._state = ProcessState.IDLE
                await loop.run_in_executor(None, self._log(LogLevel.INFO, f"Worker {self.name} resumed."))
            if task_buffer is not None:
                task = task_buffer
            else:
                task: Task = await loop.run_in_executor(None, self.task_queue.get, self._idle_timeout)
                if task is None:
                    elapsed_time = time.monotonic() - start_time
                    if elapsed_time >= self._idle_timeout:
                        self.stop()
                    else:
                        break
            try:
                self._state = ProcessState.BUSY
                if self.is_async:
                    result = await loop.run_in_executor(None, task._run_async)
                else:
                    result = task.run()
                if task._next is not None:
                    task_buffer = task._next
                self.result_queue.put((task, result, None))
                await loop.run_in_executor(None, self._log(LogLevel.INFO, f"Task {task.name} completed successfully"))
                self._state = ProcessState.IDLE
            except Exception as e:
                self.result_queue.put((task, None, e))
                await loop.run_in_executor(None, self._log(LogLevel.ERROR, f"Task {task.name} failed"))
                self._state = ProcessState.IDLE
        loop.stop()
        await loop.run_in_executor(None, self._log(LogLevel.INFO, "Worker stopped"))
        self.log_queue.put_nowait(None)
        self._state = ProcessState.TERMINATED

    async def _logging_execution(self, loop) -> None:
        await loop.run_in_executor(None, self._logging_subprocess)

    async def _logging_subprocess(self):
        while True:
            log = await self.log_queue.get()
            if log is None:
                break
            level, msg = log
            await process_logger.log(level, msg)

    @property
    def state(self) -> ProcessState:
        return self._state

    def stop(self) -> None:
        if self._state == ProcessState.STOPPED or self._state == ProcessState.TERMINATED:
            return
        self._stop_event.set()

    def resume(self) -> None:
        if self._state == ProcessState.IDLE or self._state == ProcessState.TERMINATED:
            return
        self._stop_event.set()



@dataclass
class WorkerRecord:
    name: str
    is_async: bool
    task_queue: mp.Queue
    result_queue: mp.Queue
    worker: Worker

class ProcessCompositor:
    def __init__(
            self,
            max_workers: int = None,
            max_async_workers: int = None,
            idle_timeout: str = "5 MIN",
            cpu_threshold: int = 80,
    ):
        self.sync_workers = []
        self.async_workers = []
        self.task_queue = mp.Queue()
        self.result_queue = mp.Queue()
        self.max_workers = max_workers
        self.max_async_workers = max_async_workers
        self.cpu_threshold = cpu_threshold
        self.idle_timeout = self.covert_to_timeout(idle_timeout) # Represents the time to wait before stopping a worker if idle
        self._dispatcher_task = asyncio.create_task(self._dispatcher_loop())
        self.pid_pool = set()
        self.profiler = Profiler.get_instance()

    def submit(self, task: Task):
        self.task_queue.put(task)
    def retrieve(self):
        while not self.result_queue.empty():
            task, result, error = self.result_queue.get()
            if error:
                task.put_exception(error)
            else:
                task.put_result(result)
    @staticmethod
    def covert_to_timeout(amt: str):
        split_amt = amt.split()
        if len(split_amt) != 2:
            raise Exception(f"Invalid format: {amt}. Expected format: <number> <unit>")
        if split_amt[1] not in time_type_dict or not split_amt[0].isdigit():
            raise Exception(f"Invalid time unit: {split_amt[1]}. Expected one of: SECONDS, MINUTES, HOURS")
        amt, amt_type = split_amt[0], split_amt[1]
        if amt_type.lower() in "days" or amt_type.lower() in "hours":
            raise Exception("Days are not supported. Please use seconds, minutes")
        coefficient = time_type_dict.get(amt.lower(), 1)
        return int(amt) * coefficient

    def _check_cpu_usage(self):
        return psutil.cpu_percent(interval=0.1) > self.cpu_threshold

    def _terminate_non_busy_workers(self):
        for record in self.sync_workers + self.async_workers:
            if record.worker.state in (ProcessState.IDLE, ProcessState.STOPPED):
                record.worker.terminate()
                record.worker.join()
                self.pid_pool.remove(record.worker.pid)
                if record.is_async:
                    self.async_workers.remove(record)
                else:
                    self.sync_workers.remove(record)

    def _create_worker(self, is_async: bool) -> WorkerRecord:
        tq = mp.Queue()
        rq = self.result_queue
        worker = Worker(is_async, task_queue=tq, result_queue=rq)
        record = WorkerRecord(
            name=worker.name,
            is_async=is_async,
            task_queue=tq,
            result_queue=rq,
            worker=worker,
        )
        if is_async:
            self.async_workers.append(record)
        else:
            self.sync_workers.append(record)
        return record


    def _dispatch_task(self):
        if self._check_cpu_usage():
            self._terminate_non_busy_workers()
            asyncio.create_task(process_logger.fatal("CPU usage is too high. Terminating non-busy workers."))
        task = self.task_queue.get()
        if task is None:
            return
        if task.is_async:
            for record in self.async_workers:
                if record.worker.state == ProcessState.IDLE:
                    record.task_queue.put(task)
                if record.worker.state == ProcessState.STOPPED:
                    record.worker.resume()
                    record.task_queue.put(task)
        else:
            for record in self.sync_workers:
                if record.worker.state == ProcessState.IDLE:
                    record.task_queue.put(task)
                elif record.worker.state == ProcessState.STOPPED:
                    record.worker.resume()
                    record.task_queue.put(task)

        if task.is_async and len(self.async_workers) + 1 < self.max_async_workers:
            record = self._create_worker(is_async=True)
            record.task_queue.put(task)
            record.worker.start()
            self.pid_pool.add(record.worker.pid)
        elif not task.is_async and len(self.sync_workers) + 1 < self.max_workers:
            record = self._create_worker(is_async=False)
            record.task_queue.put(task)
            record.worker.start()
            self.pid_pool.add(record.worker.pid)
        else:
            if task.is_async:
                raise RuntimeError("All async workers are busy and no new async workers can be created."
                                   "Routing task to cached task queue.")
            else:
                raise RuntimeError("All workers are busy and no new workers can be created."
                                   "Routing task to cached task queue.")

    async def _dispatcher_loop(self):
        while True:
            try:
                estimated_processes = len(self.pid_pool)
                self._dispatch_task()
                after_dispatch = len(self.pid_pool)
                if estimated_processes != after_dispatch:
                    await compositor_logger.info(f"Worker pool size changed. {estimated_processes} -> {after_dispatch}")
                    self.profiler.put_processes(list(self.pid_pool))
            except RuntimeError as e:
                await compositor_logger.error(f"Could not dispatch task to workers: " + str(e))

    def __repr__(self):
        proc_str = ""
        for record in self.sync_workers + self.async_workers:
            proc_str += f"{record.name} ({'async' if record.is_async else 'sync'}) - State: {record.worker.state.name}\n"
        return f"ProcessCompositor:\n{proc_str}\n"

    async def stop(self):
        await compositor_logger.info("The state of compositor upon stopping:\n" + self.__repr__())
        for record in self.sync_workers + self.async_workers:
            record.worker.stop()
        await asyncio.sleep(0.1)
        for record in self.sync_workers + self.async_workers:
            record.worker.join()
        await compositor_logger.info("All workers stopped.")


class TaskScheduler:

    _instance: Optional["TaskScheduler"] = None

    @classmethod
    def get_instance(cls, max_workers = None, max_async_workers = None, max_tasks_per_loop = None) -> "TaskScheduler":
        if cls._instance is None:
            cls._instance = TaskScheduler(max_workers, max_async_workers, max_tasks_per_loop)
        return cls._instance

    def __init__(
            self,
            max_workers: int = None,
            max_async_workers: int = None,
            idle_timeout: str = "5 MIN",
    ):

        self._pq: List[Tuple[float, int, Task]] = []
        self._counter = 0

        self._executor = ProcessCompositor(
            max_workers=max_workers,
            max_async_workers=max_async_workers,
            idle_timeout=idle_timeout,
        )

    def submit(self, task: Task) -> None:
        heapq.heappush(self._pq, (task.priority, task.priority, task))
        self._counter += 1

    def _dispatch_one(self):
        if not self._pq:
            return
        _, _, task = heapq.heappop(self._pq)
        self._executor.submit(task)

    async def _dispatcher(self):
        while True:
            self._dispatch_one()
            if not self._executor.task_queue.empty():
                self._executor.retrieve()
            await asyncio.sleep(0.1)

class CoreMultiprocessing:

    @staticmethod
    def push_task(task: Task) -> None:
        scheduler = TaskScheduler.get_instance()
        scheduler.submit(task)

    @staticmethod
    def push_chain(task_chain: TaskChain) -> None:
        scheduler = TaskScheduler.get_instance()
        scheduler.submit(task_chain.get())




