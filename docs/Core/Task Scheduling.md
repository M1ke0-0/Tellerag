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