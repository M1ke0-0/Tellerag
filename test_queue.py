from contextlib import asynccontextmanager
from fastapi import FastAPI, BackgroundTasks
import asyncio


# Async queue to hold tasks
task_queue = asyncio.Queue()

# Background task to process the queue


async def process_queue():
    while True:
        task = await task_queue.get()
        try:
            print(f"Processing task: {task}")
            # Simulate task processing
            await asyncio.sleep(2)
        except Exception as e:
            print(f"Error processing task: {e}")
        finally:
            task_queue.task_done()

# Lifespan context manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    queue_task = asyncio.create_task(process_queue())
    app.state.queue_task = queue_task
    try:
        yield
    finally:
        queue_task.cancel()
        try:
            await queue_task
        except asyncio.CancelledError:
            print("Queue processing loop stopped.")

app = FastAPI(lifespan=lifespan)

# Endpoint to add tasks to the queue


@app.post("/add-task/")
async def add_task(task: str, background_tasks: BackgroundTasks):
    await task_queue.put(task)
    return {"message": "Task added to the queue", "task": task}
