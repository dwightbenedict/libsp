import asyncio
from collections.abc import Callable, Coroutine
from typing import Any, Optional


AsyncFunc = Callable[..., Coroutine[Any, Any, Any]]
ProgressCallback = Callable[[], None]


class TaskPool:
    def __init__(self, pool_size: int, progress_callback: Optional[ProgressCallback] = None) -> None:
        self.progress_callback = progress_callback

        self._semaphore = asyncio.BoundedSemaphore(pool_size)
        self._tasks: set[asyncio.Task] = set()

        self._closed = False

    async def __aenter__(self) -> "TaskPool":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.join()
        self._closed = True

    async def submit(self, coro_f: AsyncFunc, *args: Any, **kwargs: Any) -> asyncio.Task:
        if self._closed:
            raise RuntimeError("Cannot add tasks to a closed TaskPool.")

        await self._semaphore.acquire()
        task = asyncio.create_task(coro_f(*args, **kwargs))
        self._tasks.add(task)
        task.add_done_callback(self._on_task_done)
        return task

    def _on_task_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        self._semaphore.release()

        if self.progress_callback:
            self.progress_callback()

    async def join(self, return_exceptions: bool = False) -> list[Any]:
        if self._closed:
            raise RuntimeError("Cannot join a TaskPool that's already closed.")

        if not self._tasks:
            return []

        # Prevent external cancellation from interrupting internal task completion.
        return await asyncio.shield(
            asyncio.gather(*self._tasks, return_exceptions=return_exceptions)
        )

    async def close(self) -> None:
        # A snapshot of tasks is taken to avoid iterating through a changing set.
        tasks_to_cancel = self._tasks.copy()

        if tasks_to_cancel:
            for task in tasks_to_cancel:
                task.cancel()

            try:
                await asyncio.gather(*tasks_to_cancel)
            except asyncio.CancelledError:
                pass

        self._tasks.clear()
        self._closed = True