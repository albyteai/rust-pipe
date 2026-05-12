from __future__ import annotations
import asyncio
import json
import time
import uuid
from typing import Any, Callable, Awaitable

import websockets

from .types import Task, TaskResult, TaskError, TaskHandler


class PipeWorker:
    def __init__(
        self,
        url: str,
        handlers: dict[str, TaskHandler],
        worker_id: str | None = None,
        max_concurrency: int = 10,
        heartbeat_interval_s: float = 5.0,
        reconnect_delay_s: float = 1.0,
        max_reconnect_attempts: int = 10,
    ):
        self.url = url
        self.handlers = handlers
        self.worker_id = worker_id or f"worker-py-{uuid.uuid4().hex[:8]}"
        self.max_concurrency = max_concurrency
        self.heartbeat_interval_s = heartbeat_interval_s
        self.reconnect_delay_s = reconnect_delay_s
        self.max_reconnect_attempts = max_reconnect_attempts
        self._active_tasks = 0
        self._running = False
        self._start_time = 0.0
        self._ws = None
        self._semaphore = asyncio.Semaphore(max_concurrency)

    async def start(self) -> None:
        self._running = True
        self._start_time = time.time()
        await self._connect()

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()

    async def _connect(self) -> None:
        attempts = 0
        while self._running:
            try:
                async with websockets.connect(self.url) as ws:
                    self._ws = ws
                    attempts = 0
                    await self._register()
                    await asyncio.gather(
                        self._receive_loop(),
                        self._heartbeat_loop(),
                    )
            except (websockets.ConnectionClosed, OSError):
                if not self._running:
                    return
                attempts += 1
                if attempts >= self.max_reconnect_attempts:
                    raise ConnectionError("Max reconnect attempts reached")
                delay = self.reconnect_delay_s * (2 ** (attempts - 1))
                await asyncio.sleep(delay)

    async def _register(self) -> None:
        await self._send({
            "type": "WorkerRegister",
            "registration": {
                "workerId": self.worker_id,
                "supportedTasks": list(self.handlers.keys()),
                "maxConcurrency": self.max_concurrency,
                "language": "Python",
            },
        })

    async def _heartbeat_loop(self) -> None:
        while self._running and self._ws:
            await self._send({
                "type": "Heartbeat",
                "payload": {
                    "workerId": self.worker_id,
                    "activeTasks": self._active_tasks,
                    "capacity": self.max_concurrency,
                    "uptimeSeconds": int(time.time() - self._start_time),
                },
            })
            await asyncio.sleep(self.heartbeat_interval_s)

    async def _receive_loop(self) -> None:
        async for raw in self._ws:
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = message.get("type")
            if msg_type == "TaskDispatch":
                task = Task(**message["task"])
                asyncio.create_task(self._execute_task(task))
            elif msg_type == "Shutdown":
                if message.get("graceful"):
                    await self.stop()
                    return

    async def _execute_task(self, task: Task) -> None:
        async with self._semaphore:
            self._active_tasks += 1

            if self._active_tasks >= self.max_concurrency:
                await self._send({
                    "type": "Backpressure",
                    "signal": {
                        "workerId": self.worker_id,
                        "currentLoad": self._active_tasks / self.max_concurrency,
                        "shouldThrottle": True,
                    },
                })

            handler = self.handlers.get(task.task_type)
            start = time.time()

            if not handler:
                await self._send_result(TaskResult(
                    task_id=task.id,
                    status="failed",
                    error=TaskError(
                        code="UNKNOWN_TASK_TYPE",
                        message=f"No handler for: {task.task_type}",
                        retryable=False,
                    ),
                    duration_ms=0,
                    worker_id=self.worker_id,
                ))
                self._active_tasks -= 1
                return

            try:
                result = await handler(task)
                await self._send_result(TaskResult(
                    task_id=task.id,
                    status="completed",
                    payload=result,
                    duration_ms=int((time.time() - start) * 1000),
                    worker_id=self.worker_id,
                ))
            except Exception as e:
                await self._send_result(TaskResult(
                    task_id=task.id,
                    status="failed",
                    error=TaskError(
                        code="HANDLER_ERROR",
                        message=str(e),
                        retryable=True,
                    ),
                    duration_ms=int((time.time() - start) * 1000),
                    worker_id=self.worker_id,
                ))
            finally:
                self._active_tasks -= 1

    async def _send_result(self, result: TaskResult) -> None:
        await self._send({"type": "TaskResult", "result": result.model_dump()})

    async def _send(self, message: dict) -> None:
        if self._ws:
            await self._ws.send(json.dumps(message))


def create_worker(
    url: str,
    handlers: dict[str, TaskHandler],
    **kwargs,
) -> PipeWorker:
    return PipeWorker(url=url, handlers=handlers, **kwargs)
