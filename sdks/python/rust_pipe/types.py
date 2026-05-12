from __future__ import annotations
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Awaitable
from pydantic import BaseModel


class Priority(str, Enum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class TaskMetadata(BaseModel):
    created_at: datetime
    timeout_ms: int = 300_000
    priority: Priority = Priority.NORMAL
    retry_count: int = 0
    max_retries: int = 3
    trace_id: str | None = None


class Task(BaseModel):
    id: str
    task_type: str
    payload: Any
    metadata: TaskMetadata


class TaskError(BaseModel):
    code: str
    message: str
    retryable: bool


class TaskResult(BaseModel):
    task_id: str
    status: str
    payload: Any | None = None
    error: TaskError | None = None
    duration_ms: int
    worker_id: str


TaskHandler = Callable[[Task], Awaitable[Any]]
