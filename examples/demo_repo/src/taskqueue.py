"""A minimal in-memory task queue used as the demo's subject matter."""

from __future__ import annotations

from collections import deque
from typing import Generic, TypeVar

T = TypeVar("T")

__all__ = ["TaskQueue"]


class TaskQueue(Generic[T]):
    """FIFO queue with a bounded size."""

    def __init__(self, capacity: int = 8) -> None:
        if capacity < 1:
            msg = "capacity must be >= 1"
            raise ValueError(msg)
        self.capacity = capacity
        self._items: deque[T] = deque()

    def push(self, item: T) -> None:
        if len(self._items) >= self.capacity:
            msg = "queue is full"
            raise BufferError(msg)
        self._items.append(item)

    def pop(self) -> T:
        return self._items.popleft()

    def __len__(self) -> int:
        return len(self._items)
