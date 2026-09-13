from src.taskqueue import TaskQueue


def test_fifo_order() -> None:
    queue: TaskQueue[int] = TaskQueue(capacity=2)
    queue.push(1)
    queue.push(2)
    assert queue.pop() == 1
    assert queue.pop() == 2


def test_capacity_is_enforced() -> None:
    queue: TaskQueue[int] = TaskQueue(capacity=1)
    queue.push(1)
    try:
        queue.push(2)
    except BufferError:
        return
    raise AssertionError("expected BufferError")
