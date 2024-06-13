import queue

from bbq.core.task import Task


class Queue:
    def __init__(self, maxsize: int, sched_type: str = "fifo"):
        self.type = sched_type
        if self.type == "fifo":
            self.queue = queue.Queue(maxsize=maxsize)
        elif self.type == "priority":
            self.queue = queue.PriorityQueue(maxsize=maxsize)
        else:
            raise ValueError("invalid type of queue")

    def put(self, task: Task) -> None:
        if self.type == "fifo":
            self.queue.put(task)
        elif self.type == "priority":
            self.queue.put((-task.priority, task))

    def pop(self) -> Task:
        item = self.queue.get()
        if item is None:
            return None
        if self.type == "fifo":
            return item
        if self.type == "priority":
            _, task = item
            return task

    def close(self) -> None:
        self.queue.put(None)

    def __len__(self) -> int:
        return self.queue.qsize()

    def empty(self) -> bool:
        return self.queue.empty()
