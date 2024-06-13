import importlib
import logging
import threading
from typing import Any, Callable, Dict, Iterable, List, Optional, Type

from bbq.core.cache import Cache
from bbq.core.executor import Executor, LocalExecutor
from bbq.core.graph import Digraph
from bbq.core.progress import Status
from bbq.core.queue import Queue
from bbq.core.task import Task


class Scheduler:
    def __init__(
        self, config: Dict[str, Any], executor: Type[Executor] = LocalExecutor
    ) -> None:
        self.config = config
        self._tasks: Dict[str, Task] = dict()
        self.task_graph = Digraph()
        self.cache = Cache(self.config)

        # queue
        queue_max_size = self.config["system"]["scheduler"]["queue"]["size"]
        sched_type = self.config["system"]["scheduler"]["queue"]["type"]
        self.task_queue = Queue(queue_max_size, sched_type)
        self.result_queue = Queue(queue_max_size)  # this one is fifo by default

        # executor
        self.executor = executor(self.config, self.task_queue, self.result_queue)

        # scheduling
        self.retry = self.config["system"]["scheduler"]["retry"]

        self._load_tasks_from_config()

    def _load_tasks_from_config(self) -> None:
        source_config = self.config["tasks"]["source"]
        mod = importlib.import_module(source_config)
        tasks: List[Task] = mod.tasks
        for task in tasks:
            self.add_task(task)

    @property
    def tasks(self) -> Iterable[Task]:
        return self._tasks.values()

    def add_task(self, task: Task) -> None:
        if task.id in self._tasks:
            raise ValueError(f"Task ({task.friendly_name}) already exists")
        task.load_config(self.config)
        self._tasks[task.id] = task
        self.task_graph.add_node(task)
        self.cache.add_task(task)

    def start(self, task_ids: Optional[List[Task]] = None) -> None:
        # TODO: support running specific tasks

        # start executor thread
        executor_thread = threading.Thread(target=self.executor.start)
        process_result_thread = threading.Thread(target=self._process_results)

        executor_thread.start()
        process_result_thread.start()

        self._run_all_tasks()

        process_result_thread.join()
        executor_thread.join()

    def _process_results(self) -> None:
        done = 0
        while done < len(self.tasks):
            completed = self.result_queue.pop()
            if completed.status == Status.SUCCESS:
                # TODO: update cache
                done += 1
                for t in completed.get_downstream():
                    self._queue_task_if_available(t)
        self.task_queue.close()

    def _run_all_tasks(self) -> None:
        for task in self.tasks:
            self._queue_task_if_available(task)

    def _queue_task_if_available(self, task: Task) -> None:
        if self._is_task_ready(task):
            self.task_queue.put(task)
            task.status = Status.QUEUED

    def _is_task_ready(self, task: Task) -> bool:
        return task.status == Status.NOT_STARTED and all(
            t.status == Status.SUCCESS for t in task.get_upstream()
        )
