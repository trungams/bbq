import importlib
import logging
import pickle
import threading
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Type

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
        self.sched_type = self.config["system"]["scheduler"]["queue"]["type"]
        self.task_queue = Queue(queue_max_size, self.sched_type)
        self.result_queue = Queue(queue_max_size)  # this one is fifo by default
        self.pending = 0

        # executor
        self.executor = executor(
            self.config, self.task_queue, self.result_queue, self.cache
        )

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

    def start(self, task_ids: Optional[List[Task]] = None) -> None:
        # TODO: support running specific tasks

        # start executor thread
        executor_thread = threading.Thread(target=self.executor.start)
        process_result_thread = threading.Thread(target=self._process_results)

        self._run_all_tasks()

        executor_thread.start()
        process_result_thread.start()

        process_result_thread.join()
        executor_thread.join()

    def _process_results(self) -> None:
        while self.pending > 0:
            completed = self.result_queue.pop()
            logging.info(f"Retrieving results for task {completed.friendly_name}")
            self.pending -= 1
            if completed.status == Status.SUCCESS:
                for t in completed.get_downstream():
                    self._queue_task_if_available(t)
        self.task_queue.close()

    def _run_all_tasks(self) -> None:
        for task in self.tasks:
            self._queue_task_if_available(task)

    def _queue_task_if_available(self, task: Task) -> None:
        if self._is_task_ready(task):
            logging.info(f"Queued task {task.friendly_name} (ID = {task.id})")
            self.task_queue.put(task)
            task.status = Status.QUEUED
            self.pending += 1

    def _is_task_ready(self, task: Task) -> bool:
        if task.status in (Status.QUEUED, Status.RUNNING):
            return False
        if task.status in (Status.SKIPPED, Status.SUCCESS):
            return self.cache.outdated(task)
        return all(t.status == Status.SUCCESS for t in task.get_upstream())

    def __getstate__(self):
        state = self.__dict__.copy()

        del state["task_queue"]
        del state["result_queue"]

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

        queue_max_size = self.config["system"]["scheduler"]["queue"]["size"]
        self.sched_type = self.config["system"]["scheduler"]["queue"]["type"]
        self.task_queue = Queue(queue_max_size, self.sched_type)
        self.result_queue = Queue(queue_max_size)

        self.executor.request_queue = self.task_queue
        self.executor.result_queue = self.result_queue

    def save(self) -> None:
        data_dir = Path(self.config["system"]["data_dir"])
        data_file = data_dir / "data.pickle"
        with data_file.open("wb") as fp:
            pickle.dump(self, fp)

    @classmethod
    def load(cls, config: Dict[str, Any]) -> "Scheduler":
        data_dir = Path(config["system"]["data_dir"])
        data_file = data_dir / "data.pickle"
        if not data_file.exists():
            return cls(config)
        with data_file.open("rb") as fp:
            return pickle.load(fp)

    def debug(self) -> None:
        logging.info(f"len(tasks) = {len(self.tasks)}")
        # log tasks and their dependencies
        tasks = []
        for task in self.tasks:
            tasks.append(
                {task.friendly_name: [t.friendly_name for t in task.get_downstream()]}
            )
        logging.info(f"Tasks = {tasks}")
