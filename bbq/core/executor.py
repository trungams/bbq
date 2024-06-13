import contextlib
import logging
import os
import shutil
from pathlib import Path
from typing import Any, Dict

from bbq.core.queue import Queue
from bbq.core.task import Task


@contextlib.contextmanager
def workdir(path: Path):
    prev_cwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)


class Executor:
    def __init__(
        self, config: Dict[str, Any], request_queue: Queue, result_queue: Queue
    ) -> None:
        self.config: Dict[str, Any] = config
        self.workspace: Path = Path(self.config["system"]["executor"]["workspace"])
        self.request_queue: Queue = request_queue
        self.result_queue: Queue = result_queue
        self.workers = None

        self.workspace.mkdir(parents=True, exist_ok=True)

    def start(self) -> None:
        raise NotImplementedError


class LocalExecutor(Executor):
    def start(self) -> None:
        while True:
            task = self.request_queue.pop()
            if task is None:
                break

            logging.info(f"Running task {task.friendly_name}")
            self._run_task(task)
            self.result_queue.put(task)

    def _run_task(self, task: Task) -> None:
        task_workdir = self.workspace / task.friendly_name
        task_workdir.mkdir(parents=True, exist_ok=True)
        with workdir(task_workdir):
            for src in task.input:
                shutil.copy(src, task_workdir)
                task.run()


class ChrootExecutor(Executor):
    def start(self) -> None:
        pass


class DockerExecutor(Executor):
    def start(self) -> None:
        pass
