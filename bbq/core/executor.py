import contextlib
import logging
import os
import shutil
from pathlib import Path
from typing import Any, Dict

from bbq.core.cache import Cache
from bbq.core.progress import Status
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
        self,
        config: Dict[str, Any],
        request_queue: Queue,
        result_queue: Queue,
        cache: Cache,
    ) -> None:
        self.config: Dict[str, Any] = config
        self.workspace: Path = Path(self.config["system"]["executor"]["workspace"])
        self.build_output_dir: Path = Path(self.config["system"]["build_output_dir"])
        self.request_queue: Queue = request_queue
        self.result_queue: Queue = result_queue
        self.workers = None
        self.cache = cache

        self.workspace.mkdir(parents=True, exist_ok=True)
        self.build_output_dir.mkdir(parents=True, exist_ok=True)

    def __getstate__(self):
        state = self.__dict__.copy()

        del state["request_queue"]
        del state["result_queue"]

        return state

    def start(self) -> None:
        while True:
            task = self.request_queue.pop()
            if task is None:
                break

            logging.info(f"Running task {task.friendly_name}")
            # check cache to see if this thing needs to run
            if self.cache.outdated(task):
                self.run_task(task)
            else:
                logging.info(
                    f"Skipping task {task.friendly_name} since its dependencies did not change"
                )
                task.status = Status.SKIPPED
            if task.status == Status.SUCCESS:
                self.cache.cache(task)
            self.result_queue.put(task)

    def run_task(self, _: Task) -> None:
        raise NotImplementedError


class LocalExecutor(Executor):
    def run_task(self, task: Task) -> None:
        task_workdir = self.workspace / task.friendly_name
        task_workdir.mkdir(parents=True, exist_ok=True)
        with workdir(task_workdir):
            for src in task.input:
                shutil.copy(src, task_workdir)
            task.run()
            for out in task.output:
                src = task_workdir / out
                dst = self.build_output_dir / out
                shutil.copy(src, dst)


class ChrootExecutor(Executor):
    pass


class DockerExecutor(Executor):
    pass
