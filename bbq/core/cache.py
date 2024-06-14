import hashlib
import logging
from pathlib import Path
from typing import Any, Dict

from bbq.core.task import Task


# https://stackoverflow.com/a/44873382/9671542
def _sha256sum(file: Path) -> str:
    file = Path(file)
    h = hashlib.sha256()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with file.open("rb", buffering=0) as f:
        while n := f.readinto(mv):
            h.update(mv[:n])
    return h.hexdigest()


class CachedTask:
    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        self.input: Dict[Path, str] = dict()
        self.output: Dict[Path, str] = dict()
        self.upstream: Dict[str, CachedTask] = dict()

    def __str__(self) -> str:
        d = {"id": self.task_id, "input": self.input, "output": self.output}
        upstream = dict()
        for i, t in self.upstream.items():
            upstream[i] = {"output": t.output}
        d["upstream"] = upstream
        return str(d)

    def __repr__(self) -> str:
        d = {"id": self.task_id, "input": self.input, "output": self.output}
        upstream = dict()
        for i, t in self.upstream.items():
            upstream[i] = {"output": t.output}
        d["upstream"] = upstream
        return str(d)


class Cache:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.build_output_dir: Path = self.config["system"]["build_output_dir"]
        self.tasks: Dict[str, CachedTask] = dict()

    def __str__(self) -> str:
        return str(self.tasks)

    def __repr__(self) -> str:
        return str(self.tasks)

    def cache(self, task: Task) -> None:
        self.tasks[task.id] = self._create_cached_task(task)

    def _create_cached_task(self, task: Task, upstream: bool = False) -> CachedTask:
        cached_task = CachedTask(task.id)
        for p in task.output:
            h = _sha256sum(self.build_output_dir / p)
            cached_task.output[p] = h
        if not upstream:
            for p in task.input:
                h = _sha256sum(p)
                cached_task.input[p] = h
            for t in task.get_upstream():
                cached_upstream = self._create_cached_task(t, upstream=True)
                cached_task.upstream[t.id] = cached_upstream
        return cached_task

    def outdated(self, task: Task) -> bool:
        if task.id not in self.tasks:
            return True

        cached_task = self.tasks[task.id]
        if self._has_files_mismatch(cached_task, task):
            return True

        if len(cached_task.upstream) != len(task.upstream_tasks):
            return True
        for upstream in task.get_upstream():
            if upstream.id not in cached_task.upstream:
                return True
            cached_upstream = cached_task.upstream[upstream.id]
            if self._has_files_mismatch(cached_upstream, upstream, output_only=True):
                return True

        return False

    def _has_files_mismatch(
        self, cached: CachedTask, task: Task, output_only=False
    ) -> bool:
        if not output_only:
            if len(cached.input) != len(task.input):
                return True
            for p in task.input:
                if p not in cached.input:
                    return True
                if cached.input[p] != _sha256sum(p):
                    return True

        if len(cached.output) != len(task.output):
            return True
        for p in task.output:
            built_p = self.build_output_dir / p
            if p not in cached.output:
                return True
            if cached.output[p] != _sha256sum(built_p):
                return True

        return False
