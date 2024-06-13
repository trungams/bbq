import hashlib
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


class TaskCache:
    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        self.input: Dict[Path, str] = dict()
        self.output: Dict[Path, str] = dict()


class Cache:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.tasks: Dict[str, TaskCache] = dict()

    def add_task(self, task: Task) -> None:
        pass

    def remove_task(self, task_id: str) -> None:
        pass
