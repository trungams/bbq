import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Set
from uuid import uuid4

from bbq.core.progress import Status


class Task:
    def __init__(
        self,
        name: str,
        task_input: Iterable[str],
        task_output: Iterable[str],
        priority: float = 1.0,
    ) -> None:
        self.id: str = str(uuid4())
        self.friendly_name: str = name
        self.input: List[Path] = task_input or list()
        self.output: List[Path] = task_output or list()
        self.priority: float = priority
        self.status: Status = Status.NOT_STARTED

        # graph stuff
        self.downstream_tasks: Set["Task"] = set()
        self.upstream_tasks: Set["Task"] = set()

    def load_config(self, config: Dict[str, Any]) -> None:
        workspace = Path(config["tasks"]["workspace"]).absolute()
        self.input = [workspace / p for p in self.input]
        self.output = [Path(p) for p in self.output]

        # dedup files
        self.input = list(set(self.input))
        self.output = list(set(self.output))

    def get_upstream(self) -> Iterator["Task"]:
        return iter(self.upstream_tasks)

    def get_downstream(self) -> Iterator["Task"]:
        return iter(self.downstream_tasks)

    def set_upstream(self, other: "Task") -> None:
        if self == other:
            raise Exception("self dependency is not allowed")
        self.upstream_tasks.add(other)
        other.downstream_tasks.add(self)

    def set_downstream(self, other: "Task") -> None:
        if self == other:
            raise Exception("self dependency is not allowed")
        self.downstream_tasks.add(other)
        other.upstream_tasks.add(self)

    def pre_execute(self) -> None:
        pass

    def execute(self) -> None:
        raise NotImplementedError("A task should at least implement execute() method")

    def post_execute(self) -> None:
        pass

    def run(self) -> None:
        self.status = Status.RUNNING
        self.pre_execute()
        self.execute()
        self.post_execute()
        self.status = Status.SUCCESS

    def cancel(self) -> None:
        pass

    def __rshift__(self, other: "Task") -> "Task":
        self.set_downstream(other)
        return other

    def __str__(self) -> str:
        return f"<Task: id={self.id} name=({self.friendly_name})>"

    def __repr__(self) -> str:
        return f"<Task: id={self.id} name=({self.friendly_name})>"


class BuildCppTask(Task):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def execute(self) -> None:
        source_file = self.input[0]
        output_file = self.output[0]
        subprocess.run(["g++", source_file, "-o", output_file])


class RunPythonTask(Task):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def execute(self) -> None:
        source_file = self.input[0]
        subprocess.run(["python3", source_file])


class RunBashTask(Task):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def execute(self) -> None:
        source_file = self.input[0]
        subprocess.run(["bash", source_file])
