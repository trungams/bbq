from collections import defaultdict, deque
from typing import Dict, Iterator, List

from bbq.core.task import Task


class Digraph:
    def __init__(self) -> None:
        self._nodes: Dict[str, Task] = dict()

    @property
    def nodes(self) -> Iterator[Task]:
        return self._nodes.values()

    def indegree(self, node: Task) -> int:
        return sum(1 for _ in node.get_upstream())

    def outdegree(self, node: Task) -> int:
        return sum(1 for _ in node.get_downstream())

    def __len__(self) -> int:
        return len(self._nodes)

    def add_node(self, node: Task) -> None:
        if node.id in self._nodes:
            raise ValueError(f"Node ({node.friendly_name}) already exists")
        self._nodes[node.id] = node

    def cycle_check(self) -> bool:
        try:
            deque(self.dfs(error_on_cycle=True), maxlen=0)
            return True
        except Exception:
            return False

    def dfs(
        self, start_nodes: List[Task] = None, error_on_cycle: bool = False
    ) -> Iterator[Task]:
        UNVISITED = 0
        PENDING = 1
        VISITED = 2
        status: Dict[Task, int] = defaultdict(int)
        pending = start_nodes
        if not pending:
            pending = list(
                filter(lambda node: self.indegree(node) == 0),
                self.nodes,
            )
        while len(pending) > 0:
            node = pending[-1]
            if status[node] == UNVISITED:
                status[node] = PENDING
                for neighbor in node.get_downstream():
                    if status[neighbor] == UNVISITED:
                        pending.append(neighbor)
                        yield neighbor
                    elif status[neighbor] == PENDING:  # cycle detected
                        if error_on_cycle:
                            raise Exception("cycle detected")
            elif status[node] == PENDING:
                pending.pop()
                status[node] = VISITED

    def bfs(self, start_nodes: List[Task] = None) -> Iterator[Task]:
        visited = set()
        pending = deque(start_nodes)
        if not pending:
            pending = deque(filter(lambda node: self.indegree(node) == 0, self.nodes))
        while len(pending) > 0:
            node = pending.popleft()
            for neighbor in node.get_downstream():
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                pending.append(neighbor)
                yield neighbor
