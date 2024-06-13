from enum import Enum


class Status(Enum):
    NOT_STARTED = 0
    QUEUED = 1
    RUNNING = 2
    CANCELLED = 3
    FAILED = 4
    SKIPPED = 5
    SUCCESS = 6
