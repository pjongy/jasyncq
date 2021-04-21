import enum
from typing import Optional

from pydantic import BaseModel


class TaskStatus(enum.IntEnum):
    DEFERRED = 1
    QUEUED = 2
    WORK_IN_PROGRESS = 3
    COMPLETED = 4


class TaskRow(BaseModel):
    uuid: str
    status: TaskStatus
    progressed_at: int  # epoch timestamp
    scheduled_at: int  # epoch timestamp
    is_urgent: bool
    task: dict
    queue_name: str
    depend_on: Optional[str] = None


class TaskRowIn(BaseModel):
    scheduled_at: int = 0  # epoch timestamp
    is_urgent: bool = False
    task: dict
    queue_name: str
    depend_on: Optional[str] = None
