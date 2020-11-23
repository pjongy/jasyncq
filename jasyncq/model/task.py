import enum
import json
from dataclasses import dataclass
from uuid import UUID

import deserialize


@dataclass
class TaskStatus(enum.IntEnum):
    DEFERRED = 1
    QUEUED = 2
    WORK_IN_PROGRESS = 3
    COMPLETED = 4


@dataclass
@deserialize.parser('uuid', UUID)
@deserialize.parser('status', TaskStatus)
@deserialize.parser('is_urgent', bool)
@deserialize.parser('task', json.loads)
class Task:
    uuid: UUID
    status: TaskStatus
    progressed_at: int  # epoch timestamp
    scheduled_at: int  # epoch timestamp
    is_urgent: bool
    task: dict
    queue_name: str
