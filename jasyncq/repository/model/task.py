import enum
import json
from dataclasses import dataclass
from typing import Optional

import deserialize

from jasyncq.util import let_if


@dataclass
class TaskStatus(enum.IntEnum):
    DEFERRED = 1
    QUEUED = 2
    WORK_IN_PROGRESS = 3
    COMPLETED = 4


@dataclass
@deserialize.parser('uuid', str)
@deserialize.parser('status', TaskStatus)
@deserialize.parser('is_urgent', bool)
@deserialize.parser('task', json.loads)
@deserialize.parser('depend_on', lambda x: let_if(x, func=str))
class TaskRow:
    uuid: str
    status: TaskStatus
    progressed_at: int  # epoch timestamp
    scheduled_at: int  # epoch timestamp
    is_urgent: bool
    task: dict
    queue_name: str
    depend_on: Optional[str]


@dataclass
@deserialize.default('scheduled_at', 0)
@deserialize.default('is_urgent', False)
@deserialize.parser('is_urgent', bool)
@deserialize.parser('task', json.loads)
@deserialize.parser('depend_on', lambda x: let_if(x, func=str))
class TaskRowIn:
    scheduled_at: int  # epoch timestamp
    is_urgent: bool
    task: dict
    queue_name: str
    depend_on: Optional[str]
