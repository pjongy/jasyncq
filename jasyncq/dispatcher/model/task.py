from dataclasses import dataclass
from typing import Optional
from uuid import UUID

import deserialize

from jasyncq.util import let_if


@dataclass
@deserialize.parser('uuid', UUID)
@deserialize.parser('depend_on', lambda x: let_if(x, func=UUID))
class TaskOut:
    uuid: UUID
    scheduled_at: int  # epoch timestamp
    task: dict
    queue_name: str
    depend_on: Optional[UUID]


@dataclass
@deserialize.default('scheduled_at', 0)
@deserialize.default('is_urgent', False)
@deserialize.parser('is_urgent', bool)
@deserialize.parser('depend_on', lambda x: let_if(x, func=UUID))
class TaskIn:
    scheduled_at: int  # epoch timestamp
    is_urgent: bool
    task: dict
    queue_name: str
    depend_on: Optional[UUID]
