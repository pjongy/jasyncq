from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class TaskOut(BaseModel):
    uuid: UUID
    scheduled_at: int  # epoch timestamp
    task: dict
    queue_name: str
    depend_on: Optional[UUID] = None


class TaskIn(BaseModel):
    scheduled_at: int = 0  # epoch timestamp
    is_urgent: bool = False
    task: dict
    queue_name: str
    depend_on: Optional[UUID] = None
