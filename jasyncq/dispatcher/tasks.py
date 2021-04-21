from typing import List
from uuid import UUID

from jasyncq.dispatcher.model.task import TaskOut, TaskIn
from jasyncq.repository.tasks import TaskRepository
from jasyncq.repository.model.task import TaskRowIn
from jasyncq.util import let_if


class TasksDispatcher:
    def __init__(self, repository: TaskRepository):
        self.repository = repository

    async def fetch_scheduled_tasks(
        self,
        queue_name: str,
        limit: int,
        offset: int = 0,
    ) -> List[TaskOut]:
        task_rows = await self.repository.fetch_scheduled_tasks(
            offset=offset,
            limit=limit,
            queue_name=queue_name,
        )
        return [
            TaskOut(
                uuid=UUID(task_row.uuid),
                scheduled_at=task_row.scheduled_at,
                task=task_row.task,
                queue_name=task_row.queue_name,
                depend_on=let_if(task_row.depend_on, UUID),
            )
            for task_row in task_rows
        ]

    async def fetch_pending_tasks(
        self,
        queue_name: str,
        limit: int,
        offset: int = 0,
        check_term_seconds: int = 30
    ) -> List[TaskOut]:
        task_rows = await self.repository.fetch_pending_tasks(
            offset=offset,
            limit=limit,
            queue_name=queue_name,
            check_term_seconds=check_term_seconds,
        )
        return [
            TaskOut(
                uuid=UUID(task_row.uuid),
                scheduled_at=task_row.scheduled_at,
                task=task_row.task,
                queue_name=task_row.queue_name,
                depend_on=let_if(task_row.depend_on, UUID),
            )
            for task_row in task_rows
        ]

    async def apply_tasks(self, tasks: List[TaskIn]) -> List[TaskOut]:
        task_rows = await self.repository.insert_tasks(tasks=[
            TaskRowIn(
                scheduled_at=task.scheduled_at,
                is_urgent=task.is_urgent,
                task=task.task,
                queue_name=task.queue_name,
                depend_on=let_if(task.depend_on, str),
            )
            for task in tasks
        ])
        return [
            TaskOut(
                uuid=UUID(task_row.uuid),
                scheduled_at=task_row.scheduled_at,
                task=task_row.task,
                queue_name=task_row.queue_name,
                depend_on=let_if(task_row.depend_on, UUID),
            )
            for task_row in task_rows
        ]

    async def complete_tasks(self, task_ids: List[str]):
        if task_ids:
            await self.repository.delete_tasks(task_ids=task_ids)
