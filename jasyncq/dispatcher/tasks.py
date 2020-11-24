from typing import List

import deserialize

from jasyncq.model.task import Task
from jasyncq.repository.tasks import TaskRepository


class TasksDispatcher:
    def __init__(self, repository: TaskRepository):
        self.repository = repository

    async def fetch_scheduled_tasks(
        self,
        queue_name: str,
        limit: int,
        offset: int = 0,
    ) -> List[Task]:
        task_rows = await self.repository.fetch_scheduled_tasks(
            offset=offset,
            limit=limit,
            queue_name=queue_name,
        )
        return [
            deserialize.deserialize(
                Task,
                {
                    'uuid': task_row[0],
                    'status': task_row[1],
                    'progressed_at': task_row[2],
                    'scheduled_at': task_row[3],
                    'is_urgent': task_row[4],
                    'task': task_row[5],
                    'queue_name': task_row[6],
                }
            )
            for task_row in task_rows
        ]

    async def fetch_pending_tasks(
        self,
        queue_name: str,
        limit: int,
        offset: int = 0,
        check_term_seconds: int = 30
    ) -> List[Task]:
        task_rows = await self.repository.fetch_pending_tasks(
            offset=offset,
            limit=limit,
            queue_name=queue_name,
            check_term_seconds=check_term_seconds,
        )
        return [
            deserialize.deserialize(
                Task,
                {
                    'uuid': task_row[0],
                    'status': task_row[1],
                    'progressed_at': task_row[2],
                    'scheduled_at': task_row[3],
                    'is_urgent': task_row[4],
                    'task': task_row[5],
                    'queue_name': task_row[6],
                }
            )
            for task_row in task_rows
        ]

    async def apply_tasks(self, tasks: List[dict], queue_name: str, scheduled_at: int = 0):
        await self.repository.insert_tasks(
            tasks=tasks,
            scheduled_at=scheduled_at,
            queue_name=queue_name,
        )

    async def complete_tasks(self, task_ids: List[str]):
        if task_ids:
            await self.repository.delete_tasks(task_ids=task_ids)
