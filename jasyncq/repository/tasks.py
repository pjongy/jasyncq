import json
import logging
import time
import uuid
from typing import List, Any

from aiomysql import Pool
from pypika import Query, Table

from jasyncq.model.task import TaskStatus
from jasyncq.repository.abstract import AbstractRepository


class TaskRepository(AbstractRepository):
    def __init__(self, pool: Pool, topic_name: str = 'default_topic'):
        super().__init__(pool=pool)
        self.table_name = f'jasyncq_{topic_name}'
        self.task = Table(self.table_name)
        self.task__uuid = self.task.field('uuid')
        self.task__status = self.task.field('status')
        self.task__progressed_at = self.task.field('progressed_at')
        self.task__scheduled_at = self.task.field('scheduled_at')
        self.task__is_urgent = self.task.field('is_urgent')
        self.task__task = self.task.field('task')
        self.task__queue_name = self.task.field('queue_name')

    async def initialize(self):
        queries = [
            f'CREATE TABLE IF NOT EXISTS {self.table_name} ('
            '  uuid VARCHAR(36) NOT NULL,'
            '  status TINYINT NOT NULL,'
            '  progressed_at BIGINT NOT NULL,'
            '  scheduled_at BIGINT NOT NULL,'
            '  is_urgent BOOL NOT NULL DEFAULT false,'
            '  task TEXT NOT NULL,'
            '  queue_name VARCHAR(255) NOT NULL,'
            'INDEX idx__uuid (uuid),'
            'INDEX idx__status (status),'
            'INDEX idx__progressed_at (progressed_at),'
            'INDEX idx__scheduled_at (scheduled_at),'
            'INDEX idx__queue_name (queue_name)'
            ');',
        ]
        await self._execute(queries=queries)

    def with_locked_table(self, query: List[str]) -> List[str]:
        return [
            f'LOCK TABLES {self.table_name} WRITE',
            *query,
            'UNLOCK TABLES'
        ]

    async def fetch_scheduled_tasks(
        self,
        offset: int,
        limit: int,
        queue_name: str,
    ) -> List[Any]:
        current_epoch = time.time()

        fetch_filter = (self.task__status == int(TaskStatus.QUEUED))
        fetch_filter &= (self.task__scheduled_at <= current_epoch)
        fetch_filter &= (self.task__queue_name == queue_name)

        get_tasks_query = Query.from_(self.task).select(
            self.task__uuid,
            self.task__status,
            self.task__progressed_at,
            self.task__scheduled_at,
            self.task__is_urgent,
            self.task__task,
            self.task__queue_name,
        ).where(fetch_filter).offset(offset).limit(limit).get_sql(quote_char='`')
        logging.debug(get_tasks_query)

        update_tasks_status = Query.update(self.task).set(
            self.task__status, int(TaskStatus.WORK_IN_PROGRESS)
        ).set(
            self.task__progressed_at, int(current_epoch)
        ).where(fetch_filter).offset(offset).limit(limit).get_sql(quote_char='`')
        logging.debug(update_tasks_status)

        task_rows = (await self._execute_and_fetch(self.with_locked_table([
            get_tasks_query,
            update_tasks_status,
        ])))[1]
        logging.debug(task_rows)
        return task_rows

    async def fetch_pending_tasks(
        self,
        offset: int,
        limit: int,
        check_term_seconds: int,
        queue_name: str,
    ) -> List[Any]:
        current_epoch = time.time()

        fetch_filter = (self.task__status == int(TaskStatus.WORK_IN_PROGRESS))
        fetch_filter &= (self.task__progressed_at <= (int(current_epoch) - check_term_seconds))
        fetch_filter &= (self.task__queue_name == queue_name)

        get_tasks_query = Query.from_(self.task).select(
            self.task__uuid,
            self.task__status,
            self.task__progressed_at,
            self.task__scheduled_at,
            self.task__is_urgent,
            self.task__task,
            self.task__queue_name,
        ).where(fetch_filter).offset(offset).limit(limit).get_sql(quote_char='`')
        logging.debug(get_tasks_query)

        update_tasks_status = Query.update(self.task).set(
            self.task__status, int(TaskStatus.WORK_IN_PROGRESS)
        ).set(
            self.task__progressed_at, int(current_epoch)
        ).where(fetch_filter).offset(offset).limit(limit).get_sql(quote_char='`')
        logging.debug(update_tasks_status)

        task_rows = (await self._execute_and_fetch(self.with_locked_table([
            get_tasks_query,
            update_tasks_status,
        ])))[1]
        logging.debug(task_rows)
        return task_rows

    async def insert_tasks(self, tasks: List[dict], scheduled_at: int, queue_name: str):
        logging.debug(tasks)
        insert_tasks_query = Query.into(self.task)
        for task in tasks:
            insert_tasks_query = insert_tasks_query.insert(
                str(uuid.uuid4()),
                int(TaskStatus.QUEUED),
                0,
                scheduled_at,
                False,
                json.dumps(task),
                queue_name,
            )
        insert_tasks_query = insert_tasks_query.get_sql(quote_char='`')
        logging.debug(insert_tasks_query)
        await self._execute([insert_tasks_query])

    async def delete_tasks(self, task_ids: List[str]):
        logging.debug(task_ids)
        fetch_filter = self.task__uuid.isin(task_ids)
        delete_tasks_query = Query.from_(self.task).where(
            fetch_filter
        ).delete().get_sql(quote_char='`')
        logging.debug(delete_tasks_query)
        await self._execute([delete_tasks_query])
