import json
import logging
import time
import uuid
from typing import List

from aiomysql import Pool, Cursor, Connection
from pypika import Query, Table, Order
from pypika.terms import BasicCriterion

from jasyncq.repository.model.task import TaskStatus, TaskRowIn, TaskRow
from jasyncq.repository.abstract import AbstractRepository


class TaskRepository(AbstractRepository):
    def __init__(self, pool: Pool, topic_name: str = 'default_topic'):
        super().__init__(pool=pool)
        self.table_name = f'jasyncq_{topic_name}'
        self.task: Table = Table(self.table_name)
        self.task__uuid = self.task.field('uuid')
        self.task__status = self.task.field('status')
        self.task__progressed_at = self.task.field('progressed_at')
        self.task__scheduled_at = self.task.field('scheduled_at')
        self.task__is_urgent = self.task.field('is_urgent')
        self.task__task = self.task.field('task')
        self.task__queue_name = self.task.field('queue_name')
        self.task__depend_on = self.task.field('depend_on')

        self.task_child: Table = Table(self.table_name).as_(f'{self.table_name}_child')
        self.task_child__uuid = self.task_child.field('uuid')

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
            '  depend_on VARCHAR(36) DEFAULT NULL,'
            'INDEX idx__uuid (uuid),'
            'INDEX idx__status (status),'
            'INDEX idx__progressed_at (progressed_at),'
            'INDEX idx__scheduled_at (scheduled_at),'
            'INDEX idx__is_urgent (is_urgent),'
            'INDEX idx__queue_name (queue_name),'
            'INDEX idx__depend_on (depend_on)'
            ');',
        ]
        await self._execute(queries=queries)

    async def _fetch_tasks_by_filter(
        self,
        fetch_filter: BasicCriterion,
        offset: int,
        limit: int,
    ) -> List[TaskRow]:
        # Note(pjongy): Represent dependent task is already done or no dependency
        fetch_filter &= self.task_child__uuid.isnull()
        current_epoch = time.time()

        get_tasks_query = Query.from_(
            self.task
        ).left_join(
            self.task_child
        ).on(
            self.task__depend_on == self.task_child__uuid
        ).select(
            self.task__uuid,
            self.task__status,
            self.task__progressed_at,
            self.task__scheduled_at,
            self.task__is_urgent,
            self.task__task,
            self.task__queue_name,
            self.task__depend_on,
        ).where(fetch_filter).orderby(
            self.task__is_urgent,
            order=Order.desc,
        ).offset(offset).limit(limit).get_sql(quote_char='`')
        logging.debug(get_tasks_query)

        async with self.pool.acquire() as conn:
            conn: Connection = conn  # NOTE(pjongy): For type hinting
            async with conn.cursor() as cur:
                await cur.execute(
                    f'LOCK TABLES {self.table_name} WRITE, '
                    f'{self.table_name} as {self.task_child.alias} WRITE'
                )
                await cur.execute(get_tasks_query)
                task_rows = await cur.fetchall()
                uuids = [task_row[0] for task_row in task_rows]
                if uuids:
                    update_tasks_status = Query.update(self.task).set(
                        self.task__status, int(TaskStatus.WORK_IN_PROGRESS)
                    ).set(
                        self.task__progressed_at, int(current_epoch)
                    ).where(self.task__uuid.isin(uuids)).get_sql(quote_char='`')
                    logging.debug(update_tasks_status)
                    await cur.execute(update_tasks_status)
                await cur.execute('UNLOCK TABLES')
                await conn.commit()
                logging.debug(task_rows)

        return [
            TaskRow(
                uuid=task_row[0],
                status=task_row[1],
                progressed_at=task_row[2],
                scheduled_at=task_row[3],
                is_urgent=task_row[4],
                task=json.loads(task_row[5]),
                queue_name=task_row[6],
                depend_on=task_row[7],
            )
            for task_row in task_rows
        ]

    async def fetch_scheduled_tasks(
        self,
        offset: int,
        limit: int,
        queue_name: str,
    ) -> List[TaskRow]:
        current_epoch = time.time()

        fetch_filter = (self.task__status == int(TaskStatus.QUEUED))
        fetch_filter &= (self.task__scheduled_at <= current_epoch)
        fetch_filter &= (self.task__queue_name == queue_name)

        return await self._fetch_tasks_by_filter(
            fetch_filter=fetch_filter,
            offset=offset,
            limit=limit,
        )

    async def fetch_pending_tasks(
        self,
        offset: int,
        limit: int,
        check_term_seconds: int,
        queue_name: str,
    ) -> List[TaskRow]:
        current_epoch = time.time()

        fetch_filter = (self.task__status == int(TaskStatus.WORK_IN_PROGRESS))
        fetch_filter &= (self.task__progressed_at <= (int(current_epoch) - check_term_seconds))
        fetch_filter &= (self.task__queue_name == queue_name)

        return await self._fetch_tasks_by_filter(
            fetch_filter=fetch_filter,
            offset=offset,
            limit=limit,
        )

    async def insert_tasks(self, tasks: List[TaskRowIn]) -> List[TaskRow]:
        logging.debug(tasks)
        insert_tasks_query = Query.into(self.task).columns(
            self.task__uuid,
            self.task__status,
            self.task__progressed_at,
            self.task__scheduled_at,
            self.task__is_urgent,
            self.task__task,
            self.task__queue_name,
            self.task__depend_on,
        )

        inserted_tasks = []
        for task in tasks:
            task_row = TaskRow(
                uuid=str(uuid.uuid4()),
                status=TaskStatus.QUEUED,
                progressed_at=0,
                scheduled_at=task.scheduled_at,
                is_urgent=task.is_urgent,
                task=task.task,
                queue_name=task.queue_name,
                depend_on=task.depend_on,
            )
            insert_tasks_query = insert_tasks_query.insert(
                task_row.uuid,
                int(task_row.status),
                task_row.progressed_at,
                task_row.scheduled_at,
                task_row.is_urgent,
                json.dumps(task_row.task),
                task_row.queue_name,
                task_row.depend_on,
            )
            inserted_tasks.append(task_row)
        insert_tasks_query = insert_tasks_query.get_sql(quote_char='`')
        logging.debug(insert_tasks_query)
        await self._execute([insert_tasks_query])
        return inserted_tasks

    async def delete_tasks(self, task_ids: List[str]):
        logging.debug(task_ids)
        fetch_filter = self.task__uuid.isin(task_ids)
        delete_tasks_query = Query.from_(self.task).where(
            fetch_filter
        ).delete().get_sql(quote_char='`')
        logging.debug(delete_tasks_query)
        await self._execute([delete_tasks_query])
