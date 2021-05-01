import json
from typing import Tuple, Any, Dict

import aiomysql
import pytest
from aiomysql import Pool, Connection, Cursor
from jasyncq.repository.model.task import TaskRowIn, TaskRow, TaskStatus

from jasyncq.repository.tasks import TaskRepository

from tests.util import random_string_lower


async def _query(pool: Pool, query: str) -> Tuple[Any]:
    async with pool.acquire() as conn:
        conn: Connection = conn  # NOTE(pjongy): For type hinting
        async with conn.cursor() as cur:
            cur: Cursor = cur  # NOTE(pjongy): For type hinting
            await cur.execute(query)
            result = await cur.fetchall()
            await conn.commit()
    return result


@pytest.mark.asyncio
async def test_initialize():
    pool: Pool = await aiomysql.create_pool(
        host='127.0.0.1',
        port=3306,
        user='root',
        db='test',
        autocommit=False,
    )
    test_topic_name = random_string_lower()
    repository = TaskRepository(pool=pool, topic_name=test_topic_name)
    await repository.initialize()
    rows = await _query(pool, 'SHOW TABLES')
    tables = [row[0] for row in rows]
    assert tables.index(f'jasyncq_{test_topic_name}') > 0


@pytest.mark.asyncio
async def test_insert_tasks():
    pool: Pool = await aiomysql.create_pool(
        host='127.0.0.1',
        port=3306,
        user='root',
        db='test',
        autocommit=False,
    )
    test_topic_name = random_string_lower()
    repository = TaskRepository(pool=pool, topic_name=test_topic_name)
    await repository.initialize()

    test_tasks = [
        TaskRowIn(task={'id': 1}, queue_name=random_string_lower()),
        TaskRowIn(task={'id': 2}, queue_name=random_string_lower()),
    ]
    inserted_tasks = await repository.insert_tasks(test_tasks)
    inserted_tasks_by_uuid: Dict[str, TaskRow] = {}
    for inserted_task in inserted_tasks:
        inserted_tasks_by_uuid[inserted_task.uuid] = inserted_task

    columns = ['uuid', 'status', 'progressed_at', 'scheduled_at',
               'is_urgent', 'task', 'queue_name', 'depend_on']
    fetching_query = f'SELECT {",".join(columns)} FROM jasyncq_{test_topic_name}'
    task_rows = await _query(pool, fetching_query)

    for task_row in task_rows:
        uuid = task_row[0]
        status = task_row[1]
        progressed_at = task_row[2]
        scheduled_at = task_row[3]
        is_urgent = task_row[4]
        task = task_row[5]
        queue_name = task_row[6]
        depend_on = task_row[7]

        assert uuid in inserted_tasks_by_uuid
        assert inserted_tasks_by_uuid[uuid].status == status
        assert inserted_tasks_by_uuid[uuid].status == TaskStatus.QUEUED
        assert inserted_tasks_by_uuid[uuid].progressed_at == progressed_at
        assert inserted_tasks_by_uuid[uuid].progressed_at == 0
        assert inserted_tasks_by_uuid[uuid].scheduled_at == scheduled_at
        assert inserted_tasks_by_uuid[uuid].is_urgent == is_urgent
        assert inserted_tasks_by_uuid[uuid].task == json.loads(task)
        assert inserted_tasks_by_uuid[uuid].queue_name == queue_name
        assert inserted_tasks_by_uuid[uuid].depend_on == depend_on


@pytest.mark.asyncio
async def test_if_fetch_scheduled_tasks_updating_status_to_WIP_with_fetching():
    pool: Pool = await aiomysql.create_pool(
        host='127.0.0.1',
        port=3306,
        user='root',
        db='test',
        autocommit=False,
    )
    test_topic_name = random_string_lower()
    repository = TaskRepository(pool=pool, topic_name=test_topic_name)
    await repository.initialize()

    queue_name = random_string_lower()
    test_tasks = [
        TaskRowIn(task={'id': 1}, queue_name=queue_name),
        TaskRowIn(task={'id': 2}, queue_name=queue_name),
    ]
    inserted_tasks = await repository.insert_tasks(test_tasks)
    inserted_tasks_by_uuid: Dict[str, TaskRow] = {}
    for inserted_task in inserted_tasks:
        inserted_tasks_by_uuid[inserted_task.uuid] = inserted_task

    scheduled_tasks = await repository.fetch_scheduled_tasks(
        0, 10, queue_name)
    for scheduled_task in scheduled_tasks:
        uuid = scheduled_task.uuid
        assert uuid in inserted_tasks_by_uuid
        assert inserted_tasks_by_uuid[uuid].status == scheduled_task.status
        assert inserted_tasks_by_uuid[uuid].progressed_at == scheduled_task.progressed_at
        assert inserted_tasks_by_uuid[uuid].scheduled_at == scheduled_task.scheduled_at
        assert inserted_tasks_by_uuid[uuid].is_urgent == scheduled_task.is_urgent
        assert inserted_tasks_by_uuid[uuid].task == scheduled_task.task
        assert inserted_tasks_by_uuid[uuid].queue_name == scheduled_task.queue_name
        assert inserted_tasks_by_uuid[uuid].depend_on == scheduled_task.depend_on

    columns = ['uuid', 'status', 'progressed_at']
    fetching_query = f'SELECT {",".join(columns)} FROM jasyncq_{test_topic_name}'
    task_rows = await _query(pool, fetching_query)

    for task_row in task_rows:
        uuid = task_row[0]
        status = task_row[1]
        progressed_at = task_row[2]
        assert uuid in inserted_tasks_by_uuid
        assert status == TaskStatus.WORK_IN_PROGRESS
        assert progressed_at > 0
