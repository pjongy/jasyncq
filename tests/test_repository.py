from typing import Tuple, Any

import aiomysql
import pytest
from aiomysql import Pool, Connection, Cursor

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
