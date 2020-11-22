from typing import List, Any

from aiomysql import Pool, Connection, Cursor


class AbstractRepository:
    def __init__(self, pool: Pool):
        self.pool = pool

    async def _execute(self, queries: List[str]):
        async with self.pool.acquire() as conn:
            conn: Connection = conn  # NOTE(pjongy): For type hinting
            async with conn.cursor() as cur:
                cur: Cursor = cur  # NOTE(pjongy): For type hinting
                [
                    await cur.execute(clause)
                    for clause in queries
                ]
                await conn.commit()

    async def _execute_and_fetch(self, queries: List[str]) -> List[List[Any]]:
        async def _run(cur_: Cursor, clause_: str) -> List[Any]:
            await cur_.execute(clause_)
            return await cur_.fetchall()

        async with self.pool.acquire() as conn:
            conn: Connection = conn  # NOTE(pjongy): For type hinting
            async with conn.cursor() as cur:
                cur: Cursor = cur  # NOTE(pjongy): For type hinting
                result = [
                    await _run(cur, clause)
                    for clause in queries
                ]
                await conn.commit()
                return result

    async def _fetch(self, query: str, fetch_size: int):
        async with self.pool.acquire() as conn:
            conn: Connection = conn  # NOTE(pjongy): For type hinting
            async with conn.cursor() as cur:
                cur: Cursor = cur  # NOTE(pjongy): For type hinting
                await cur.execute(query)
                return await cur.fetchmany(size=fetch_size)
