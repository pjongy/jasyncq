import json
import os
from typing import List

from aiomysql import Pool

from repository.abstract import AbstractRepository

migrate_targets = [
    '20201122172511_create_task_table.json'
]


class NaiveRepository(AbstractRepository):
    def __init__(self, pool: Pool):
        super().__init__(pool=pool)

    async def execute(self, queries: List[str]):
        await self._execute(queries)


def _get_queries(direction: str):
    queries = []
    current_path = os.path.dirname(os.path.realpath(__file__))

    for json_file_name in migrate_targets:
        json_file_path = f'{current_path}/{json_file_name}'
        with open(json_file_path) as target_file:
            query = json.load(target_file)[direction]
            queries.append(query)

    return queries


def up():
    return _get_queries('UP')


def down():
    return _get_queries('DOWN')
