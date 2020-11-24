import asyncio
import logging
from asyncio import AbstractEventLoop

import aiomysql

from jasyncq.dispatcher.tasks import TasksDispatcher
from jasyncq.repository.tasks import TaskRepository


async def run(loop: AbstractEventLoop):
    pool = await aiomysql.create_pool(
        host='127.0.0.1',
        port=3306,
        user='root',
        db='test',
        loop=loop,
        autocommit=False,
    )
    repository = TaskRepository(pool=pool, topic_name='test_topic')
    await repository.initialize()
    dispatcher = TasksDispatcher(repository=repository)

    await dispatcher.apply_tasks(
        tasks=[
            {'a': 1},
            {'b': 1}
        ],
        queue_name='QUEUE_TEST',
    )


if __name__ == '__main__':
    FORMAT = "[%(asctime)s / %(filename)s:%(lineno)s - %(funcName)s ] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)

    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(run(loop=event_loop))
