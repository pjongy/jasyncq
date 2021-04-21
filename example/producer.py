import asyncio
import logging
from asyncio import AbstractEventLoop

import aiomysql

from jasyncq.dispatcher.model.task import TaskIn
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

    tasks = [
        {'a': 1},
        {'b': 1},
    ]
    queue_name = 'QUEUE_TEST'
    queued_tasks = await dispatcher.apply_tasks(
        tasks=[
            TaskIn(task=task, queue_name=queue_name)
            for task in tasks
        ],
    )

    await dispatcher.apply_tasks(
        tasks=[
            TaskIn(task=task, queue_name=queue_name, is_urgent=True)
            for task in tasks
        ],
    )

    depended_task = queued_tasks[0]
    await dispatcher.apply_tasks(
        tasks=[
            TaskIn(task=task, queue_name=queue_name, depend_on=depended_task.uuid)
            for task in tasks
        ],
    )


if __name__ == '__main__':
    FORMAT = "[%(asctime)s / %(filename)s:%(lineno)s - %(funcName)s ] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)

    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(run(loop=event_loop))
