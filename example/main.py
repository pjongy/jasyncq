import asyncio
import logging

import aiomysql

from jasyncq.dispatcher.tasks import TasksDispatcher
from jasyncq.repository.tasks import TaskRepository

loop = asyncio.get_event_loop()


async def run():
    FORMAT = "[%(filename)s:%(lineno)s - %(funcName)s ] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)
    pool = await aiomysql.create_pool(
        host='127.0.0.1',
        port=3306,
        user='root',
        db='test',
        loop=loop,
        autocommit=False,
    )
    dispatcher = TasksDispatcher(repository=TaskRepository(pool=pool))
    await dispatcher.apply_tasks([
        {'a': 1},
        {'b': 1}
    ])
    tasks = await dispatcher.fetch_scheduled_tasks(0, 10)
    pending_tasks = await dispatcher.fetch_pending_tasks(0, 10)
    logging.info(tasks)
    logging.info(pending_tasks)
    # ...RUN JOBS WITH tasks and pending_tasks

    task_ids = [str(task.uuid) for task in [*tasks, *pending_tasks]]
    await dispatcher.complete_tasks(task_ids=task_ids)


loop.run_until_complete(run())
