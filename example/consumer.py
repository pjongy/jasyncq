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

    while True:
        scheduled_tasks = await dispatcher.fetch_scheduled_tasks(queue_name='QUEUE_TEST', limit=10)
        pending_tasks = await dispatcher.fetch_pending_tasks(
            queue_name='QUEUE_TEST',
            limit=10,
            check_term_seconds=60,
        )

        tasks = [*pending_tasks, *scheduled_tasks]
        if not tasks:
            # NOTE(pjongy): Relax wasting
            await asyncio.sleep(1)

        # ...RUN JOBS WITH tasks
        for task in tasks:
            logging.info(task)

        task_ids = [str(task.uuid) for task in tasks]
        await dispatcher.complete_tasks(task_ids=task_ids)


if __name__ == '__main__':
    FORMAT = "[%(asctime)s / %(filename)s:%(lineno)s - %(funcName)s ] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)

    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(run(loop=event_loop))
