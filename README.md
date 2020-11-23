# jasyncq
[![PyPI version](https://badge.fury.io/py/jasyncq.svg)](https://badge.fury.io/py/jasyncq)

Asynchronous task queue using mysql

## Requirements
```
deserialize~=1.8.0
aiomysql~=0.0.20
PyPika~=0.37.6
```

## Example
```python
import asyncio
import logging

import aiomysql

from jasyncq.dispatcher.tasks import TasksDispatcher
from jasyncq.repository.tasks import TaskRepository


async def run(loop):
    FORMAT = "[%(filename)s:%(lineno)s - %(funcName)s ] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)
    pool = await aiomysql.create_pool(
        host='127.0.0.1',
        port=3306,
        user='root',
        password='password',
        db='test',
        loop=loop,
        autocommit=False,
    )
    dispatcher = TasksDispatcher(repository=TaskRepository(pool=pool))
    await dispatcher.apply_tasks(
        [
            {'a': 1},
            {'b': 1}
        ],
        queue_name='QUEUE_TEST',
    )
    tasks = await dispatcher.fetch_scheduled_tasks('QUEUE_TEST', 10)
    pending_tasks = await dispatcher.fetch_pending_tasks('QUEUE_TEST', 10)
    logging.info(tasks)
    logging.info(pending_tasks)
    # ...RUN JOBS WITH tasks and pending_tasks

    task_ids = [str(task.uuid) for task in [*tasks, *pending_tasks]]
    await dispatcher.complete_tasks(task_ids=task_ids)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop=loop))

```

## You should know

- Dispatcher's `fetch_scheduled_tasks` and `fetch_pending_tasks` method takes scheduled job and concurrently update their status as `WORK IN PROGRESS` in same transaction
- Most of tasks that queued in jasyncq would run in `exactly once` by `fetch_scheduled_tasks` BUT, some cases job disappeared because of worker shutdown while working. It could be restored by `fetch_pending_tasks` (that can check how long worker tolerate `WIP`-ed but not `Completed`(deleted row))
