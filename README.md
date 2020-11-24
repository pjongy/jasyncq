# jasyncq
[![PyPI version](https://badge.fury.io/py/jasyncq.svg)](https://badge.fury.io/py/jasyncq)

Asynchronous task queue using mysql

## Requirements
```
deserialize~=1.8.0
aiomysql~=0.0.20
PyPika~=0.37.6
```

## How to use

#### 1. Create aiomysql connection pool
```python
import asyncio
import logging

import aiomysql

loop = asyncio.get_event_loop()

pool = await aiomysql.create_pool(
    host='127.0.0.1',
    port=3306,
    user='root',
    db='test',
    loop=loop,
    autocommit=False,
)
```

#### 2. Generate topic (table) with initialize and inject repository to dispatcher
```python
from jasyncq.dispatcher.tasks import TasksDispatcher
from jasyncq.repository.tasks import TaskRepository

repository = TaskRepository(pool=pool, topic_name='test_topic')
await repository.initialize()
dispatcher = TasksDispatcher(repository=repository)
```

#### 3. Enjoy queue
- Publish tasks
```python
await dispatcher.apply_tasks(
    tasks=[...dict type tasks...],
    queue_name='QUEUE_TEST',
)
```
- Consume tasks
```python
scheduled_tasks = await dispatcher.fetch_scheduled_tasks(queue_name='QUEUE_TEST', limit=10)
pending_tasks = await dispatcher.fetch_pending_tasks(
    queue_name='QUEUE_TEST',
    limit=10,
    check_term_seconds=60,
)
tasks = [*pending_tasks, *scheduled_tasks]
# ...RUN JOBS WITH tasks
```

#### 4. Complete tasks
```python
task_ids = [str(task.uuid) for task in tasks]
await dispatcher.complete_tasks(task_ids=task_ids)
```

## Example
- Consumer: jasyncq/example/consumer.py
- Producer: jasyncq/example/producer.py


## You should know

- Dispatcher's `fetch_scheduled_tasks` and `fetch_pending_tasks` method takes scheduled job and concurrently update their status as `WORK IN PROGRESS` in same transaction
- Most of tasks that queued in jasyncq would run in `exactly once` by `fetch_scheduled_tasks` BUT, some cases job disappeared because of worker shutdown while working. It could be restored by `fetch_pending_tasks` (that can check how long worker tolerate `WIP`-ed but not `Completed`(deleted row))
