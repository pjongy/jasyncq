# jasyncq
[![PyPI version](https://badge.fury.io/py/jasyncq.svg)](https://badge.fury.io/py/jasyncq)

Asynchronous task queue using mysql

## You should know

- Dispatcher's `fetch_scheduled_tasks` and `fetch_pending_tasks` method takes scheduled job and concurrently update their status as `WORK IN PROGRESS` in same transaction
- Most of tasks that queued in jasyncq would run in `exactly once` by `fetch_scheduled_tasks` BUT, some cases job disappeared because of worker shutdown while working. It could be restored by `fetch_pending_tasks` (that can check how long worker tolerate `WIP`-ed but not `Completed`(deleted row))


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
    tasks=[...list of jasyncq.dispatcher.model.task.TaskIn...],
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
- Consumer: /example/consumer.py
- Producer: /example/producer.py

### Run example scripts
```
$ docker run --name test_db -p 3306:3306 -e MYSQL_ALLOW_EMPTY_PASSWORD=true -d mysql:8.0.17
$ docker exec -it test_db bash -c 'mysql -u root -e "create database test;"'
$ python3 -m example.producer
$ python3 -m example.consumer
```


## Build
```
$ python3 setup.py sdist
$ python3 -m pip install ./dist/jasyncq-*
```

## Deploy
```
$ twine upload ./dist/jasyncq-{version}.tar.gz
```

## Test
```
$ docker run --name test_db -p 3306:3306 -e MYSQL_ALLOW_EMPTY_PASSWORD=true -d mysql:8.0.17
$ docker exec -it test_db bash -c 'mysql -u root -e "create database test;"'
$ python3 -m pip install pytest==6.2.3
$ pytest
```
