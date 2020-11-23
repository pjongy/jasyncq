import asyncio
import logging

import aiomysql

from jasyncq.migrate.queries import NaiveRepository, up


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
    repository = NaiveRepository(pool=pool)
    queries = up()
    logging.debug(queries)
    await repository.execute(queries)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop=loop))
