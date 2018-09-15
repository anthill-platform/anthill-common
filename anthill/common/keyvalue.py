
from aioredis import ConnectionsPool, Redis
from tornado.ioloop import IOLoop


class Connection(object):
    def __init__(self, pool):
        self.pool = pool

    async def __aenter__(self):
        self.connection = await self.pool.acquire()
        return Redis(self.connection)

    async def __aexit__(self, *exc_info):
        del exc_info
        if self.connection is not None:
            self.pool.release(self.connection)


class KeyValueStorage(object):
    def __init__(self, host='localhost', port=6379, db=0, max_connections=500, **kwargs):

        self.connection_pool = ConnectionsPool(
            "redis://{0}:{1}".format(host, port),
            db=db, minsize=1, maxsize=max_connections,
            loop=IOLoop.current().asyncio_loop
        )

    def acquire(self):
        """
        Acquires a connection from connection pool

        Usage:
            async with kv.acquire() as db:
                await db.set("test", "value")
                test = await db.get("test")
        """

        if self.connection_pool is None:
            raise Exception("Connection pool is not created yet")

        return Connection(self.connection_pool)
