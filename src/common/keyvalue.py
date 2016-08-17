
from tornadoredis import Client, ConnectionPool
from tornado.gen import Task
import tornado.ioloop


class Connection(Client):
    def __init__(self, kv):
        super(Connection, self).__init__(selected_db=kv.db, connection_pool=kv.connection_pool)

    def release(self):
        return Task(self.disconnect)


class KeyValueStorage(object):
    def __init__(self, host='localhost', port=6379, db=0, max_connections=500, wait_for_available=True):

        self.host = host
        self.port = port
        self.db = db

        self.connection_pool = ConnectionPool(
            host=self.host,
            port=self.port,
            max_connections=max_connections,
            wait_for_available=wait_for_available)

    def acquire(self):
        """
        Acquires a connection from connection pool

        Usage:
            db = kv.acquire()

            try:
                yield Task(db.set, "test", "value")
                test = yield Task(db.get, "test")
            finally:
                yield db.release()
        """
        return Connection(self)
