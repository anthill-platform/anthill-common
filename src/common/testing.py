
from tornado.gen import coroutine, Return
from tornado.testing import AsyncTestCase, gen_test
from tornado.ioloop import IOLoop

from database import Database, DatabaseError


TEST_DATABASE = "test"
TEST_DB_HOST = "localhost"
TEST_DB_USERNAME = "test"
TEST_DB_PASSWORD = ""


class TestError(Exception):
    pass


class ServerTestCase(AsyncTestCase):

    def get_new_ioloop(self):
        return IOLoop.instance()

    @classmethod
    @coroutine
    def co_setup_class(cls):
        pass

    @classmethod
    def setUpClass(cls):
        IOLoop.current().run_sync(cls.co_setup_class)

    @classmethod
    @coroutine
    def get_test_db(cls, db_host=TEST_DB_HOST, db_name=TEST_DATABASE,
                    db_username=TEST_DB_USERNAME, db_password=TEST_DB_PASSWORD):

        database = Database(
            host=db_host,
            user=db_username,
            password=db_password
        )

        try:
            with (yield database.acquire()) as db:
                yield db.execute(
                    """
                        DROP DATABASE IF EXISTS `{0}`;
                    """.format(db_name))

                yield db.execute(
                    """
                        CREATE DATABASE IF NOT EXISTS `{0}` CHARACTER SET utf8;
                    """.format(db_name))

                yield db.execute(
                    """
                        USE `{0}`;
                    """.format(db_name))

                db.conn._kwargs["db"] = TEST_DATABASE

        except DatabaseError as e:
            raise TestError("Failed to initialize database. Please make sure "
                            "you've configured access to a test database. Reason: " + e.args[1])

        # since we have the database now, use this dirty hack to set up a database
        # for each new connection in the connection pool
        database.pool._kwargs["db"] = TEST_DATABASE

        raise Return(database)
