
from tornado.gen import coroutine, Return, Task

import tormysql
import tormysql.cursor

# noinspection PyUnresolvedReferences
from pymysql import DatabaseError as DatabaseError
from pymysql import OperationalError as ConnectionError
from pymysql import IntegrityError as DuplicateError
from pymysql import IntegrityError as ConstraintsError

import ujson


def concatenated_hash(args):
    return "_".join(str(h_arg) for h_arg in args)


class DatabaseConnection(object):
    def __init__(self, conn):
        self.conn = conn

    @coroutine
    def autocommit(self, value):
        yield self.conn.autocommit(value)

    def close(self):
        self.conn.close()

    @coroutine
    def commit(self):
        """
        Commits a non-autocommit cursor.

        Usage:

        with (yield db.acquire(auto_commit=False)) as db:
            row = yield db.get("SELECT ... FOR UPDATE");
            ...
            yield db.execute("UPDATE ...");
            yield db.commit()


        """
        yield self.conn.commit()

    @coroutine
    def execute(self, query, *args, **kwargs):
        """
        Executes a mysql query.
        Used for 'UPDATE' and 'DELETE'
        """

        with self.conn.cursor() as cursor:
            result = yield cursor.execute(query, args)
            raise Return(result)

    @coroutine
    def get(self, query, *args, **kwargs):
        """
        Returns one row from a mysql query (a dict).
        Used for 'SELECT'.
        """

        with self.conn.cursor() as cursor:
            yield cursor.execute(query, args)
            raise Return(cursor.fetchone())

    @coroutine
    def insert(self, query, *args, **kwargs):
        """
        Inserts a new row into a mysql.
        Returns LAST_INSERT_ID, so used only for 'INSERT' queries.
        """

        with self.conn.cursor() as cursor:
            yield cursor.execute(query, args)
            raise Return(cursor.lastrowid)

    @coroutine
    def query(self, query, *args, **kwargs):
        """
        Returns all rows from a mysql query (a list of dicts, each dict represents a row).
        Used for 'SELECT'.
        """

        with self.conn.cursor() as cursor:
            yield cursor.execute(query, args)
            raise Return(cursor.fetchall())

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        del exc_info
        self.conn.close()


class Database(object):

    """
    Asynchronous MySQL database with connection pool.
    """

    def __init__(self, host=None, database=None, user=None, password=None, *args, **kwargs):
        self.pool = tormysql.ConnectionPool(
            max_connections=20,
            idle_seconds=7200,
            host=host,
            db=database,
            user=user,
            passwd=password,
            cursorclass=tormysql.cursor.DictCursor,
            autocommit=True,
            use_unicode=True,
            charset="utf8",
            **kwargs
        )

    @coroutine
    def acquire(self, auto_commit=True):

        """
        Acquires a new connection from pool. Acquired connection has context management, so
        it can be used with 'with' statement, and few requests will happen in a single connection.

        Usage:

        with (yield db.acquire()) as db:
            yield db.get("...")
            yield db.insert("...")

        """

        wrapper = DatabaseConnection((yield self.pool.Connection()))
        yield wrapper.autocommit(auto_commit)

        raise Return(wrapper)

    @coroutine
    def execute(self, query, *args, **kwargs):
        """
        Executes a mysql query.
        Used for 'UPDATE' and 'DELETE'.

        Please use 'acquire' method if you would like to make few requests in a row.
        """

        with (yield self.acquire()) as conn:
            result = yield conn.execute(query, *args, **kwargs)

        raise Return(result)

    @coroutine
    def get(self, query, *args, **kwargs):
        """
        Returns one row from a mysql query (a dict).
        Used for 'SELECT'.

        Please use 'acquire' method if you would like to make few requests in a row.
        """

        with (yield self.acquire()) as conn:
            result = yield conn.get(query, *args, **kwargs)

        raise Return(result)

    @coroutine
    def insert(self, query, *args, **kwargs):
        """
        Inserts a new row into a mysql.
        Returns LAST_INSERT_ID, so used only for 'INSERT' queries.

        Please use 'acquire' method if you would like to make few requests in a row.
        """

        with (yield self.acquire()) as conn:
            result = yield conn.insert(query, *args, **kwargs)

        raise Return(result)

    @coroutine
    def query(self, query, *args, **kwargs):
        """
        Returns all rows from a mysql query (a list of dicts, each dict represents a row).
        Used for 'SELECT'.

        Please use 'acquire' method if you would like to make few requests in a row.
        """

        with (yield self.acquire()) as conn:
            result = yield conn.query(query, *args, **kwargs)

        raise Return(result)


class ConditionError(Exception):
    pass


def format_conditions_json(field, args):

    def parse_condition(obj):
        if isinstance(obj, (str, unicode, bool, float, int)):
            return "=", obj

        if isinstance(obj, dict):
            if "@func" in obj and "@value" in obj:
                cond = obj["@func"]
                val = obj["@value"]

                if cond not in ["=", "<", ">", "<=", ">=", "!="]:
                    raise ConditionError("Not allowed condition!")

                return cond, val

        raise ConditionError("Bad value!")

    for arg in args:
        if not isinstance(arg, (str, unicode)):
            raise ConditionError("Bad condition: not a string")

    tuples = [(key,) + parse_condition(value) for key, value in args.iteritems()]

    conditions = [
        """
            CONVERT(JSON_EXTRACT(`{0}`, %s), CHAR(255)) {1} %s
        """.format(field, cond)
        for key, cond, value in tuples
    ]

    def values():
        for key, cond, value in tuples:
            yield ("$.\"{0}\"".format(key), ujson.dumps(value))

    return zip(conditions, list(values()))
