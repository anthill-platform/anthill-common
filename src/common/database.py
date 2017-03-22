
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
            max_connections=64,
            wait_connection_timeout=5,
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


class ConditionFunctions(object):
    @staticmethod
    def equal(field, path, obj):
        if "@value" not in obj:
            raise ConditionError("Value not passed")

        value = obj["@value"]

        if not isinstance(value, (str, unicode, int, float, bool)):
            raise ConditionError("Bad value")

        return "CAST(JSON_UNQUOTE(JSON_EXTRACT(`{0}`, %s)) AS CHAR) = %s".format(field), ["$.{0}".format(path), str(value)]

    @staticmethod
    def greater_than(field, path, obj):
        if "@value" not in obj:
            raise ConditionError("Value not passed")

        value = obj["@value"]

        if not isinstance(value, (int, float)):
            raise ConditionError("Bad value")

        return "JSON_UNQUOTE(JSON_EXTRACT(`{0}`, %s)) > %s".format(field), ["$.\"{0}\"".format(path), value]

    @staticmethod
    def less_than(field, path, obj):
        if "@value" not in obj:
            raise ConditionError("Value not passed")

        value = obj["@value"]

        if not isinstance(value, (int, float)):
            raise ConditionError("Bad value")

        return "JSON_UNQUOTE(JSON_EXTRACT(`{0}`, %s)) < %s".format(field), ["$.\"{0}\"".format(path), value]

    @staticmethod
    def greater_or_equal_than(field, path, obj):
        if "@value" not in obj:
            raise ConditionError("Value not passed")

        value = obj["@value"]

        if not isinstance(value, (int, float)):
            raise ConditionError("Bad value")

        return "JSON_UNQUOTE(JSON_EXTRACT(`{0}`, %s)) >= %s".format(field), ["$.\"{0}\"".format(path), value]

    @staticmethod
    def lass_or_equal_than(field, path, obj):
        if "@value" not in obj:
            raise ConditionError("Value not passed")

        value = obj["@value"]

        if not isinstance(value, (int, float)):
            raise ConditionError("Bad value")

        return "JSON_UNQUOTE(JSON_EXTRACT(`{0}`, %s)) <= %s".format(field), ["$.\"{0}\"".format(path), value]

    @staticmethod
    def not_equal(field, path, obj):
        if "@value" not in obj:
            raise ConditionError("Value not passed")

        value = obj["@value"]

        if not isinstance(value, (int, float)):
            raise ConditionError("Bad value")

        return "JSON_UNQUOTE(JSON_EXTRACT(`{0}`, %s)) != %s".format(field), ["$.\"{0}\"".format(path), value]

    @staticmethod
    def between(field, path, obj):
        if "@a" not in obj:
            raise ConditionError("@a is not passed")
        if "@b" not in obj:
            raise ConditionError("@b is not passed")

        a, b = obj["@a"], obj["@b"]

        if not isinstance(a, (int, float)):
            raise ConditionError("Bad @a value")

        if not isinstance(b, (int, float)):
            raise ConditionError("Bad @b value")

        return "JSON_UNQUOTE(JSON_EXTRACT(`{0}`, %s)) BETWEEN %s AND %s".format(field), ["$.\"{0}\"".format(path), a, b]

    @staticmethod
    def in_set(field, path, obj):
        if "@values" not in obj:
            raise ConditionError("@values is not passed")

        values = obj["@values"]

        if not isinstance(values, list):
            raise ConditionError("@values should be a list")

        if not values:
            raise ConditionError("Empty @values")

        for value in values:
            if not isinstance(value, (str, unicode, int, float, bool)):
                raise ConditionError("Bad @value")

        condition = " OR ".join(["JSON_EXTRACT(`{0}`, %s) = %s".format(field)] * len(values))
        result_values = []

        for value in values:
            result_values.append("$.\"{0}\"".format(path))
            result_values.append(value)

        return condition, result_values


def format_conditions_json(field, args):

    functions = {
        "=": ConditionFunctions.equal,
        ">": ConditionFunctions.greater_than,
        "<": ConditionFunctions.less_than,
        ">=": ConditionFunctions.greater_or_equal_than,
        "<=": ConditionFunctions.lass_or_equal_than,
        "!=": ConditionFunctions.not_equal,
        "between": ConditionFunctions.between,
        "in": ConditionFunctions.in_set
    }

    def parse_condition(path, obj):
        if isinstance(obj, bool):
            return ConditionFunctions.equal(field, path, {
                "@value": "true" if obj else "false"
            })

        if isinstance(obj, (str, unicode, bool, float, int)):
            return ConditionFunctions.equal(field, path, {
                "@value": obj
            })

        # if the value is the list, assume it's in_set @func
        if isinstance(obj, list):
            return ConditionFunctions.in_set(field, path, {
                "@values": obj
            })

        if isinstance(obj, dict):
            if "@func" in obj:
                cond = obj["@func"]

                if cond not in functions:
                    raise ConditionError("Not allowed condition!")

                return functions[cond](field, path, obj)

        raise ConditionError("Bad value!")

    if not isinstance(args, dict):
        raise ConnectionError("Conditions expected to be a dict")

    for arg in args:
        if not isinstance(arg, (str, unicode)):
            raise ConditionError("Bad condition: not a string")

    result = [parse_condition(key, value) for key, value in args.iteritems()]
    return result
