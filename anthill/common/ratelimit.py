
from tornado.gen import coroutine, Return, Task
from common.options import options

import common.keyvalue
from common import to_int

import logging


class RateLimitExceeded(Exception):
    pass


class RateLimitLock(object):
    def __init__(self, limit, action, key):
        self.limit = limit
        self.action = action
        self.key = key
        self._allowed = True

    async def rollback(self):
        if not self._allowed:
            return )

        self._allowed = False

        db = self.limit.kv.acquire(

        keys = [
            "rate:" + self.action + ":" + self.key + ":" + str(range_)
            for range_, time_ in RateLimit.RANGES
        ]

        try:
            values = await db.mget(keys)
            pipe = db.pipeline()

            for key, value in zip(keys, values):
                if value is not None:
                    pipe.incr(key)

            await Task(pipe.execute)
        finally:
            await db.release()


class RateLimit(object):
    """
    Limits allowed amount of certain actions for an account

    Initialization (see constructor):

    RateLimit({
        "start_server": (1, 15),
        "upload_score": (10, 60)
    })

    Usage:

    try:
        limit = await ratelimit.limit("test", 5)
    except RateLimitExceeded:
        code_is_not_allowed()
    else:
        try:
            allowed_code()
        except SomeError:
            # should be called only if the allowed code is failed
            await limit.rollback()

    """

    RANGES = [(8, 16), (4, 8), (2, 4), (1, 1)]

    def __init__(self, actions):
        """
        :param actions: A disc of tuples where:

            A key: is action to be limited
            A value is (amount, time) - maximum <amount> of actions for a <time>

            Missing actions considered unlimited

        """
        self.kv = common.keyvalue.KeyValueStorage(
            host=options.rate_cache_host,
            port=options.rate_cache_port,
            db=options.rate_cache_db,
            max_connections=options.rate_cache_max_connections)

        self.actions = {}

        for action_name, value in actions.iteritems():
            split = value.split(",")
            if len(split) != 2:
                logging.error("Bad tuple {0}: wrong number of arguments, expected {1}, got {2}".format(
                    action_name, 2, len(split)
                ))
                continue

            try:
                value_a = int(split[0])
                value_b = int(split[1])
            except (KeyError, ValueError) as e:
                logging.error("Bad tuple {0}: {1}".format(
                    action_name, str(e)
                ))
                continue
            else:
                self.actions[action_name] = (value_a, value_b)

    async def limit(self, action, key):
        """
        Tries to proceed action <action> for key <key> (may be account, ip address, group, anything)

        :returns RateLimitLock That allows to rollback the usage (in case the <action> failed)
        :raises RateLimitExceeded If account exceeded maximum limit of actions
        """

        limit = self.actions.get(action)

        if not limit:
            return True

        max_requests, requests_in_time = limit

        db = self.kv.acquire()

        keys = [
            "rate:" + action + ":" + key + ":" + str(range_)
            for range_, time_ in RateLimit.RANGES
        ]

        try:
            values = await db.mget(keys)

            for value in values:
                if value is not None and to_int(value) <= 0:
                    raise RateLimitExceeded()

            pipe = db.pipeline()

            for (range_, time_), value in zip(RateLimit.RANGES, values):
                key_ = "rate:" + action + ":" + key + ":" + str(range_)

                if value is None:
                    pipe.setex(key_, requests_in_time * time_, max_requests * range_ - 1)
                else:
                    pipe.decr(key_)

            await Task(pipe.execute)
            return RateLimitLock(self, action, key)
        finally:
            await db.release()
