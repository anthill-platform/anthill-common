
from tornado.gen import coroutine, Return, Task
from common.options import options

import common.keyvalue
from common import to_int


class RateLimitExceeded(Exception):
    pass


class RateLimitLock(object):
    def __init__(self, limit, action, key):
        self.limit = limit
        self.action = action
        self.key = key
        self._allowed = True

    @coroutine
    def rollback(self):
        if not self._allowed:
            raise Return()

        self._allowed = False

        db = self.limit.kv.acquire()
        pipe = db.pipeline()

        try:
            for range_, time_ in RateLimit.RANGES:
                key_ = "rate:" + self.action + ":" + self.key + ":" + str(range_)
                pipe.incr(key_)
        finally:
            yield Task(pipe.execute)
            yield db.release()


class RateLimit(object):
    """
    Limits allowed amount of certain actions for an account

    Initialization (see constructor):

    RateLimit(kv, {
        "start_server": (1, 15),
        "upload_score": (10, 60)
    })

    Usage:

    try:
        limit = yield ratelimit.limit("test", 5)
    except RateLimitExceeded:
        code_is_not_allowed()
    else:
        try:
            allowed_code()
        except SomeError:
            # should be called only if the allowed code is failed
            yield limit.rollback()

    """

    RANGES = [(8, 16), (4, 8), (2, 4), (1, 1)]

    def __init__(self, actions):
        """
        :param kv: A key-value store
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

        self.actions = actions

    @coroutine
    def limit(self, action, key):
        """
        Tries to proceed action <action> for key <key> (may be account, ip address, group, anything)

        :returns RateLimitLock That allows to rollback the usage (in case the <action> failed)
        :raises RateLimitExceeded If account exceeded maximum limit of actions
        """

        limit = self.actions.get(action)

        if not limit:
            raise Return(True)

        max_requests, requests_in_time = limit

        db = self.kv.acquire()

        try:
            keys = ["rate:" + action + ":" + key + ":" + str(range_) for range_, time_ in RateLimit.RANGES]

            values = yield Task(db.mget, keys)

            pipe = db.pipeline()

            try:
                for (range_, time_), value in zip(RateLimit.RANGES, values):
                    key_ = "rate:" + action + ":" + key + ":" + str(range_)

                    if value is None:
                        pipe.setex(key_, requests_in_time * time_, max_requests * range_ - 1)
                    else:
                        value = to_int(value)

                        if value <= 0:
                            raise RateLimitExceeded()
                        else:
                            pipe.decr(key_)
            finally:
                yield Task(pipe.execute)

            raise Return(RateLimitLock(self, action, key))

        finally:
            yield db.release()
