
from tornado.gen import coroutine, Return, Task
from common.options import options

import common.keyvalue

class RateLimit(object):
    """
    Limits allowed amount of certain actions for an account

    Initialization (see constructor):

    RateLimit(kv, {
        "start_server": (1, 15),
        "upload_score": (10, 60)
    })

    """

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

        If account exceeded maximum limit of actions, returns False

        Otherwise, returns true
        """

        limit = self.actions.get(action)

        if not limit:
            raise Return(True)

        max_requests, requests_in_time = limit

        db = self.kv.acquire()

        try:
            key = "rate:" + key
            value = yield Task(db.get, key)

            if value:
                if value < 0:
                    raise Return(False)
                else:
                    yield Task(db.decr, key)
                    raise Return(True)
            else:
                yield Task(db.setex, key, requests_in_time, max_requests)
                raise Return(True)

        finally:
            yield db.release()
