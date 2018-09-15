
from tornado.gen import Task, sleep
from tornado.ioloop import IOLoop
from asyncio import BaseEventLoop

import logging
import collections
import random
import string
import time
import ujson
import signal
from inspect import isfunction


def cached(kv, h, ttl=300, lock=False, json=False, check_is_cached=False):
    """
        Coroutine-friendly decorator to cache a call result into a key/value storage.
        :param kv: a key-value storage
        :param h: an unique string identifying a cache item for this method call
                  may be a function (usually a lambda), then result will be evaluated
        :param ttl: number of seconds for a cache record to live
        :param lock: whenever a request should be locked for `cache_hash` to deal with concurrent requests
        :param json: whenever the data being cached is a json object
                     if it is, it will be packed properly
        :param check_is_cached: result will be returned as tuple (result, is_cached), where is_cached is bool, meaning
                                whenever result was a fresh one or pulled from a cache

        Decorated method should have such arguments passed:
            cache_hash:
            cache_time:
            lock:

        For example:

        @cached(kv=storage,
                h="test",
                ttl=60,
                lock=True)
        async def do_task(location):
            a = await client.fetch(location)
            return a

        result = await do_task("test")
    """

    def wrapper1(method):
        async def wrapper2(*args, **kwargs):

            async with kv.acquire() as db:
                if isfunction(h):
                    _hash = h()
                else:
                    _hash = h

                if lock:
                    lock_name = "l" + _hash
                    lock_obj = db.lock(lock_name)
                    await lock_obj.acquire()
                else:
                    lock_obj = None

                logging.debug("Looking for '%s' in the cache" % _hash)
                cache = await db.get(_hash)

                if cache:
                    if json:
                        cache = ujson.loads(cache)
                    _is_cached = True
                else:
                    logging.debug("Noting found, resolving the value")

                    cache = await method(*args, **kwargs)

                    if json:
                        to_store = ujson.dumps(cache)
                    else:
                        to_store = cache

                    logging.debug("Storing key '%s' in the cache", _hash)
                    await db.setex(_hash, ttl, to_store)
                    _is_cached = False

                if lock_obj:
                    await lock_obj.release()

            if check_is_cached:
                result = (cache, _is_cached)
                return result

            return cache

        return wrapper2
    return wrapper1


def retry(operation=None, max=3, delay=5, predicate=None):
    """
        Coroutine-friendly decorator to retry some operations:
        :param operation: operation name
        :param max: max number of tries this operation should be tried
        :param delay: a delay between retries
    """

    def wrapper1(method):
        # noinspection PyBroadException
        async def wrapper2(*args, **kwargs):

            counter = max
            ext = None
            while counter > 0:
                try:
                    result = await method(*args, **kwargs)
                except Exception as e:
                    if predicate:
                        if predicate(e):
                            raise e
                    logging.error("Failed to '{0}': {1}, retrying...".format(operation, e.__class__.__name__))
                    counter -= 1
                    ext = e
                    if delay != 0:
                        await sleep(delay)
                else:
                    return result

            logging.fatal("Failed to '{0}' in {1} retries.".format(operation, max))

            raise ext

        return wrapper2
    return wrapper1


def run_on_executor(method):
    def wrapper(self, *args):
        executor = getattr(self, "executor")
        return IOLoop.current().run_in_executor(executor, method, self, *args)
    return wrapper


def to_int(value, default=0):
    if value is None:
        return default

    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def clamp(n, smallest, largest):
    return max(smallest, min(n, largest))


def update(d, u):
    for k, v in u.items():
        if v is None:
            d.pop(k)
        elif isinstance(v, collections.Mapping):
            r = update(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d


def random_string(n):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(n))


class ElapsedTime(object):
    def __init__(self, name):
        self.name = name
        self.start_time = time.time()

    def done(self):
        elapsed_time = time.time() - self.start_time
        return '[{}] finished in {} ms'.format(self.name, int(elapsed_time * 1000))


class SyncTimeout():
    """Timeout class using ALARM signal."""

    class TimeoutError(Exception):
        pass

    def __init__(self, sec):
        self.sec = sec

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.raise_timeout)
        signal.alarm(self.sec)

    def __exit__(self, *args):
        signal.alarm(0)  # disable alarm

    def raise_timeout(self, *args):
        raise SyncTimeout.TimeoutError()


class MetaEnum(type):
    # noinspection PyUnresolvedReferences
    def __contains__(cls, x):
            return x in cls.ALL


class Enum(object):
    __metaclass__ = MetaEnum
    ALL = {}

    def __init__(self, method):
        self.method = str(method).lower()

    def __eq__(self, o):
        return self.method == o

    def __ne__(self, o):
        return self.method != o

    def __str__(self):
        return self.method


class Flags(object):
    def __init__(self, flags=None):
        if flags:
            if isinstance(flags, (set, list,)):
                self._flags = set(filter(lambda flag: isinstance(flag, (str, unicode,)), flags))
            else:
                self._flags = set()
        else:
            self._flags = set()

    def __contains__(self, key):
        return key in self._flags

    def __str__(self):
        return ",".join(self._flags)

    def set(self, flag, value=True):
        if value:
            self._flags.add(flag)
        else:
            self._flags.discard(flag)

    def clear(self, flag):
        self._flags.discard(flag)

    def dump(self):
        return ",".join(self._flags)

    def as_list(self):
        return list(self._flags)
