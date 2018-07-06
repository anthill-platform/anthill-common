
from tornado.gen import coroutine, Return, Task

from . import cached
from validate import validate
from internal import Internal, InternalError

import singleton
import ujson


class GamespaceAdapter(object):
    def __init__(self, data):
        self.gamespace_id = data.get("id")
        self.name = data.get("name")
        self.title = data.get("title")

    def dump(self):
        return {
            "id": self.gamespace_id,
            "name": self.name,
            "title": self.title
        }


class LoginClientError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message


class LoginClient(object):
    __metaclass__ = singleton.Singleton

    def __init__(self, cache):
        self.cache = cache
        self.internal = Internal()

    @coroutine
    @validate(gamespace_name="str_name", gamespace_info=GamespaceAdapter)
    def set_gamespace(self, gamespace_name, gamespace_info):
        """
        Do not use this method for any purposes except testing,
        as its affect the cache permanently
        """

        db = self.cache.acquire()

        try:
            yield Task(db.set, "gamespace_info:" + gamespace_name, ujson.dumps(gamespace_info.dump()))
        finally:
            yield db.release()

    @coroutine
    def find_gamespace(self, gamespace_name):

        @cached(kv=self.cache,
                h=lambda: "gamespace_info:" + gamespace_name,
                ttl=300,
                json=True)
        @coroutine
        def get():
            try:
                response = yield self.internal.request(
                    "login",
                    "get_gamespace",
                    name=gamespace_name)

            except InternalError as e:
                raise LoginClientError(e.code, e.message)
            else:
                raise Return(response)

        gamespace_info = yield get()

        if gamespace_info is None:
            raise Return(None)

        raise Return(GamespaceAdapter(gamespace_info))

    @coroutine
    def get_gamespaces(self):
        @cached(kv=self.cache,
                h=lambda: "gamespaces_list",
                ttl=30,
                json=True)
        @coroutine
        def get():
            try:
                response = yield self.internal.request("login", "get_gamespaces")
            except InternalError as e:
                raise LoginClientError(e.code, e.message)
            else:
                raise Return(response)

        gamespace_list = yield get()

        if gamespace_list is None:
            raise Return(None)

        raise Return(map(GamespaceAdapter, gamespace_list))
