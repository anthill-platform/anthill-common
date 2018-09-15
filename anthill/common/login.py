
from tornado.gen import Task

from . import cached
from . validate import validate
from . internal import Internal, InternalError

from . import singleton
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

    @validate(gamespace_name="str_name", gamespace_info=GamespaceAdapter)
    async def set_gamespace(self, gamespace_name, gamespace_info):
        """
        Do not use this method for any purposes except testing,
        as its affect the cache permanently
        """

        async with self.cache.acquire() as db:
            await db.set("gamespace_info:" + gamespace_name, ujson.dumps(gamespace_info.dump()))

    async def find_gamespace(self, gamespace_name):

        @cached(kv=self.cache,
                h=lambda: "gamespace_info:" + gamespace_name,
                ttl=300,
                json=True)
        async def get():
            try:
                response = await self.internal.request(
                    "login",
                    "get_gamespace",
                    name=gamespace_name)

            except InternalError as e:
                raise LoginClientError(e.code, str(e))
            else:
                return response

        gamespace_info = await get()

        if gamespace_info is None:
            return None

        return GamespaceAdapter(gamespace_info)

    async def get_gamespaces(self):
        @cached(kv=self.cache,
                h=lambda: "gamespaces_list",
                ttl=30,
                json=True)
        async def get():
            try:
                response = await self.internal.request("login", "get_gamespaces")
            except InternalError as e:
                raise LoginClientError(e.code, str(e))
            else:
                return response

        gamespace_list = await get()

        if gamespace_list is None:
            return None

        return map(GamespaceAdapter, gamespace_list)
