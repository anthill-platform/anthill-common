from tornado.gen import Task

from . import cached
from . validate import validate
from . import internal
from . import singleton

import logging
import ujson


class AppNotFound(Exception):
    pass


class ApplicationInfoAdapter(object):
    def __init__(self, data):
        self.id = data.get("id")
        self.name = data.get("name")
        self.title = data.get("title")
        self.versions = data.get("versions")

    def dump(self):
        return {
            "id": self.id,
            "name": self.name,
            "title": self.title,
            "versions": self.versions
        }


class EnvironmentClient(object):
    __metaclass__ = singleton.Singleton

    def __init__(self, cache):
        self.internal = internal.Internal()
        self.cache = cache

    async def list_apps(self):
        @cached(kv=self.cache,
                h="environment_apps",
                json=True)
        async def get():

            try:
                response = await self.internal.request(
                    "environment",
                    "get_apps")
            except internal.InternalError:
                logging.exception("Failed to list apps")
                return []
            else:
                return response

        all_apps = await get()

        return {
            app_data["app_name"]: app_data["app_title"]
            for app_data in all_apps
        }

    @validate(app_name="str_name", app_info=ApplicationInfoAdapter)
    async def set_app_info(self, app_name, app_info):
        """
        Do not use this method for any purposes except testing,
        as its affect the cache permanently
        """

        async with self.cache.acquire() as db:
            await db.set("environment_app:" + app_name, ujson.dumps(app_info.dump()))

    async def get_app_info(self, app_name):
        @cached(kv=self.cache,
                h=lambda: "environment_app:" + app_name,
                json=True)
        async def get():
            response = await self.internal.request(
                "environment",
                "get_app_info",
                app_name=app_name)

            return response

        try:
            app_info = await get()
            return ApplicationInfoAdapter(app_info)

        except internal.InternalError as e:
            if e.code == 404:
                raise AppNotFound()
            else:
                raise e

    async def get_app_title(self, app_name):
        app_info = await self.get_app_info(app_name)
        return app_info.title

    async def get_app_versions(self, app_name):
        app_info = await self.get_app_info(app_name)
        return app_info.versions
