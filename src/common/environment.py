from tornado.gen import coroutine, Return, Task

from . import cached
from validate import validate

import internal
import logging
import ujson
import singleton


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

    @coroutine
    def list_apps(self):
        @cached(kv=self.cache,
                h="environment_apps",
                json=True)
        @coroutine
        def get():

            try:
                response = yield self.internal.request(
                    "environment",
                    "get_apps")
            except internal.InternalError:
                logging.exception("Failed to list apps")
                raise Return([])
            else:
                raise Return(response)

        all_apps = yield get()

        raise Return({
            app_data["app_name"]: app_data["app_title"]
            for app_data in all_apps
        })

    @coroutine
    @validate(app_name="str_name", app_info=ApplicationInfoAdapter)
    def set_app_info(self, app_name, app_info):
        """
        Do not use this method for any purposes except testing,
        as its affect the cache permanently
        """

        db = self.cache.acquire()

        try:
            yield Task(db.set, "environment_app:" + app_name, ujson.dumps(app_info.dump()))
        finally:
            yield db.release()

    @coroutine
    def get_app_info(self, app_name):
        @cached(kv=self.cache,
                h=lambda: "environment_app:" + app_name,
                json=True)
        @coroutine
        def get():
            response = yield self.internal.request(
                "environment",
                "get_app_info",
                app_name=app_name)

            raise Return(response)

        try:
            app_info = yield get()
            raise Return(ApplicationInfoAdapter(app_info))

        except internal.InternalError as e:
            if e.code == 404:
                raise AppNotFound()
            else:
                raise e

    @coroutine
    def get_app_title(self, app_name):
        app_info = yield self.get_app_info(app_name)
        raise Return(app_info.title)

    @coroutine
    def get_app_versions(self, app_name):
        app_info = yield self.get_app_info(app_name)
        raise Return(app_info.versions)
