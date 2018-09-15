
import tornado.httpclient
import ujson
import logging

from .. import cached
from .. internal import Internal


class APIError(Exception):
    def __init__(self, code, body):
        self.code = code
        self.body = body

    def __str__(self):
        return str(self.code) + ": " + self.body


class AuthResponse(object):
    def __getattr__(self, item):
        return self.data.get(item, None)

    def __init__(self, *args, **kwargs):
        self.data = {key: value for key, value in kwargs.items() if value is not None}

    def __str__(self):
        return ujson.dumps(self.data)

    def data(self):
        return self.data

    @staticmethod
    def parse(data):
        content = ujson.loads(data)
        return AuthResponse(**content)


class SocialNetworkAPI(object):
    def __init__(self, credential_type, cache):
        self.client = tornado.httpclient.AsyncHTTPClient()
        self.internal = Internal()
        self.cache = cache
        self.credential_type = credential_type

    async def get_private_key(self, gamespace, data=None):
        """
        Looks for a key from login service.
        """

        if not data:
            key_name = self.credential_type

            @cached(kv=self.cache,
                    h=lambda: "auth_key:" + str(gamespace) + ":" + key_name,
                    ttl=300,
                    json=True)
            async def get():
                logging.info("Looking for key '{0}' in gamespace @{1}".format(key_name, gamespace))

                key_data = await self.internal.request(
                    "login", "get_key", gamespace=gamespace, key_name=key_name)

                return key_data

            data = await get()

        return self.new_private_key(data)

    def has_private_key(self):
        return False

    def new_private_key(self, data):
        raise NotImplementedError()


class SocialPrivateKey(object):
    def __init__(self, data):
        self.data = data

    def get_app_id(self):
        return None

    def dump(self):
        return self.data

    def has_ui(self):
        return False

    def get(self):
        raise NotImplementedError()

    def render(self):
        raise NotImplementedError()

    def update(self, **kwargs):
        raise NotImplementedError()

