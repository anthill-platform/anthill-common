
import tornado.httpclient
import ujson
import abc


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
        self.data = {key: value for key, value in kwargs.iteritems() if value is not None}

    def __str__(self):
        return ujson.dumps(self.data)

    def data(self):
        return self.data

    @staticmethod
    def parse(data):
        content = ujson.loads(data)
        return AuthResponse(**content)


class SocialNetworkAPI(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.client = tornado.httpclient.AsyncHTTPClient()

    def get_private_key(self, gamespace, data=None):
        raise NotImplementedError()

    @abc.abstractmethod
    def new_private_key(self, data):
        raise NotImplementedError()


class SocialPrivateKey(object):
    def __init__(self, data):
        self.data = data

    def get_app_id(self):
        return None


