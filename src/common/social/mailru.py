from tornado.gen import coroutine, Return

import tornado.httpclient
import urllib
import ujson
import hashlib
import abc

from .. import admin as a

from common.social import SocialNetworkAPI, APIError, AuthResponse, SocialPrivateKey


class MailRuAPI(SocialNetworkAPI):
    __metaclass__ = abc.ABCMeta

    NAME = "mailru"

    def __init__(self, cache):
        super(MailRuAPI, self).__init__(MailRuAPI.NAME, cache)

    def calculate_signature(self, data, private_key):
        data = ""

        hash = hashlib.md5()

        for key in sorted(data):
            hash.update("{0}={1}".format(key, data[key]))

        hash.update(private_key.app_secret)

        return hash.hexdigest()

    def has_private_key(self):
        return True

    def new_private_key(self, data):
        return MailRuPrivateKey(data)


class MailRuPrivateKey(SocialPrivateKey):
    def __init__(self, key):
        super(MailRuPrivateKey, self).__init__(key)

        self.app_id = self.data.get("app_id") if self.data else None
        self.app_secret = self.data.get("app_secret") if self.data else None

    def get_app_id(self):
        return self.app_id

    def dump(self):
        return {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }

    def has_ui(self):
        return True

    def get(self):
        return {
            "app_id": self.app_id,
            "app_secret": self.app_secret,
        }

    def render(self):
        return {
            "app_id": a.field(
                "Game ID", "text", "primary", "non-empty", order=1),
            "app_secret": a.field(
                "Secret", "text", "primary", "non-empty", order=2)
        }

    def update(self, app_id, app_secret, **ignored):
        self.app_id = app_id
        self.app_secret = app_secret
