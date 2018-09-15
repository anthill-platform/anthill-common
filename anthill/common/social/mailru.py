
from tornado.httpclient import HTTPRequest, HTTPError

import abc
import socket
import hashlib
import ujson
from urllib import parse

from .. import admin as a
from .. social import APIError
from . steam import SteamAPI, SteamPrivateKey


class MailRuAPI(SteamAPI):
    """
    MailRu API pretty much copies Steam API
    """

    __metaclass__ = abc.ABCMeta

    MAILRU_API = "https://games.mail.ru/app/"
    STEAM_API = "https://api.games.mail.ru/steam"
    NAME = "mailru"

    def __init__(self, cache):
        super(MailRuAPI, self).__init__(cache, name=MailRuAPI.NAME, steam_api=MailRuAPI.STEAM_API)

    def has_private_key(self):
        return True

    def new_private_key(self, data):
        return MailRuPrivateKey(data)

    # noinspection PyMethodMayBeStatic
    def calculate_signature(self, data, private_key):
        hash_ = hashlib.md5()

        for key in sorted(data):
            hash_.update("{0}={1}".format(key, data[key]))

            hash_.update(private_key.mailru_app_secret)

        return hash_.hexdigest()

    @staticmethod
    def process_user_info(data):
        return {
            "name": data["nick"],
            "avatar": data["avatar"],
            "mailru_uid": data["uid"]
        }

    async def mailru_api_get(self, operation, private_key, **kwargs):

        sign = self.calculate_signature(kwargs, private_key)

        request = HTTPRequest(
            MailRuAPI.MAILRU_API + "/" + str(private_key.get_mailru_app_id()) + "/" + operation +
            "?sign=" + sign + "&" + parse.urlencode(kwargs))

        try:
            result = await self.client.fetch(request)
        except socket.error as e:
            raise APIError(500, "Connection error: " + str(e))
        except HTTPError as e:
            try:
                parsed = ujson.loads(e.response.body)
            except (KeyError, ValueError):
                raise APIError(e.code, "Internal API error")
            else:
                code = parsed.get("errcode", 500)
                message = parsed.get("errmsg", "Internal API error")
                raise APIError(code, message)

        try:
            response_object = ujson.loads(result.body)
        except (KeyError, ValueError):
            raise APIError(500, "Corrupted mailru response")

        status = response_object.get("status", "error")

        if status == "error":
            code = response_object.get("errcode", 500)
            message = response_object.get("errmsg", "Internal API error")
            raise APIError(code, message)

        return response_object

    async def mailru_api_post(self, operation, private_key, **kwargs):

        sign = self.calculate_signature(kwargs, private_key)

        request = HTTPRequest(
            MailRuAPI.MAILRU_API + "/" + str(private_key.get_mailru_app_id()) + "/" + operation + "?sign=" + sign,
            body=parse.urlencode(kwargs),
            method="POST")

        try:
            result = await self.client.fetch(request)
        except socket.error as e:
            raise APIError(500, "Connection error: " + str(e))
        except HTTPError as e:
            try:
                parsed = ujson.loads(e.response.body)
            except (KeyError, ValueError):
                raise APIError(e.code, "Internal API error")
            else:
                code = parsed.get("errcode", 500)
                message = parsed.get("errmsg", "Internal API error")
                raise APIError(code, message)

        try:
            response_object = ujson.loads(result.body)
        except (KeyError, ValueError):
            raise APIError(500, "Corrupted mailru response")

        status = response_object.get("status", "error")

        if status == "error":
            code = response_object.get("errcode", 500)
            message = response_object.get("errmsg", "Internal API error")
            raise APIError(code, message)

        return response_object

    async def api_get_user_info(self, username=None, key=None, env=None):
        response = await self.mailru_api_get("user/profile", key, uid=username)
        return MailRuAPI.process_user_info(response)


class MailRuPrivateKey(SteamPrivateKey):
    def __init__(self, key):
        super(MailRuPrivateKey, self).__init__(key)

        self.mailru_app_id = self.data.get("mailru_app_id") if self.data else None
        self.mailru_app_secret = self.data.get("mailru_app_secret") if self.data else None

    def get_steam_app_id(self):
        return self.app_id

    def get_mailru_app_id(self):
        return self.mailru_app_id

    def dump(self):
        return {
            "app_id": self.app_id,
            "key": self.key,
            "mailru_app_id": self.mailru_app_id,
            "mailru_app_secret": self.mailru_app_secret
        }

    def has_ui(self):
        return True

    def get(self):
        return {
            "app_id": self.app_id,
            "key": self.key,
            "mailru_app_id": self.mailru_app_id,
            "mailru_app_secret": self.mailru_app_secret
        }

    def render(self):
        return {
            "app_id": a.field(
                "Fake Steam Game ID", "text", "primary", "non-empty",
                order=1),
            "key": a.field(
                "Fake Encrypted App Ticket Key", "text", "primary", "non-empty",
                order=2,
                description="Open <a href=\"https://games.mail.ru/dev/games/\">Games</a>, select "
                            "the game, select \"System characteristics\" tab, then copy the "
                            "\"Secret for Steam API emulation\"."),
            "mailru_app_id": a.field(
                "Game ID", "text", "primary", "non-empty", order=3,
                description="Open <a href=\"https://games.mail.ru/dev/games/\">Games</a>, select "
                            "the game, select \"System characteristics\" tab, then copy the "
                            "\"games.mail.ru ID (GMRID)\"."),
            "mailru_app_secret": a.field(
                "Secret", "text", "primary", "non-empty", order=4,
                description="Open <a href=\"https://games.mail.ru/dev/games/\">Games</a>, select "
                            "the game, select \"System characteristics\" tab, then copy the "
                            "\"Secret for api.games.mail/gc.mail.ru\".")
        }

    # noinspection PyMethodOverriding
    def update(self, key, app_id, mailru_app_id, mailru_app_secret, **ignored):
        super(MailRuPrivateKey, self).update(key, app_id)
        self.mailru_app_id = mailru_app_id
        self.mailru_app_secret = mailru_app_secret
