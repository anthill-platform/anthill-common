from tornado.gen import coroutine, Return

import tornado.httpclient
import urllib
import ujson
import abc

import common.social


class SteamAPI(common.social.SocialNetworkAPI):
    __metaclass__ = abc.ABCMeta

    STEAM_API = "https://api.steampowered.com"

    def __init__(self, cache):
        super(SteamAPI, self).__init__("steam", cache)

    @coroutine
    def api_auth(self, gamespace, ticket, app_id):

        private_key = yield self.get_private_key(gamespace)

        if private_key.app_id != app_id:
            raise common.social.APIError(400, "Wrong app_id")

        fields = {
            "key": private_key.key,
            "ticket": ticket,
            "appid": app_id
        }

        try:
            response = yield self.api_get("ISteamUserAuth/AuthenticateUserTicket", fields)
        except tornado.httpclient.HTTPError as e:
            raise common.social.APIError(
                e.code,
                e.response.body if hasattr(e.response, "body") else str(e))
        else:
            response_object = ujson.loads(response.body)

            if "response" not in response_object:
                raise common.social.APIError(500, "Steam error: no response field")

            response = response_object["response"]

            if "errorcode" in response:
                raise common.social.APIError(
                    400, "Steam error: " + str(response["errorcode"]) + " " + response["errordesc"])


            if "params" not in response:
                raise common.social.APIError(500, "Steam error: no response/params field")

            params = response["params"]

            steam_id = str(params["steamid"])

            if params["vacbanned"]:
                raise common.social.APIError(403, "VAC Banned")

            result = common.social.AuthResponse(
                username=steam_id,
                import_social=False)

            raise Return(result)

    @coroutine
    def api_get_user_info(self, username=None, key=None):
        try:
            response = yield self.api_get(
                "ISteamUser/GetPlayerSummaries",
                {},
                v="v0002",
                key=key,
                steamids=username)

        except tornado.httpclient.HTTPError as e:
            raise common.social.APIError(e.code, e.response.body)
        else:
            data = ujson.loads(response.body)
            try:
                data = data["response"]["players"][0]
            except KeyError:
                raise common.social.APIError(500, "Steam error: bad user info response")

            raise Return(SteamAPI.process_user_info(data))

    @coroutine
    def api_get(self, operation, fields, v="v1", **kwargs):

        fields.update(**kwargs)
        result = yield self.client.fetch(
            SteamAPI.STEAM_API + "/" + operation + "/" + v + "?" +
            urllib.urlencode(fields))

        raise Return(result)

    @coroutine
    def api_post(self, operation, fields, v="v1", **kwargs):

        fields.update(**kwargs)
        result = yield self.client.fetch(
            SteamAPI.STEAM_API + "/" + operation + "/" + v + "/",
            method="POST",
            body=urllib.urlencode(fields))

        raise Return(result)

    @staticmethod
    def process_user_info(data):
        return {
            "name": data["personaname"],
            "avatar": data["avatarmedium"],
            "profile": data["profileurl"]
        }

    def new_private_key(self, data):
        return SteamPrivateKey(data)


class SteamPrivateKey(common.social.SocialPrivateKey):
    def __init__(self, key):
        super(SteamPrivateKey, self).__init__(key)

        self.key = self.data["key"]
        self.app_id = self.data["app_id"]

    def get_app_id(self):
        return self.app_id
