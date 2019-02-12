
import tornado.httpclient

import ujson
import jwt
import abc
from urllib import parse

from .. import admin as a
from .. social import SocialNetworkAPI, APIError, AuthResponse, SocialPrivateKey


class GoogleAPI(SocialNetworkAPI, metaclass=abc.ABCMeta):

    GOOGLE_OAUTH = "https://www.googleapis.com/oauth2/"
    NAME = "google"

    def __init__(self, cache):
        super(GoogleAPI, self).__init__(GoogleAPI.NAME, cache)

    # noinspection PyMethodMayBeStatic
    def __parse_friend__(self, friend):
        try:
            return {
                "id": friend["id"],
                "avatar": friend["image"]["url"],
                "profile": friend["url"],
                "display_name": friend["displayName"]
            }
        except KeyError:
            return None

    async def api_auth(self, gamespace, code, redirect_uri):

        private_key = await self.get_private_key(gamespace)

        fields = {
            "code": code,
            "client_id": private_key.app_id,
            "client_secret": private_key.app_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
            "access_type": "offline"
        }

        try:
            response = await self.api_post("token", fields)
        except tornado.httpclient.HTTPError as e:
            raise APIError(
                e.code,
                e.response.body if hasattr(e.response, "body") else str(e))
        else:
            payload = ujson.loads(response.body)

            refresh_token = payload.get("refresh_token", None)
            access_token = payload["access_token"]
            expires_in = payload["expires_in"]
            id_token = payload["id_token"]

            user_info = jwt.decode(id_token, verify=False)
            username = user_info["sub"]

            result = AuthResponse(
                access_token=access_token,
                expires_in=expires_in,
                refresh_token=refresh_token,
                username=username,
                import_social=True)

            return result

    async def api_get(self, operation, fields, v="v4", **kwargs):

        fields.update(**kwargs)
        result = await self.client.fetch(
            GoogleAPI.GOOGLE_OAUTH + v + "/" + operation + "?" +
            parse.urlencode(fields))

        return result

    async def api_get_user_info(self, access_token=None):
        try:
            response = await self.api_get(
                "userinfo",
                {},
                v="v2",
                access_token=access_token)

        except tornado.httpclient.HTTPError as e:
            raise APIError(e.code, e.response.body)
        else:
            data = ujson.loads(response.body)
            return GoogleAPI.process_user_info(data)

    async def api_post(self, operation, fields, v="v4", **kwargs):

        fields.update(**kwargs)
        result = await self.client.fetch(
            GoogleAPI.GOOGLE_OAUTH + v + "/" + operation,
            method="POST",
            body=parse.urlencode(fields))

        return result

    async def api_refresh_token(self, refresh_token, gamespace):

        private_key = await self.get_private_key(gamespace)

        try:
            response = await self.api_post("token", {
                "client_id": private_key.app_id,
                "client_secret": private_key.app_secret,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token"
            })
        except tornado.httpclient.HTTPError as e:
            raise APIError(e.code, e.response.body)
        else:
            data = ujson.loads(response.body)
            return data

    async def get(self, url, headers=None, **kwargs):

        result = await self.client.fetch(
            url + "?" + parse.urlencode(kwargs),
            headers=headers)

        return result

    @staticmethod
    def process_user_info(data):
        return {
            "name": data["name"],
            "avatar": data["picture"],
            "language": data["locale"],
            "email": data["email"]
        }

    def has_private_key(self):
        return True

    def new_private_key(self, data):
        return GooglePrivateKey(data)


class GooglePrivateKey(SocialPrivateKey):
    def __init__(self, key):
        super(GooglePrivateKey, self).__init__(key)

        self.app_secret = self.data["web"]["client_secret"] if self.data else None
        self.app_id = self.data["web"]["client_id"] if self.data else None

    def get_app_id(self):
        return self.app_id

    def dump(self):
        return {
            "web": {
                "client_secret": self.app_secret,
                "client_id": self.app_id
            }
        }

    def has_ui(self):
        return True

    def get(self):
        return {
            "app_secret": self.app_secret,
            "app_id": self.app_id
        }

    def render(self):
        return {
            "app_id": a.field(
                "Client ID", "text", "primary", "non-empty",
                order=1,
                description="Client ID from Google's project Credentials, "
                            "see <a href=\"https://console.developers.google.com/apis/credentials\">Google "
                            "Credentials</a>"),
            "app_secret": a.field(
                "Client Secret", "text", "primary", "non-empty",
                order=2,
                description="Same as above, but called \"Client Secret\"")
        }

    def update(self, app_secret, app_id, **ignored):
        self.app_secret = app_secret
        self.app_id = app_id
