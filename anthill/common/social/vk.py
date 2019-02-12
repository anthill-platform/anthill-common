
import tornado.httpclient
import ujson
import abc
from urllib import parse

from .. import admin as a
from .. social import SocialNetworkAPI, APIError, AuthResponse, SocialPrivateKey


class VKAPI(SocialNetworkAPI, metaclass=abc.ABCMeta):

    VK_OAUTH = "https://oauth.vk.com/"
    VK_API = "https://api.vk.com/method/"
    VERSION = "5.68"
    NAME = "vk"

    def __init__(self, cache):
        super(VKAPI, self).__init__(VKAPI.NAME, cache)

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
            "redirect_uri": redirect_uri
        }

        try:
            response = await self.api_oauth_post("access_token", fields)
        except tornado.httpclient.HTTPError as e:
            raise APIError(
                e.code,
                e.response.body if hasattr(e.response, "body") else str(e))
        else:
            payload = ujson.loads(response.body)

            access_token = payload["access_token"]
            expires_in = payload["expires_in"]
            username = str(payload["user_id"])

            result = AuthResponse(
                access_token=access_token,
                expires_in=expires_in,
                username=username,
                import_social=True)

            return result

    async def api_get(self, operation, fields, **kwargs):

        fields.update(**kwargs)
        result = await self.client.fetch(
            VKAPI.VK_API + operation + "?" +
            parse.urlencode(fields))

        return result

    async def api_get_friends(self, access_token=None):
        try:
            response = await self.api_get(
                "friends.get",
                {
                    "fields": "photo_200"
                },
                v=VKAPI.VERSION,
                access_token=access_token)

        except tornado.httpclient.HTTPError as e:
            raise APIError(e.code, e.response.body)
        else:
            data = ujson.loads(response.body)

            response = data["response"]
            items = response["items"]

            def parse_item(item):
                result = {
                    "display_name": item["first_name"] + " " + item["last_name"]
                }

                if "photo_200" in item:
                    result["avatar"] = item["photo_200"]

                return result

            return {
                str(item["id"]): parse_item(item)
                for item in items
            }

    async def api_get_user_info(self, access_token=None):
        try:
            response = await self.api_get(
                "friends.get",
                {
                    "fields": "photo_200"
                },
                v=VKAPI.VERSION,
                access_token=access_token)

        except tornado.httpclient.HTTPError as e:
            raise APIError(e.code, e.response.body)
        else:
            data = ujson.loads(response.body)
            return VKAPI.process_user_info(data["response"][0])

    async def api_oauth_post(self, operation, fields, **kwargs):

        fields.update(**kwargs)
        result = await self.client.fetch(
            VKAPI.VK_OAUTH + operation,
            method="POST",
            body=parse.urlencode(fields))

        return result

    async def api_post(self, operation, fields, **kwargs):

        fields.update(**kwargs)
        result = await self.client.fetch(
            VKAPI.VK_API + operation,
            method="POST",
            body=parse.urlencode(fields))

        return result

    async def get(self, url, headers=None, **kwargs):

        result = await self.client.fetch(
            url + "?" + parse.urlencode(kwargs),
            headers=headers)

        return result

    @staticmethod
    def process_user_info(data):
        return {
            "name": u"{0} {1}".format(data["first_name"], data["last_name"]),
            "avatar": data["photo_200"]
        }

    def has_private_key(self):
        return True

    def new_private_key(self, data):
        return VKPrivateKey(data)


class VKPrivateKey(SocialPrivateKey):
    def __init__(self, key):
        super(VKPrivateKey, self).__init__(key)

        self.app_secret = self.data["client_secret"] if self.data else None
        self.app_id = self.data["client_id"] if self.data else None

    def get_app_id(self):
        return self.app_id

    def dump(self):
        return {
            "client_secret": self.app_secret,
            "client_id": self.app_id
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
                "Application ID", "text", "primary", "non-empty",
                order=1,
                description="Select <a href=\"https://vk.com/apps?act=manage\">Manage</a> your VK's application, "
                            "switch to Settings tab, and copy the \"Application ID.\" field."),
            "app_secret": a.field(
                "Secure key", "text", "primary", "non-empty",
                order=2,
                description="Select <a href=\"https://vk.com/apps?act=manage\">Manage</a> your VK's application, "
                            "switch to Settings tab, and copy the \"Secure key.\" field.")
        }

    def update(self, app_secret, app_id, **ignored):
        self.app_secret = app_secret
        self.app_id = app_id
