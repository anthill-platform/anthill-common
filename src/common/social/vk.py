from tornado.gen import coroutine, Return

import tornado.httpclient
import urllib
import ujson
import abc

import common.social


class VKAPI(common.social.SocialNetworkAPI):
    __metaclass__ = abc.ABCMeta

    VK_OAUTH = "https://oauth.vk.com/"
    VK_API = "https://api.vk.com/method/"
    VERSION = "5.68"

    def __init__(self, cache):
        super(VKAPI, self).__init__("vk", cache)

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

    @coroutine
    def api_auth(self, gamespace, code, redirect_uri):
        private_key = yield self.get_private_key(gamespace)

        fields = {
            "code": code,
            "client_id": private_key.app_id,
            "client_secret": private_key.app_secret,
            "redirect_uri": redirect_uri
        }

        try:
            response = yield self.api_oauth_post("access_token", fields)
        except tornado.httpclient.HTTPError as e:
            raise common.social.APIError(
                e.code,
                e.response.body if hasattr(e.response, "body") else str(e))
        else:
            payload = ujson.loads(response.body)

            access_token = payload["access_token"]
            expires_in = payload["expires_in"]
            username = str(payload["user_id"])

            result = common.social.AuthResponse(
                access_token=access_token,
                expires_in=expires_in,
                username=username,
                import_social=True)

            raise Return(result)

    @coroutine
    def api_get(self, operation, fields, **kwargs):

        fields.update(**kwargs)
        result = yield self.client.fetch(
            VKAPI.VK_API + operation + "?" +
            urllib.urlencode(fields))

        raise Return(result)

    @coroutine
    def api_get_friends(self, access_token=None):
        try:
            response = yield self.api_get(
                "friends.get",
                {
                    "fields": "photo_200"
                },
                v=VKAPI.VERSION,
                access_token=access_token)

        except tornado.httpclient.HTTPError as e:
            raise common.social.APIError(e.code, e.response.body)
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

            raise Return({
                str(item["id"]): parse_item(item)
                for item in items
            })

    @coroutine
    def api_get_user_info(self, access_token=None):
        try:
            response = yield self.api_get(
                "friends.get",
                {
                    "fields": "photo_200"
                },
                v=VKAPI.VERSION,
                access_token=access_token)

        except tornado.httpclient.HTTPError as e:
            raise common.social.APIError(e.code, e.response.body)
        else:
            data = ujson.loads(response.body)
            raise Return(VKAPI.process_user_info(data["response"][0]))

    @coroutine
    def api_oauth_post(self, operation, fields, **kwargs):

        fields.update(**kwargs)
        result = yield self.client.fetch(
            VKAPI.VK_OAUTH + operation,
            method="POST",
            body=urllib.urlencode(fields))

        raise Return(result)

    @coroutine
    def api_post(self, operation, fields, **kwargs):

        fields.update(**kwargs)
        result = yield self.client.fetch(
            VKAPI.VK_API + operation,
            method="POST",
            body=urllib.urlencode(fields))

        raise Return(result)

    @coroutine
    def get(self, url, headers=None, **kwargs):

        result = yield self.client.fetch(
            url + "?" + urllib.urlencode(kwargs),
            headers=headers)

        raise Return(result)

    @staticmethod
    def process_user_info(data):
        return {
            "name": u"{0} {1}".format(data["first_name"], data["last_name"]),
            "avatar": data["photo_200"]
        }

    def new_private_key(self, data):
        return VKPrivateKey(data)


class VKPrivateKey(common.social.SocialPrivateKey):
    def __init__(self, key):
        super(VKPrivateKey, self).__init__(key)

        self.app_secret = self.data["client_secret"]
        self.app_id = self.data["client_id"]

    def get_app_id(self):
        return self.app_id
