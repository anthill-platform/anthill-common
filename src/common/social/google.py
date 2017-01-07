from tornado.gen import coroutine, Return

import tornado.httpclient
import urllib
import ujson
import jwt
import abc

import common.social


class GoogleAPI(common.social.SocialNetworkAPI):
    __metaclass__ = abc.ABCMeta

    GOOGLE_OAUTH = "https://www.googleapis.com/oauth2/"

    def __init__(self):
        super(GoogleAPI, self).__init__()

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
    def api_auth(self, gamespace, key):

        private_key = yield self.get_private_key(gamespace)

        fields = {
            "code": key,
            "client_id": private_key.app_id,
            "client_secret": private_key.app_secret,
            "redirect_uri": 'postmessage',
            "grant_type": "authorization_code"
        }

        try:
            response = yield self.api_post("token", fields)
        except tornado.httpclient.HTTPError as e:
            raise common.social.APIError(
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

            result = common.social.AuthResponse(
                access_token=access_token,
                expires_in=expires_in,
                refresh_token=refresh_token,
                username=username,
                import_social=True)

            raise Return(result)

    @coroutine
    def api_get(self, operation, fields, v="v4", **kwargs):

        fields.update(**kwargs)
        result = yield self.client.fetch(
            GoogleAPI.GOOGLE_OAUTH + v + "/" + operation + "?" +
            urllib.urlencode(fields))

        raise Return(result)

    @coroutine
    def api_get_friends(self, access_token=None):
        try:
            response = yield self.get(
                "https://www.googleapis.com/plus/v1/people/me/people/visible",
                access_token=access_token)

        except tornado.httpclient.HTTPError as e:
            raise common.social.APIError(e.code, e.response.body)
        else:
            data = ujson.loads(response.body)

            friends = data["items"]
            result = filter(
                bool,
                [self.__parse_friend__(friend) for friend in friends])

            raise Return(result)

    @coroutine
    def api_get_user_info(self, access_token=None):
        try:
            response = yield self.api_get(
                "userinfo",
                {},
                v="v2",
                access_token=access_token)

        except tornado.httpclient.HTTPError as e:
            raise common.social.APIError(e.code, e.response.body)
        else:
            data = ujson.loads(response.body)
            raise Return(GoogleAPI.process_user_info(data))

    @coroutine
    def api_post(self, operation, fields, v="v4", **kwargs):

        fields.update(**kwargs)
        result = yield self.client.fetch(
            GoogleAPI.GOOGLE_OAUTH + v + "/" + operation,
            method="POST",
            body=urllib.urlencode(fields))

        raise Return(result)

    @coroutine
    def api_refresh_token(self, refresh_token, gamespace):

        private_key = yield self.get_private_key(gamespace)

        try:
            response = yield self.api_post("token", {
                "client_id": private_key.app_id,
                "client_secret": private_key.app_secret,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token"
            })
        except tornado.httpclient.HTTPError as e:
            raise common.social.APIError(e.code, e.response.body)
        else:
            data = ujson.loads(response.body)
            raise Return(data)

    @coroutine
    def get(self, url, headers=None, **kwargs):

        result = yield self.client.fetch(
            url + "?" + urllib.urlencode(kwargs),
            headers=headers)

        raise Return(result)

    @staticmethod
    def process_user_info(data):
        return {
            "name": data["name"],
            "avatar": data["picture"],
            "language": data["locale"],
            "email": data["email"]
        }


class GooglePrivateKey(common.social.SocialPrivateKey):
    def __init__(self, key):
        super(GooglePrivateKey, self).__init__(key)

        self.app_secret = self.data["web"]["client_secret"]
        self.app_id = self.data["web"]["client_id"]

    def get_app_id(self):
        return self.app_id
