
import tornado.httpclient

from tornado.gen import coroutine, Return

import urllib
import urlparse
import ujson
import hmac
import hashlib

import common.social


class FacebookAPI(common.social.SocialNetworkAPI):
    def __init__(self):
        super(FacebookAPI, self).__init__()

    def __parse_friend__(self, friend):
        try:
            return {
                "id": friend["id"],
                "avatar": friend["picture"]["data"]["url"],
                "display_name": friend["name"]
            }
        except KeyError:
            return None

    @coroutine
    def api_auth(self, gamespace, key):

        private_key = yield self.get_private_key(gamespace)

        try:
            # exchange access token for long period
            response = yield self.get("oauth/access_token", {
                "client_id": private_key.app_id,
                "client_secret": private_key.app_secret,
                "grant_type": "fb_exchange_token",
                "fb_exchange_token": key
            })
        except tornado.httpclient.HTTPError as e:
            raise common.social.APIError(e.code, e.response.body if e.response else "")
        else:
            data = dict(urlparse.parse_qsl(response.body))

            access_token = data["access_token"]
            expires_in = data["expires"]

            result = common.social.AuthResponse(access_token=access_token, expires_in=expires_in)
            raise Return(result)

    @coroutine
    def api_get_friends(self, gamespace, access_token=None):

        private_key = yield self.get_private_key(gamespace)

        try:
            response = yield self.get(
                "v2.5/me/invitable_friends",
                {},
                private_key=private_key, access_token=access_token)

        except tornado.httpclient.HTTPError as e:
            raise common.social.APIError(e.code, e.response.body)
        else:
            data = ujson.loads(response.body)

            friends = data["data"]
            result = filter(
                bool,
                [self.__parse_friend__(friend) for friend in friends])

            raise Return(result)

    @coroutine
    def api_get_user_info(self, gamespace, access_token=None, fields=None):

        private_key = yield self.get_private_key(gamespace)

        try:
            response = yield self.get("me", {
                "fields": fields
            }, private_key=private_key, access_token=access_token)
        except tornado.httpclient.HTTPError as e:
            raise common.social.APIError(e.code, e.response.body)
        else:

            data = ujson.loads(response.body)
            raise Return(FacebookAPI.process_user_info(data))

    @coroutine
    def get(self, operation, fields, private_key=None, access_token=None):
        f = {
            "access_token": access_token,
            "appsecret_proof": self.get_proof(private_key.app_secret, access_token)
        } if access_token is not None else {}
        f.update(fields)

        result = yield self.client.fetch(
            "https://graph.facebook.com/" + operation + "?" +
            urllib.urlencode(f))

        raise Return(result)

    def get_proof(self, app_secret, access_token):
        h = hmac.new(
            app_secret.encode('utf-8'),
            msg=access_token.encode('utf-8'),
            digestmod=hashlib.sha256
        )
        return h.hexdigest()

    @staticmethod
    def process_user_info(data):
        return {
            "name": data["name"],
            "avatar": "http://graph.facebook.com/{0}/picture".format(data["id"]),
            "language": data["locale"],
            "email": data.get("email", None)
        }


class FacebookPrivateKey(common.social.SocialPrivateKey):
    def __init__(self, key):
        super(FacebookPrivateKey, self).__init__(key)

        self.app_secret = self.data["app-secret"]
        self.app_id = self.data["app-id"]

    def get_app_id(self):
        return self.app_id


# noinspection PyMethodMayBeStatic
