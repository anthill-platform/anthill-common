
import tornado.httpclient

from tornado.gen import coroutine, Return

import urllib
import urlparse
import ujson
import hmac
import hashlib

from .. import admin as a

import common.social


class FacebookAPI(common.social.SocialNetworkAPI):
    NAME = "facebook"

    def __init__(self, cache):
        super(FacebookAPI, self).__init__(FacebookAPI.NAME, cache)

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
    def api_auth(self, gamespace, code, redirect_uri):
        private_key = yield self.get_private_key(gamespace)

        try:
            response = yield self.get("oauth/access_token", {
                "client_id": private_key.app_id,
                "client_secret": private_key.app_secret,
                "redirect_uri": redirect_uri,
                "code": code
            })
        except tornado.httpclient.HTTPError as e:
            raise common.social.APIError(e.code, e.response.body if e.response else "")
        else:
            try:
                data = ujson.loads(response.body)
            except (KeyError, ValueError):
                raise common.social.APIError(400, "corrupted_facebook_response")

            try:
                access_token = data["access_token"]
                expires_in = data["expires_in"]
            except KeyError:
                raise common.social.APIError(400, "corrupted_facebook_response")

            result = common.social.AuthResponse(access_token=access_token, expires_in=expires_in, import_social=True)
            raise Return(result)

    @coroutine
    def api_get_friends(self, gamespace, access_token=None):

        private_key = yield self.get_private_key(gamespace)

        try:
            response = yield self.get(
                "v2.5/me/friends",
                {},
                private_key=private_key, access_token=access_token)

        except tornado.httpclient.HTTPError as e:
            raise common.social.APIError(e.code, e.response.body)
        else:
            data = ujson.loads(response.body)

            friends = data["data"]

            raise Return({
                friend["id"]: {
                    "display_name": friend["name"]
                }
                for friend in friends
            })

    @coroutine
    def api_get_user_info(self, gamespace, access_token=None, fields=None, parse=True):

        private_key = yield self.get_private_key(gamespace)

        try:
            response = yield self.get("me", {
                "fields": fields
            }, private_key=private_key, access_token=access_token)
        except tornado.httpclient.HTTPError as e:
            raise common.social.APIError(e.code, e.response.body)
        else:

            data = ujson.loads(response.body)

            if not parse:
                raise Return(data)

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

    def has_private_key(self):
        return True

    def new_private_key(self, data=None):
        return FacebookPrivateKey(data)


class FacebookPrivateKey(common.social.SocialPrivateKey):
    def __init__(self, key):
        super(FacebookPrivateKey, self).__init__(key)

        self.app_secret = self.data.get("client_secret", self.data.get("app-secret")) if self.data else None
        self.app_id = self.data.get("client_id", self.data.get("app-id")) if self.data else None

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
                "App ID", "text", "primary", "non-empty",
                order=1,
                description="Open <a href=\"https://developers.facebook.com/apps/\">Manage Apps</a>, select "
                            "Settings -> Basic, and copy the App ID."),
            "app_secret": a.field(
                "App Secret", "text", "primary", "non-empty",
                order=2,
                description="Open <a href=\"https://developers.facebook.com/apps/\">Manage Apps</a>, select "
                            "Settings -> Basic, and reveal the App Secret.")
        }

    def update(self, app_secret, app_id, **ignored):
        self.app_secret = app_secret
        self.app_id = app_id
