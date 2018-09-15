
from tornado.httpclient import HTTPRequest, HTTPError

import ujson
import abc
import socket
from urllib import parse

from .. import admin as a
from .. social import SocialNetworkAPI, APIError, SocialPrivateKey


class XsollaAPI(SocialNetworkAPI):
    __metaclass__ = abc.ABCMeta

    XSOLLA_API = "https://api.xsolla.com"
    NAME = "xsolla"

    def __init__(self, cache):
        super(XsollaAPI, self).__init__(XsollaAPI.NAME, cache)

    async def api_get(self, operation, merchant_id, api_key, **kwargs):

        request = HTTPRequest(
            XsollaAPI.XSOLLA_API + "/merchant/merchants/" +
                str(merchant_id) + "/" + operation + "?" + parse.urlencode(kwargs),
            method="GET",
            auth_mode="basic",
            auth_username=str(merchant_id),
            auth_password=str(api_key),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json"
            })

        result = await self.client.fetch(request)

        try:
            response_object = ujson.loads(result.body)
        except (KeyError, ValueError):
            raise APIError(500, "Corrupted xsolla response")

        return response_object

    async def api_post(self, operation, merchant_id, api_key, **kwargs):

        request = HTTPRequest(
            XsollaAPI.XSOLLA_API + "/merchant/merchants/" + str(merchant_id) + "/" + operation,
            body=ujson.dumps(kwargs),
            method="POST",
            auth_mode="basic",
            auth_username=str(merchant_id),
            auth_password=str(api_key),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json"
            })

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
                code = parsed.get("http_status_code", e.code)
                message = parsed.get("message", "Internal API error")
                raise APIError(code, message)

        try:
            response_object = ujson.loads(result.body)
        except (KeyError, ValueError):
            raise APIError(500, "Corrupted xsolla response")

        return response_object

    def has_private_key(self):
        return True

    def new_private_key(self, data):
        return XsollaPrivateKey(data)


class XsollaPrivateKey(SocialPrivateKey):
    def __init__(self, key):
        super(XsollaPrivateKey, self).__init__(key)

        self.api_key = self.data["api_key"] if self.data else None
        self.project_key = self.data["project_key"] if self.data else None
        self.merchant_id = self.data["merchant_id"] if self.data else None

    def get_app_id(self):
        return self.merchant_id

    def dump(self):
        return {
            "api_key": self.api_key,
            "project_key": self.project_key,
            "merchant_id": self.merchant_id,
        }

    def has_ui(self):
        return True

    def get(self):
        return {
            "api_key": self.api_key,
            "project_key": self.project_key,
            "merchant_id": self.merchant_id
        }

    def render(self):
        return {
            "merchant_id": a.field(
                "Merchant ID", "text", "primary", "non-empty",
                order=1,),
            "project_key": a.field(
                "Project Key", "text", "primary", "non-empty",
                order=2),
            "api_key": a.field(
                "API Key", "text", "primary", "non-empty",
                order=2)
        }

    def update(self, merchant_id, project_key, api_key, **ignored):
        self.merchant_id = merchant_id
        self.project_key = project_key
        self.api_key = api_key
