from tornado.gen import coroutine, Return

from tornado.httpclient import HTTPRequest, HTTPError

import urllib
import ujson
import abc
import logging
import socket

import common.social


class XsollaAPI(common.social.SocialNetworkAPI):
    __metaclass__ = abc.ABCMeta

    XSOLLA_API = "https://api.xsolla.com"

    def __init__(self, cache):
        super(XsollaAPI, self).__init__("xsolla", cache)

    @coroutine
    def api_get(self, operation, merchant_id, api_key, **kwargs):

        request = HTTPRequest(
            XsollaAPI.XSOLLA_API + "/merchant/merchants/" +
                str(merchant_id) + "/" + operation + "?" + urllib.urlencode(kwargs),
            method="GET",
            auth_mode="basic",
            auth_username=str(merchant_id),
            auth_password=str(api_key),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json"
            })

        result = yield self.client.fetch(request)

        try:
            response_object = ujson.loads(result.body)
        except (KeyError, ValueError):
            raise common.social.APIError(500, "Corrupted xsolla response")

        raise Return(response_object)

    @coroutine
    def api_post(self, operation, merchant_id, api_key, **kwargs):

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
            result = yield self.client.fetch(request)
        except socket.error as e:
            raise common.social.APIError(500, "Connection error: " + e.message)
        except HTTPError as e:
            logging.info("Failed to POST xsolla " + str(e.response.body))
            raise common.social.APIError(e.code, e.message)

        try:
            response_object = ujson.loads(result.body)
        except (KeyError, ValueError):
            raise common.social.APIError(500, "Corrupted xsolla response")

        raise Return(response_object)

    def new_private_key(self, data):
        return XsollaPrivateKey(data)


class XsollaPrivateKey(common.social.SocialPrivateKey):
    def __init__(self, key):
        super(XsollaPrivateKey, self).__init__(key)

        self.api_key = self.data["api_key"]
        self.project_key = self.data["project_key"]
        self.merchant_id = self.data["merchant_id"]

    def get_app_id(self):
        return self.merchant_id