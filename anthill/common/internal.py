
import tornado.httpclient

from . import discover
from . import singleton
from . import rabbitrpc
from . import jsonrpc
from . import ElapsedTime

from . options import options

import ipaddress
import logging
import socket
from urllib import parse
import ujson


class Internal(rabbitrpc.RabbitMQJsonRPC, metaclass=singleton.Singleton):
    """
    Internal class is user for 'internal' communication between services across the environment.
    """

    def __init__(self):
        logging.info("Constructing new Internal instance")

        self.client = tornado.httpclient.AsyncHTTPClient()

        self.internal_locations = [
            ipaddress.ip_network(network, False)
            for network in options.internal_restrict
        ]
        self.broker = options.internal_broker

        super(Internal, self).__init__()

    async def get(self, service, url, data, use_json=True, discover_service=True, timeout=20, network="internal"):
        """
        Requests a http GET page.

        :param service: Service ID the page is requested from
        :param url: Last par of the request url
        :param data: a disc to be converted to request arguments
        :param use_json: whenever should the result be converted to json or not
        :param discover_service: if True, <service> argument is a service ID,
            if False, <service> is just server location.
        :param timeout: a request timeout in seconds
        :param network: a network to make request in. Default is internal network
        :return: Requested data
        """
        if discover_service:
            try:
                service_location = await discover.cache.get_service(service, network=network)
            except discover.DiscoveryError as e:
                raise InternalError(e.code, "Failed to discover '{0}': ".format(service) + e.message)
        else:
            service_location = service

        if service_location is None:
            raise InternalError(404, "Requesting empty url")

        timer = ElapsedTime("get -> {0}@{1}".format(url, service))

        try:
            request = tornado.httpclient.HTTPRequest(
                service_location + "/" + url + "?" + parse.urlencode(data),
                method='GET',
                request_timeout=timeout,
                headers={
                    "X-Api-Version": options.api_version
                }
            )

            result = await self.client.fetch(request)

        except tornado.httpclient.HTTPError as e:
            message = e.response.body if hasattr(e.response, "body") else b""
            raise InternalError(e.code, str(message, "utf-8"), e.response)

        except socket.error as e:
            logging.exception("get {0}: {1}".format(service, url))
            raise InternalError(599, "Connection error: " + str(e), None)

        finally:
            logging.info(timer.done())

        return Internal.__parse_result__(result, use_json=use_json)

    def is_internal(self, remote_ip):
        """
        Checks if the IP is considered internal (is inside the internal environment).
        Use 'restrict_internal' command line argument to add IP.
        """
        return any((ipaddress.ip_address(remote_ip) in network) for network in self.internal_locations)

    async def listen(self, service_name, on_receive):
        await self.listen_broker(self.broker, service_name, on_receive)

    @staticmethod
    def __parse_result__(result, use_json=True, return_headers=False):
        data = result.body.decode()

        if not use_json:
            return data

        if len(data) == 0:
            return None

        try:
            content = ujson.loads(data)
        except (KeyError, ValueError):
            raise IndexError(400, "Body is corrupted: " + data)

        if return_headers:
            return content, result.headers

        return content

    async def post(self, service, url, data, use_json=True, discover_service=True,
             timeout=20, network="internal", return_headers=False):
        """
        Posts a http request to a certain service

        :param service: Service ID the page is requested from
        :param url: Last par of the request url
        :param data: a disc to be converted to request arguments
        :param use_json: whenever should the result be converted to json or not
        :param discover_service: if True, <service> argument is a service ID,
            if False, <service> is just server location.
        :param timeout: a request timeout in seconds
        :param network: a network to make request in. Default is internal network
        :param return_headers: if True, a tuple (result, headers) instead of just result will be returned
        :return: Requested data
        """
        if discover_service:
            try:
                service_location = await discover.cache.get_service(service, network=network)
            except discover.DiscoveryError as e:
                raise InternalError(e.code, "Failed to discover '{0}': " + e.message)
        else:
            service_location = service

        if service_location is None:
            raise InternalError(404, "Requesting empty url")

        timer = ElapsedTime("post -> {0}@{1}".format(url, service))

        try:
            request = tornado.httpclient.HTTPRequest(
                service_location + "/" + url,
                method='POST',
                body=parse.urlencode(data),
                request_timeout=timeout,
                headers={
                    "X-Api-Version": options.api_version
                })

            result = await self.client.fetch(request)

        except tornado.httpclient.HTTPError as e:
            raise InternalError(e.code, e.response.body if hasattr(e.response, "body") else "", e.response)

        except socket.error as e:
            raise InternalError(599, "Connection error: " + str(e), None)
        finally:
            logging.info(timer.done())

        return Internal.__parse_result__(result, use_json=use_json, return_headers=return_headers)

    async def request(self, service, method, timeout=jsonrpc.JSONRPC_TIMEOUT, *args, **kwargs):
        """
        Makes a RabbitMQ RPC request to a certain service.

        :param service: Service ID the page is requested from
        :param method: Service Method to call (as described in internal handler)
        :param args, kwargs: Arguments to send to the method
        :param timeout: A timeout
        
        :returns Request response from service from the other side
        :raises InternalError on either connection issues or the requested service responded so
        
        """

        timer = ElapsedTime("request -> {0}@{1}".format(method, service))

        try:
            result = await self.send_mq_request(service, method, timeout, *args, **kwargs)
        except jsonrpc.JsonRPCError as e:
            raise InternalError(e.code, e.message, e.data)
        except jsonrpc.JsonRPCTimeout:
            raise InternalError(599, "Timed out for request {0}@{1}".format(method, service))

        logging.info(timer.done())

        return result

    async def rpc(self, service, method, *args, **kwargs):
        """
        Unlike 'request' method, sends a simple RabbitMQ message to a certain service (no response is ever returned)

        :param service: Service ID the page is requested from
        :param method: Service Method to call (as described in internal handler)
        :param args, kwargs: Arguments to send to the method
        
        """

        try:
            await self.send_mq_rpc(service, method, *args, **kwargs)
        except jsonrpc.JsonRPCError as e:
            raise InternalError(e.code, e.message, e.data)


class InternalError(Exception):
    def __init__(self, code, body, response=None):
        self.code = code
        self.body = str(body)
        self.response = response

    def __str__(self):
        return str(self.code) + ": " + str(self.body)
