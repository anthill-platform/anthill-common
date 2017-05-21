
from tornado.gen import coroutine, Return
from tornado.web import HTTPError
from common.options import options

import tornado.httpclient

import discover
import singleton
import ipaddr
import rabbitrpc
import logging
import socket
import urllib
import ujson
import jsonrpc

from . import ElapsedTime


class Internal(rabbitrpc.RabbitMQJsonRPC):
    """
    Internal class is user for 'internal' communication between services across the environment.
    """
    __metaclass__ = singleton.Singleton

    def __init__(self):
        self.client = tornado.httpclient.AsyncHTTPClient()

        self.internal_locations = map(ipaddr.IPNetwork, options.internal_restrict)
        self.broker = options.internal_broker

        super(Internal, self).__init__()

    @coroutine
    def get(self, service, url, data, use_json=True, discover_service=True, timeout=20, network="internal"):
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
                service_location = yield discover.cache.get_service(service, network=network)
            except discover.DiscoveryError as e:
                raise InternalError(e.code, "Failed to discover '{0}': ".format(service) + e.message)
        else:
            service_location = service

        if service_location is None:
            raise InternalError(404, "Requesting empty url")

        timer = ElapsedTime("get -> {0}@{1}".format(url, service))

        try:
            request = tornado.httpclient.HTTPRequest(
                service_location + "/" + url + "?" + urllib.urlencode(data),
                method='GET',
                request_timeout=timeout
            )

            result = yield self.client.fetch(request)

        except tornado.httpclient.HTTPError as e:
            raise InternalError(e.code, e.response.body if hasattr(e.response, "body") else "", e.response)

        except socket.error as e:
            logging.exception("get {0}: {1}".format(service, url))
            raise InternalError(599, "Connection error: " + e.message, None)

        finally:
            logging.info(timer.done())

        raise Return(Internal.__parse_result__(result, use_json=use_json))

    def is_internal(self, remote_ip):
        """
        Checks if the IP is considered internal (is inside the internal environment).
        Use 'restrict_internal' command line argument to add IP.
        """
        return any((ipaddr.IPAddress(remote_ip) in network) for network in self.internal_locations)

    @coroutine
    def listen(self, service_name, on_receive):
        yield super(Internal, self).listen(self.broker, service_name, on_receive)

    @staticmethod
    def __parse_result__(result, use_json=True):
        data = result.body

        if not use_json:
            return data

        if len(data) == 0:
            return None

        try:
            content = ujson.loads(data)
        except (KeyError, ValueError):
            raise IndexError(400, "Body is corrupted: " + data)

        return content

    @coroutine
    def post(self, service, url, data, use_json=True, discover_service=True, timeout=20, network="internal"):
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
        :return: Requested data
        """
        if discover_service:
            try:
                service_location = yield discover.cache.get_service(service, network=network)
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
                body=urllib.urlencode(data),
                request_timeout=timeout)

            result = yield self.client.fetch(request)

        except tornado.httpclient.HTTPError as e:
            raise InternalError(e.code, e.response.body if hasattr(e.response, "body") else "", e.response)

        except socket.error as e:
            raise InternalError(599, "Connection error: " + e.message, None)
        finally:
            logging.info(timer.done())

        raise Return(Internal.__parse_result__(result, use_json=use_json))

    @coroutine
    def request(self, service, method, timeout=jsonrpc.JSONRPC_TIMEOUT, *args, **kwargs):
        """
        Makes a RabbitMQ RPC request to a certain service.

        :param service: Service ID the page is requested from
        :param method: Service Method to call (as described in internal handler)
        :param args, kwargs: Arguments to send to the method
        :param timeout: A timeout
        
        :returns Request response from service from the other side
        :raises InternalError on either connection issues or the requested service responded so
        
        """

        try:
            service_broker = yield discover.cache.get_service(service, network="broker", version=False)
        except discover.DiscoveryError as e:
            raise InternalError(e.code, e.message)

        max_connections = options.internal_max_connections

        connection = yield self.__get_connection__(
            service_broker,
            max_connections=max_connections,
            connection_name="request.{0}".format(service),
            channel_prefetch_count=options.internal_channel_prefetch_count)

        context = yield connection.__declare_queue__(service)

        timer = ElapsedTime("request -> {0}@{1}".format(method, service))

        try:
            result = yield super(Internal, self).request(context, method, timeout, *args, **kwargs)
        except jsonrpc.JsonRPCError as e:
            raise InternalError(e.code, e.message, e.data)
        except jsonrpc.JsonRPCTimeout:
            raise InternalError(599, "Timed out for request {0}@{1}".format(method, service))

        logging.info(timer.done())

        raise Return(result)

    @coroutine
    def rpc(self, service, method, *args, **kwargs):
        """
        Unlike 'request' method, sends a simple RabbitMQ message to a certain service (no response is ever returned)

        :param service: Service ID the page is requested from
        :param method: Service Method to call (as described in internal handler)
        :param args, kwargs: Arguments to send to the method
        
        """

        try:
            service_broker = yield discover.cache.get_service(service, network="broker", version=False)
        except discover.DiscoveryError as e:
            raise InternalError(e.code, e.message)

        max_connections = options.internal_max_connections

        connection = yield self.__get_connection__(
            service_broker,
            max_connections=max_connections,
            connection_name="request.{0}".format(service),
            channel_prefetch_count=options.internal_channel_prefetch_count)

        context = yield connection.__declare_queue__(service)

        try:
            yield super(Internal, self).rpc(context, method, *args, **kwargs)
        except jsonrpc.JsonRPCError as e:
            raise InternalError(e.code, e.message, e.data)
        except jsonrpc.JsonRPCTimeout:
            raise InternalError(599, "Timed out for rpc {0}@{1}".format(method, service))


class InternalError(Exception):
    def __init__(self, code, body, response=None):
        self.code = code
        self.body = body
        self.response = response

    def __str__(self):
        return str(self.code) + ": " + str(self.body)
