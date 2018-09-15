
from tornado.gen import coroutine, Task

import tornado.ioloop
import logging
import zmq
import json
from zmq.eventloop import zmqstream

from pubsub import Subscriber, Publisher
from jsonrpc import JsonRPC, JsonRPCError


class ZMQInterProcess(JsonRPC):
    context = zmq.Context.instance()
    context.set(zmq.MAX_SOCKETS, 999999)

    def __init__(self, **settings):
        super(ZMQInterProcess, self).__init__()
        self.socket = None
        self.stream = None
        self.settings = settings

    def __on_receive__(self, messages):
        for msg in messages:
            tornado.ioloop.IOLoop.current().add_callback(self.received, self, msg)

    def __post_init__(self):
        self.stream = zmqstream.ZMQStream(self.socket)
        self.stream.on_recv(self.__on_receive__)

    def __pre_init__(self):
        self.socket = self.context.socket(zmq.PAIR)

    async def client(self):
        self.__pre_init__()
        path = self.settings["path"]
        logging.info("Listening as client: " + path)
        self.socket.connect("ipc://{0}".format(path))
        self.__post_init__()

    async def release(self):
        await super(ZMQInterProcess, self).release()

        path = self.settings["path"]
        logging.info("Closing: " + path)

        try:
            self.stream.on_recv(None)
            self.stream.close()
        except IOError:
            pass

    async def server(self):
        self.__pre_init__()
        path = self.settings["path"]
        logging.info("Listening as server: " + path)
        try:
            self.socket.bind("ipc://{0}".format(path))
        except zmq.ZMQError as e:
            raise JsonRPCError(500, "Failed to listen socket: " + str(e))
        self.__post_init__()

    async def write_data(self, context, data):
        try:
            self.stream.send(data)
        except IOError:
            pass
