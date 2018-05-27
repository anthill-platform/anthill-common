
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

    @coroutine
    def client(self):
        self.__pre_init__()
        path = self.settings["path"]
        logging.info("Listening as client: " + path)
        self.socket.connect("ipc://{0}".format(path))
        self.__post_init__()

    @coroutine
    def release(self):
        yield super(ZMQInterProcess, self).release()

        path = self.settings["path"]
        logging.info("Closing: " + path)

        try:
            self.stream.on_recv(None)
            self.stream.close()
        except IOError:
            pass

    @coroutine
    def server(self):
        self.__pre_init__()
        path = self.settings["path"]
        logging.info("Listening as server: " + path)
        try:
            self.socket.bind("ipc://{0}".format(path))
        except zmq.ZMQError as e:
            raise JsonRPCError(500, "Failed to listen socket: " + str(e))
        self.__post_init__()

    @coroutine
    def write_data(self, context, data):
        try:
            self.stream.send(data)
        except IOError:
            pass


class ZMQPublisher(Publisher):
    def __init__(self, **settings):
        super(ZMQPublisher, self).__init__()

        self.context = None
        self.stream = None
        self.socket = None

        self.port = settings["port"]
        self.host = settings["host"]

    @coroutine
    def publish(self, channel, payload):
        logging.debug("Publishing '{0}' : {1}.".format(channel, json.dumps(payload)))

        content = {
            "ch": channel,
            "pl": payload
        }

        self.stream.send_json(content)


    @coroutine
    def start(self):
        # connect
        self.context = zmq.Context()

        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind("tcp://{0}:{1}".format(self.host, self.port))

        self.stream = zmqstream.ZMQStream(self.socket)

        yield super(ZMQPublisher, self).start()


class ZMQSubscriber(Subscriber):
    def __init__(self, **settings):
        super(ZMQSubscriber, self).__init__()
        self.context = zmq.Context()

        self.stream = None
        self.socket = None
        self.port = settings["port"]

    def __on_message__(self, messages):
        for msg in messages:
            try:
                content = json.loads(msg)
            except (KeyError, ValueError):
                logging.exception("Failed to decode incoming message")
            else:
                channel = content["ch"]
                payload = content["pl"]

                self.on_receive(channel, payload)

    @coroutine
    def start(self):
        # connect
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect("tcp://*:{0}".format(self.port))
        self.socket.setsockopt(zmq.SUBSCRIBE, "")

        self.stream = zmqstream.ZMQStream(self.socket)
        self.stream.on_recv(self.__on_message__)

        yield super(ZMQSubscriber, self).start()

