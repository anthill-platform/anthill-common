
from tornado.ioloop import IOLoop
from asyncio import CancelledError
import zmq
from zmq.asyncio import Context
from zmq.eventloop import zmqstream

from . jsonrpc import JsonRPC, JsonRPCError

import logging
import os


class ZMQInterProcess(JsonRPC):
    context = Context.instance()
    if os.name != "nt":
        context.set(zmq.MAX_SOCKETS, 999999)

    def __init__(self, **settings):
        super(ZMQInterProcess, self).__init__()
        self.socket = None
        self.stream = None
        self.settings = settings

    async def __loop__(self):
        while True:
            try:
                messages = await self.socket.recv_multipart()
            except CancelledError:
                break
            else:
                for msg in messages:
                    await self.received(self, msg)

    def __pre_init__(self):
        # noinspection PyUnresolvedReferences
        self.socket = self.context.socket(zmq.PAIR)

    async def client(self):
        self.__pre_init__()
        path = self.settings["path"]
        logging.info("Listening as client: " + path)
        self.socket.connect("ipc://{0}".format(path))
        IOLoop.current().spawn_callback(self.__loop__)

    async def release(self):
        await super().release()
        if self.socket:
            try:
                self.socket.close()
            except IOError:
                pass

    async def server(self):
        self.__pre_init__()
        path = self.settings["path"]

        if path is None:
            logging.info("Listening as server on random port")
            try:
                port = self.socket.bind_to_random_port()
            except zmq.ZMQError as e:
                raise JsonRPCError(500, "Failed to listen socket: " + str(e))
            else:
                tcp_path = "tcp://127.0.0.1:{0}".format(port)
                logging.info("Port is: {0}".format(port))
                result = tcp_path
        else:
            logging.info("Listening as server on unix domain sockets: " + path)
            ipc_path = "ipc://{0}".format(path)
            try:
                self.socket.bind(ipc_path)
            except zmq.ZMQError as e:
                raise JsonRPCError(500, "Failed to listen socket: " + str(e))
            else:
                result = ipc_path

        IOLoop.current().spawn_callback(self.__loop__)
        return result

    async def write_data(self, context, data):
        try:
            await self.socket.send_string(data)
        except IOError:
            pass
