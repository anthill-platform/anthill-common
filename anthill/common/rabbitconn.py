
from tornado.gen import coroutine, Future, Return
from tornado.ioloop import IOLoop

from . import aqmp

import pika
import logging
import socket


from abc import ABCMeta, abstractmethod


class RabbitMQConnection(aqmp.AMQPConnection):

    SOCKET_TIMEOUT = 1.0

    def __init__(self, broker, connection_name=None, channel_prefetch_count=0, **kwargs):

        self.connected = Future()

        params = pika.URLParameters(broker)
        params.socket_timeout = RabbitMQConnection.SOCKET_TIMEOUT

        self.connection_name = connection_name
        if connection_name:
            params.client_properties = {
                "connection_name": connection_name
            }

        super(RabbitMQConnection, self).__init__(
            params,
            io_loop=IOLoop.instance(),
            on_open_callback=self.__connected__,
            on_close_callback=self.__closed__,
            **kwargs
        )

        self.channel_pool = RabbitMQChannelPool(
            self,
            channel_prefetch_count=channel_prefetch_count)

    async def __connected__(self, connection, *args, **kwargs):
        sock_name = str(connection.server_properties.get("cluster_name", "Unknown"))

        logging.info("Connected to rabbitmq: {0}".format(
            str(sock_name) + " " + self.connection_name if self.connection_name else str(sock_name)
        ))
        if self.connected:
            self.connected.set_result(True)
        self.connected = None

    async def __closed__(self, connection, *args, **kwargs):
        logging.info("Connection lost: {0}".format(
            self.connection_name if self.connection_name else str(connection)
        ))

    async def wait_connect(self):
        if self.connected:
            await self.connected

    def with_channel(self):
        return self.channel_pool.with_channel()

    def acquire_channel(self, *args, **kwargs):
        return self.channel_pool.acquire(*args, **kwargs)

    def release_channel(self, channel):
        self.channel_pool.release(channel)


class RoundRobinPool(list):

    __metaclass__ = ABCMeta

    def __init__(self, max_objects, **kwargs):
        super(RoundRobinPool, self).__init__()

        self.max_objects = max_objects
        self.object_args = kwargs
        self.next_id = 0

    @abstractmethod
    def __new_object__(self, **kwargs):
        """
        Should be a coroutine to construct a new object
        :param kwargs: kwargs passed to the RoundRobinPool constructor
        """
        raise NotImplementedError()

    async def get(self):
        if self.next_id < self.max_objects:
            obj = await self.__new_object__(**self.object_args)
            self.append(obj)
        else:
            index = self.next_id % self.max_objects
            obj = self[index]
            if not obj:
                obj = await self.__new_object__(**self.object_args)
                self[index] = obj

        self.next_id += 1
        return obj

    def remove_object(self, obj):
        index = self.index(obj)
        if index >= 0:
            self[index] = None


class RabbitMQConnectionPool(RoundRobinPool):
    def __init__(self, broker, max_connections, connection_name=None, **kwargs):
        super(RabbitMQConnectionPool, self).__init__(
            max_connections, broker=broker, connection_name=connection_name, **kwargs)

    async def __new_object__(self, **kwargs):
        connection = RabbitMQConnection(**kwargs)
        await connection.wait_connect()
        return connection


class RabbitMQPooledChannel(object):
    def __init__(self, pool, channel):
        self.pool = pool
        self.channel = channel

    def __enter__(self):
        return self.channel

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.release(self.channel)


class RabbitMQChannelPool(object):
    def __init__(self, connection, channel_prefetch_count=0, **kwargs):
        self.connection = connection
        self.channels = list()
        self.channel_prefetch_count = channel_prefetch_count

    async def acquire(self, *args, **kwargs):
        while self.channels:
            channel = self.channels.pop(0)

            # get the first working channel
            if channel.is_open:
                return channel

        channel = await self.connection.channel(prefetch_count=self.channel_prefetch_count, *args, **kwargs)

        return channel

    def clear(self):
        self.channels = list()

    async def with_channel(self):
        """
        Used with a 'with' statement (with auto returning to the pool):

        with (await pool.with_channel()) as channel:
            ...
            ...

        """
        if self.channels:
            return RabbitMQPooledChannel(self, self.channels.pop(0))

        channel = await self.connection.channel()
        return RabbitMQPooledChannel(self, channel)

    def release(self, channel):
        if channel.is_open:
            self.channels.append(channel)
