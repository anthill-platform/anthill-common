
from tornado.gen import coroutine, Future, Return
from tornado.ioloop import IOLoop

import pika
import aqmp
import logging
import socket


from abc import ABCMeta, abstractmethod


class RabbitMQConnection(aqmp.AMQPConnection):

    SOCKET_TIMEOUT = 1.0
    CHANNEL_DEFAULT_PREFETCH_COUNT = 1024

    def __init__(self, broker, connection_name=None, channel_prefetch_count=CHANNEL_DEFAULT_PREFETCH_COUNT, **kwargs):

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

    @coroutine
    def __connected__(self, connection, *args, **kwargs):

        try:
            sock_name = str(connection.socket.getsockname())
        except socket.error:
            sock_name = "Unknown"
            pass

        logging.info("Connected to rabbitmq: {0}".format(
            str(sock_name) + " " + self.connection_name if self.connection_name else str(sock_name)
        ))
        if self.connected:
            self.connected.set_result(True)
        self.connected = None

    @coroutine
    def __closed__(self, connection, *args, **kwargs):
        logging.info("Connection lost: {0}".format(
            self.connection_name if self.connection_name else str(connection)
        ))

    @coroutine
    def wait_connect(self):
        if self.connected:
            yield self.connected

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

    @coroutine
    def get(self):
        if self.next_id < self.max_objects:
            obj = yield self.__new_object__(**self.object_args)
            self.append(obj)
        else:
            index = self.next_id % self.max_objects
            obj = self[index]
            if not obj:
                obj = yield self.__new_object__(**self.object_args)
                self[index] = obj

        self.next_id += 1
        raise Return(obj)

    def remove_object(self, obj):
        index = self.index(obj)
        if index >= 0:
            self[index] = None


class RabbitMQConnectionPool(RoundRobinPool):
    def __init__(self, broker, max_connections, connection_name=None, **kwargs):
        super(RabbitMQConnectionPool, self).__init__(
            max_connections, broker=broker, connection_name=connection_name, **kwargs)

    @coroutine
    def __new_object__(self, **kwargs):
        connection = RabbitMQConnection(**kwargs)
        yield connection.wait_connect()
        raise Return(connection)


class RabbitMQPooledChannel(object):
    def __init__(self, pool, channel):
        self.pool = pool
        self.channel = channel

    def __enter__(self):
        return self.channel

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.release(self.channel)


class RabbitMQChannelPool(object):
    def __init__(self, connection, channel_prefetch_count=RabbitMQConnection.CHANNEL_DEFAULT_PREFETCH_COUNT, **kwargs):
        self.connection = connection
        self.channels = list()
        self.channel_prefetch_count = channel_prefetch_count

    @coroutine
    def acquire(self, *args, **kwargs):
        while self.channels:
            channel = self.channels.pop(0)

            # get the first working channel
            if channel.is_open:
                raise Return(channel)

        channel = yield self.connection.channel(prefetch_count=self.channel_prefetch_count, *args, **kwargs)

        raise Return(channel)

    def clear(self):
        self.channels = list()

    @coroutine
    def with_channel(self):
        """
        Used with a 'with' statement (with auto returning to the pool):

        with (yield pool.with_channel()) as channel:
            ...
            ...

        """
        if self.channels:
            raise Return(RabbitMQPooledChannel(self, self.channels.pop(0)))

        channel = yield self.connection.channel()
        raise Return(RabbitMQPooledChannel(self, channel))

    def release(self, channel):
        if channel.is_open:
            self.channels.append(channel)
