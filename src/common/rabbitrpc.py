
import pika

from aqmp import AMQPConnection, AMQPQueue
from pika.exceptions import ChannelClosed

from tornado.gen import coroutine, Return, Future

import jsonrpc
import rabbitconn

import tornado.ioloop
import logging
import ujson

"""
Asynchronous JSON-RPC protocol implementation for RabbitMQ. See http://www.jsonrpc.org/specification
"""


class Context(object):
    def __init__(self, channel=None, routing_key=None, reply_to=None):
        self.channel = channel
        self.routing_key = routing_key or (lambda: None)
        self.reply_to = reply_to or (lambda: None)
        self.return_future = None


class JsonAMQPConnection(rabbitconn.RabbitMQConnection):
    @coroutine
    def __declare_queue__(self, name):
        context = self.named_channels.get(name)
        if context:
            if context.channel.is_open:
                raise Return(context)
            else:
                del self.named_channels[name]
                self.queues.pop(name, None)

        context = Context(channel=None, routing_key=None, reply_to=None)

        def on_return_callback(ch, method, properties, body):
            if context.return_future:
                if method.reply_code:
                    context.return_future.set_exception(jsonrpc.JsonRPCError(method.reply_code, method.reply_text))
                context.return_future = None

        def confirm_delivery(method):
            if context.return_future:
                context.return_future.set_result(True)
                context.return_future = None

        try:
            channel = yield self.acquire_channel(on_return_callback=on_return_callback,
                                                 confirm_delivery=confirm_delivery)
        except Exception as e:
            raise jsonrpc.JsonRPCError(500, "Failed to acquire a channel: " + e.message)

        callback_queue = yield channel.queue(exclusive=True)

        context.channel = channel
        context.routing_key = lambda: "rpc." + name
        context.reply_to = lambda: callback_queue.routing_key

        self.named_channels[name] = context
        self.queues[name] = callback_queue

        @coroutine
        def response(channel, method, properties, body):
            yield self.mq.received(context, body, id=int(properties.correlation_id))

        self.consumers[name] = callback_queue.consume(response, no_ack=True)

        raise Return(context)

    def __init__(self, mq, broker, connection_name=None, **kwargs):
        super(JsonAMQPConnection, self).__init__(broker, connection_name, **kwargs)
        self.named_channels = {}
        self.queues = {}
        self.consumers = {}
        self.mq = mq


class JsonAMQPConnectionPool(rabbitconn.RoundRobinPool):
    def __init__(self, mq, broker, max_connections, connection_name=None, **kwargs):
        super(JsonAMQPConnectionPool, self).__init__(max_connections, **kwargs)
        self.mq = mq
        self.broker = broker
        self.connection_name = connection_name

    @coroutine
    def __new_object__(self, **kwargs):
        connection = JsonAMQPConnection(self.mq, self.broker, self.connection_name, **kwargs)
        yield connection.wait_connect()

        logging.debug("New connection constructed in a pool")

        raise Return(connection)


class RabbitMQJsonRPC(jsonrpc.JsonRPC):
    @coroutine
    def __get_connection__(self, broker, max_connections, **kwargs):
        if broker in self.pools:
            pool = self.pools[broker]
            connection = yield pool.get()
            raise Return(connection)

        pool = JsonAMQPConnectionPool(
            self, broker, max_connections=max_connections, **kwargs)

        logging.debug("New connection pool created: " + broker)

        connection = yield pool.get()

        self.pools[broker] = pool

        raise Return(connection)

    @coroutine
    def __on_connected__(self, *args, **kwargs):
        pass

    @coroutine
    def __incoming_request__(self, channel, method, properties, body):
        payload = {}

        if properties.correlation_id:
            try:
                payload["id"] = int(properties.correlation_id or "-1")
            except ValueError:
                logging.error("Bad correlation id received: " + str(properties.correlation_id))
                # ignore that message
                return

        context = Context(self.listen_channel,
                          routing_key=lambda: str(properties.reply_to),
                          reply_to=lambda: self.callback_queue.routing_key)

        yield self.received(context, body, **payload)

    def __init__(self):
        super(RabbitMQJsonRPC, self).__init__()

        self.req_channel = None
        self.req_queue = None
        self.pools = {}
        self.listen_connection = None
        self.listen_context = None

        self.listen_channel = None
        self.handler_queue = None
        self.callback_queue = None
        self.handler_consumer = None
        self.callback_consumer = None

    @coroutine
    def listen(self, broker, internal_name, on_receive):
        self.listen_connection = JsonAMQPConnection(
            self,
            broker,
            connection_name="rpc." + internal_name)

        yield self.listen_connection.wait_connect()

        self.listen_channel = yield self.listen_connection.channel()

        # initial incoming request queue

        self.handler_queue = yield self.listen_channel.queue(queue="rpc." + internal_name, auto_delete=True)

        # a queue for response callbacks`
        #
        #  other server                 | our server
        #    a request                 --> processing (handler_queue)
        #    response processing       <-- process result
        #    response processing error --> notification (callback_queue)

        self.callback_queue = yield self.listen_channel.queue(exclusive=True)

        self.listen_context = Context(
            self.listen_channel,
            routing_key=lambda: self.callback_queue.routing_key,
            reply_to=lambda: self.handler_queue.routing_key)

        self.handler_consumer = yield self.handler_queue.consume(
            consumer_callback=self.__incoming_request__,
            no_ack=True)

        self.callback_consumer = yield self.callback_queue.consume(
            consumer_callback=self.__incoming_request__,
            no_ack=True)

        self.set_receive(on_receive)

    @coroutine
    def write_object(self, context, data, **payload):

        channel = context.channel

        if not channel.is_active:
            return

        routing_key = context.routing_key()
        reply_to = context.reply_to()

        correlation_id = payload.get("id", None)
        if correlation_id:
            correlation_id = str(correlation_id)

        properties = pika.BasicProperties(
            correlation_id=correlation_id,
            reply_to=str(reply_to),
            delivery_mode=1
        )

        logging.debug("Sending: {0} to {1} reply {2}".format(ujson.dumps(data), routing_key, reply_to))

        try:
            yield channel.basic_publish(
                exchange='',
                routing_key=str(routing_key),
                properties=properties,
                body=ujson.dumps(data),
                mandatory=True)
        except ChannelClosed:
            raise jsonrpc.JsonRPCError(503, "Channel is closed")
