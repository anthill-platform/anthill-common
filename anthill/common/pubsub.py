
from tornado.ioloop import IOLoop

from . import rabbitconn
from pika.exceptions import ChannelClosed

import logging
import ujson


class Publisher(object):
    def __init__(self):
        pass

    async def publish(self, channel, payload, routing_key=''):
        raise NotImplementedError()

    async def release(self):
        pass

    async def start(self):
        raise NotImplementedError()


class Subscriber(object):
    def __init__(self):
        self.handlers = {}

    async def on_receive(self, channel, payload):
        handlers = self.handlers.get(channel, None)
        if handlers is not None:
            for handler in handlers:
                await handler(payload)

    async def release(self):
        pass

    async def start(self):
        pass

    async def on_channel_handled(self, channel_name, routing_key=None):
        logging.info("Listening for channel '{0}'.".format(channel_name))

    async def handle(self, channel, handler, routing_key=None):
        existing_handlers = self.handlers.get(channel, None)

        if existing_handlers is not None:
            existing_handlers.append(handler)
            return

        self.handlers[channel] = [handler]
        await self.on_channel_handled(channel, routing_key=routing_key)


EXCHANGE_PREFIX = "pub_"
QUEUE_PREFIX = "sub_"


class RabbitMQSubscriber(Subscriber):

    def __init__(self, broker, name=None, round_robin=True, **settings):
        super(RabbitMQSubscriber, self).__init__()

        self.broker = broker

        self.settings = settings
        self.connection = None
        self.queue = None
        self.consumer = None
        self.name = name or "*"
        self.round_robin = round_robin
        self.channel = None

    def __on_message__(self, channel, method, properties, body):

        exchange_name = method.exchange
        if exchange_name.startswith(EXCHANGE_PREFIX):
            # cut first letters
            channel_name = exchange_name[len(EXCHANGE_PREFIX):]

            logging.debug("Received '{0}' : {1}.".format(channel_name, body))

            try:
                content = ujson.loads(body)
            except (KeyError, ValueError):
                logging.exception("Failed to decode incoming message")
            else:
                IOLoop.current().spawn_callback(self.on_receive, channel_name, content)
        else:
            logging.error("Bad exchange name")

        channel.basic_ack(delivery_tag=method.delivery_tag)

    async def release(self):
        if self.queue:
            await self.queue.delete()
        self.connection.close()

    async def on_channel_handled(self, channel_name, routing_key=None):
        await self.channel.exchange(
            exchange=EXCHANGE_PREFIX + channel_name,
            exchange_type='direct' if routing_key else 'fanout')

        await self.queue.bind(exchange=EXCHANGE_PREFIX + channel_name, routing_key=routing_key)
        await super(RabbitMQSubscriber, self).on_channel_handled(channel_name, routing_key=routing_key)

    async def start(self):

        self.connection = rabbitconn.RabbitMQConnection(
            self.broker,
            connection_name="sub." + self.name,
            **self.settings)
        await self.connection.wait_connect()

        self.channel = await self.connection.channel(prefetch_count=self.settings.get("channel_prefetch_count", 1024))

        if self.round_robin:
            self.queue = await self.channel.queue(queue=QUEUE_PREFIX + self.name, auto_delete=True)
        else:
            self.queue = await self.channel.queue(exclusive=True)

        self.consumer = await self.queue.consume(
            consumer_callback=self.__on_message__,
            no_ack=False)

        await super(RabbitMQSubscriber, self).start()


class RabbitMQPublisher(Publisher):
    def __init__(self, broker, name, **settings):
        super(RabbitMQPublisher, self).__init__()

        self.broker = broker
        self.settings = settings
        self.connection = None
        self.channel = None
        self.exchanges = set()
        self.name = name

    async def publish(self, channel, payload, routing_key=''):

        body = ujson.dumps(payload)

        logging.info("Publishing '{0}' : {1}.".format(channel, body))

        try:
            self.channel.basic_publish(
                exchange=EXCHANGE_PREFIX + channel,
                routing_key=routing_key,
                body=body)
        except ChannelClosed:
            logging.info("Channel '{0}' closed.".format(channel))
            await self.release()
            await self.start()

    async def release(self):
        self.connection.close()

    async def start(self):
        # connect
        self.connection = rabbitconn.RabbitMQConnection(
            self.broker,
            connection_name="pub." + str(self.name),
            **self.settings)

        await self.connection.wait_connect()
        self.channel = await self.connection.channel()
