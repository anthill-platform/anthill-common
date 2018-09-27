
from tornado.ioloop import IOLoop

from . import rabbitconn

import logging
import ujson


class Publisher(object):
    def __init__(self):
        pass

    async def publish(self, channel, payload):
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

    async def on_channel_handled(self, channel_name):
        logging.info("Listening for channel '{0}'.".format(channel_name))

    async def handle(self, channel, handler):
        existing_handlers = self.handlers.get(channel, None)

        if existing_handlers is not None:
            existing_handlers.append(handler)
            return

        self.handlers[channel] = [handler]
        await self.on_channel_handled(channel)


EXCHANGE_PREFIX = "pub_"
QUEUE_PREFIX = "sub_"


class RabbitMQSubscriber(Subscriber):

    def __init__(self, broker, name=None, **settings):
        super(RabbitMQSubscriber, self).__init__()

        self.broker = broker

        self.settings = settings
        self.connection = None
        self.queue = None
        self.consumer = None
        self.name = name or "*"
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
        self.connection.close()

    async def on_channel_handled(self, channel_name):
        await self.channel.exchange(
            exchange=EXCHANGE_PREFIX + channel_name,
            exchange_type='fanout')

        await self.queue.bind(exchange=EXCHANGE_PREFIX + channel_name)
        await super(RabbitMQSubscriber, self).on_channel_handled(channel_name)

    async def start(self):

        self.connection = rabbitconn.RabbitMQConnection(
            self.broker,
            connection_name="sub." + self.name,
            **self.settings)
        await self.connection.wait_connect()

        self.channel = await self.connection.channel(prefetch_count=self.settings.get("channel_prefetch_count", 1024))
        self.queue = await self.channel.queue(queue=QUEUE_PREFIX + self.name, auto_delete=True)

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

    async def publish(self, channel, payload):

        if channel not in self.exchanges:
            await self.channel.exchange(
                exchange=EXCHANGE_PREFIX + channel,
                exchange_type='fanout')
            self.exchanges.add(channel)

        body = ujson.dumps(payload)

        logging.info("Publishing '{0}' : {1}.".format(channel, body))

        self.channel.basic_publish(
            exchange=EXCHANGE_PREFIX + channel,
            routing_key='',
            body=body)

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
