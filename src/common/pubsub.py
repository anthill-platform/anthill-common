
import logging
import ujson

import rabbitconn

from tornado.gen import coroutine


class Publisher(object):
    def __init__(self):
        pass

    @coroutine
    def publish(self, channel, payload):
        raise NotImplementedError()

    @coroutine
    def release(self):
        pass

    @coroutine
    def start(self):
        raise NotImplementedError()


class Subscriber(object):
    def __init__(self):
        self.handlers = {}

    @coroutine
    def on_receive(self, channel, payload):
        if channel in self.handlers:
            logging.debug("'{0}' received: {1}.".format(channel, ujson.dumps(payload)))
            yield self.handlers[channel](payload)

    @coroutine
    def release(self):
        pass

    @coroutine
    def start(self):
        logging.info("Listening for [{0}].".format(", ".join(self.handlers.keys())))

    def handle(self, channel, handler):
        self.handlers[channel] = handler


EXCHANGE_PREFIX = "pub."
QUEUE_PREFIX = "sub."


class RabbitMQSubscriber(Subscriber):

    def __init__(self, channels, broker, name=None, **settings):
        super(RabbitMQSubscriber, self).__init__()

        self.channels = channels
        self.broker = broker

        self.settings = settings
        self.connection = None
        self.queue = None
        self.consumer = None
        self.name = name or "*"

    @coroutine
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
                yield self.on_receive(channel_name, content)
        else:
            logging.error("Bad exchange name")

    @coroutine
    def release(self):
        yield self.connection.close()

    @coroutine
    def start(self):

        self.connection = rabbitconn.RabbitMQConnection(self.broker, **self.settings)
        yield self.connection.wait_connect()

        channel = yield self.connection.channel()
        self.queue = yield channel.queue(queue=QUEUE_PREFIX + self.name, auto_delete=True)

        for channel_name in self.channels:
            yield channel.exchange(
                exchange=EXCHANGE_PREFIX + channel_name,
                exchange_type='fanout')

            yield self.queue.bind(exchange=EXCHANGE_PREFIX + channel_name)

        self.consumer = yield self.queue.consume(
            consumer_callback=self.__on_message__,
            no_ack=True)

        yield super(RabbitMQSubscriber, self).start()


class RabbitMQPublisher(Publisher):
    def __init__(self, channels, broker, **settings):
        super(RabbitMQPublisher, self).__init__()

        self.channels = channels
        self.broker = broker
        self.settings = settings
        self.connection = None
        self.channel = None
        self.exchanges = {}

    @coroutine
    def publish(self, channel, payload):

        body = ujson.dumps(payload)

        logging.info("Publishing '{0}' : {1}.".format(channel, body))

        yield self.channel.basic_publish(
            exchange=EXCHANGE_PREFIX + channel,
            routing_key='',
            body=body)

    @coroutine
    def release(self):
        yield self.connection.close()

    @coroutine
    def start(self):
        # connect
        self.connection = rabbitconn.RabbitMQConnection(
            self.broker,
            **self.settings)

        yield self.connection.wait_connect()

        self.channel = yield self.connection.channel()

        for channel_name in self.channels:

            exchange = yield self.channel.exchange(
                exchange=EXCHANGE_PREFIX + channel_name,
                exchange_type='fanout')

            self.exchanges[channel_name] = exchange
