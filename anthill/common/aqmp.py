#!/usr/bin/python

import datetime
import logging
import threading
import weakref

from tornado.gen import Task, Future, convert_yielded
from tornado.ioloop import IOLoop
from asyncio import iscoroutine

import tornado
import tornado.ioloop
import tornado.concurrent
import pika

"""
Pika-Tornado: a Tornado coroutine-based abstraction layer for Pika.
"""


# (C) 2015 Stuart Longland <stuartl@longlandclan.yi.org>
# Released under the terms of the Mozilla Public License v2.0

class AMQPObject(object):
    @classmethod
    def _get_log(cls, *name):
        return logging.getLogger('.'.join(
            (cls.__module__, cls.__name__) + name))


# noinspection PyProtectedMember,PyPropertyDefinition
class AMQPConnection(AMQPObject):
    """
    Connection to AMQP, this object handles unexpected disconnections (and
    re-connects accordingly) and wraps the channel opening process in a
    coroutine-friendly manner.

    Support for multiple IOLoops is provided and thread-safety is attempted.

    Callbacks may be specified in a number of ways:
    - as a reference to the function directly
    - as a tuple:
        - first element gives the IOLoop (or None for the client's IOLoop)
        - second element gives the reference to the function
        - third element if present gives the initial args
        - forth element if present gives the initial kwargs
    """

    CONNECTION_CLOSED = pika.connection.Connection.CONNECTION_CLOSED
    CONNECTION_INIT = pika.connection.Connection.CONNECTION_INIT
    CONNECTION_PROTOCOL = pika.connection.Connection.CONNECTION_PROTOCOL
    CONNECTION_START = pika.connection.Connection.CONNECTION_START
    CONNECTION_TUNE = pika.connection.Connection.CONNECTION_TUNE
    CONNECTION_OPEN = pika.connection.Connection.CONNECTION_OPEN
    CONNECTION_CLOSING = pika.connection.Connection.CONNECTION_CLOSING

    def __init__(self, parameters, on_open_callback=None,
                 on_open_error_callback=None, on_close_callback=None,
                 on_giveup_callback=None, reconnect_delay=5.0, reconnect_max=-1,
                 io_loop=None, io_thread=None):

        """
        Initialise a connection to AMQP.

        :param: pika.connection.Parameters parameters:
            Connection parameters, see Pika documentation.

        :param method on_open_callback:
            Called when the connection is opened.

        :param method on_open_error_callback:
            Called if the connection can't be opened.

        :param method on_close_callback:
            Called when the connection is closed.

        :param method on_giveup_callback:
            Called when the connection attempts have been exhausted
            and this library "gives up".  This might be used to log
            an alarm and shut down the daemon.

        :param float reconnect_delay:
            The amount of time to wait before attempting to reconnect.
            Set to 0 to disable reconnect.

        :param int reconnect_max:
            The maximum number of reconnection attempts.  Set to 0 to
            permit unlimited reconnections.

        :param tornado.ioloop.IOLoop io_loop:
            IOLoop instance to use for the AMQP client itself.  By default,
            the current IOLoop is used.

        :param threading.Thread io_thread:
            Thread used for AMQP communications.  Since Pika itself is not
            thread-safe, it is imperative that all AMQP operations take place
            in the same thread as the AMQP IOLoop instance.

            If this is different to the thread the constructor is running in
            for whatever reason, the reference to that thread can be given
            here.
        """

        if io_loop is None:
            io_loop = tornado.ioloop.IOLoop.current()
        if io_thread is None:
            io_thread = threading.current_thread()

        self._io_loop = io_loop
        self._io_thread = io_thread
        self._reconnect_delay = datetime.timedelta(seconds=reconnect_delay)
        self._reconnect_max = reconnect_max
        self._reconnect_rem = reconnect_max
        self._reconnect = reconnect_delay > 0
        self._giveup = False

        self._on_open_cb = []
        self._on_open_error_cb = []
        self._on_close_cb = []
        self._on_giveup_cb = []
        if on_open_callback is not None:
            self._on_open_cb.append(on_open_callback)
        if on_open_error_callback is not None:
            self._on_open_error_cb.append(on_open_error_callback)
        if on_close_callback is not None:
            self._on_close_cb.append(on_close_callback)
        if on_giveup_callback is not None:
            self._on_giveup_cb.append(on_giveup_callback)

        self._amqp_parameters = parameters
        self._amqp_connection = None
        self._channels = []
        self._replace_connection()

    # ------------------------------------------------------------------------
    # Connection state
    # ------------------------------------------------------------------------

    connection_state = property(lambda s: s._amqp_connection.connection_state)
    is_closed = property(lambda s: s._amqp_connection.is_closed)
    is_closing = property(lambda s: s._amqp_connection.is_closing)
    is_open = property(lambda s: s._amqp_connection.is_open)

    # ------------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------------

    # noinspection PyBroadException
    def _replace_connection(self):
        log = self._get_log('_replace_connection')
        try:
            log.debug('Gathering a list of existing channels')
            # Grab a list of channels
            channels = self._get_channels()
            log.debug('We have %s channels to reopen.  Opening '
                      'connection to AMQP with parameters %s.',
                      len(channels), self._amqp_parameters)

            # Replace the connection
            self._amqp_connection = pika.adapters.TornadoConnection(
                parameters=self._amqp_parameters,
                on_open_callback=self._handle_on_open_callback,
                on_open_error_callback=self._handle_on_open_error_callback,
                on_close_callback=self._handle_on_close_callback,
                custom_ioloop=self._io_loop)
            log.debug('AMQP connection started')

            # Mark these channels as stale
            for ch in channels:
                try:
                    log.debug('Re-opening %s', ch)
                    ch._mark_stale()
                except Exception:
                    pass
        except Exception:
            self._do_reconnect()
            log.exception('Failed to replace connection')

    def connect(self):
        """
        Connect to AMQP.
        """
        self._amqp_connection.connect()

    def close(self, reply_code=200, reply_text='Normal shutdown'):
        """
        Disconnect from AMQP.  This disables reconnections.
        See pika.BaseConnection.close for details on arguments.
        """
        self._giveup = False
        self._reconnect = False
        self._amqp_connection.close(reply_code=reply_code, reply_text=reply_text)

    def set_backpressure_multiplier(self, value):
        self._amqp_connection.set_backpressure_multiplier(value)

    # ------------------------------------------------------------------------
    # Connection callbacks
    # ------------------------------------------------------------------------

    def add_backpressure_callback(self, callback):
        self._amqp_connection.add_on_close_callback(callback)

    def add_on_close_callback(self, callback):
        self._amqp_connection.add_on_close_callback(callback)

    def add_on_open_callback(self, callback):
        self._amqp_connection.add_on_close_callback(callback)

    def add_on_open_error_callback(self, callback):
        self._amqp_connection.add_on_open_error_callback(callback)

    # ------------------------------------------------------------------------
    # Channel management
    # ------------------------------------------------------------------------

    def _cleanup_channels(self):
        self._channels = list(filter(lambda cr: cr() is not None, self._channels))

    def _get_channels(self):
        self._cleanup_channels()
        return [ch() for ch in self._channels]

    async def channel(self, channel_number=None, prefetch_count=None,
                      on_close_callback=None, on_return_callback=None,
                      on_cancel_callback=None, on_flow_callback=None, confirm_delivery=None):
        """
        Open a new channel on the AMQP connection.
        """
        ch = AMQPChannel(self, channel_number, prefetch_count,
                         on_close_callback, on_return_callback, on_cancel_callback,
                         on_flow_callback, confirm_delivery)

        await ch._init_channel()
        self._cleanup_channels()
        self._channels.append(weakref.ref(ch))
        return ch

    # ------------------------------------------------------------------------
    # Connection handling callbacks
    # ------------------------------------------------------------------------

    def _handle_on_open_callback(self, *args, **kwargs):
        """
        What to do when the connection is opened?
        """
        log = self._get_log('_handle_on_open_callback')
        # noinspection PyBroadException
        try:
            log.info('Connection open')
            self._reconnect = self._reconnect_delay.total_seconds() > 0
            self._reconnect_rem = self._reconnect_max
            self._giveup = False

            # Grab a list of channels
            channels = self._get_channels()
            # Re-open those channels
            for ch in channels:
                log.debug('Re-opening %s', ch)
                self._io_loop.add_callback(ch._init_channel)

            log.debug('Calling open callbacks')
            for cb in self._on_open_cb:
                self._io_loop.add_callback(cb, *args, **kwargs)
        except Exception:
            log.exception('Failed to handle connection open')

    # noinspection PyBroadException
    def _handle_on_close_callback(self, *args, **kwargs):
        """
        What to do when the connection is closed?
        """
        log = self._get_log('_handle_on_close_callback')
        try:
            log.info('Connection closed')
            for cb in self._on_close_cb:
                try:
                    IOLoop.current().spawn_callback(cb, *args, **kwargs)
                except Exception:
                    log.exception('Exception in callback %s(*%s, **%s)',
                                  cb, args, kwargs)
        except Exception:
            log.exception('Exception in close callback')
        self._do_reconnect()

    # noinspection PyBroadException
    def _handle_on_open_error_callback(self, connection, error,
                                       *args, **kwargs):
        """
        What to do if we fail to open the connection?
        """
        log = self._get_log('_handle_on_open_error_callback')
        try:
            log.error('Received %s whilst attempting connection', error)

            # Pass it through to the callback handler we were given
            if self._on_open_error_cb is not None:
                for cb in self._on_open_error_cb:
                    try:
                        cb(connection, error, *args, **kwargs)
                    except Exception:
                        log.exception('Error in callback %s(*%s, **%s)',
                                      cb, args, kwargs)
        except Exception:
            log.exception('Failed in open-error callback')
        self._do_reconnect()

    # noinspection PyBroadException
    def _do_reconnect(self):
        """
        Perform a re-connection, if enabled.
        """
        log = self._get_log('_reconnect')
        try:
            # Sanity check, ensure we're running in the correct thread for this.
            # add_timeout is *NOT* thread-safe.
            if not self._is_own_thread:
                # This should do the trick!
                self._io_loop.add_callback(self._do_reconnect)
                return

            # Reconnect?
            if self._reconnect:
                if self._reconnect_max > 0:
                    self._reconnect_rem -= 1
                    self._reconnect = self._reconnect_rem > 0
                    self._giveup = not self._reconnect
                    if self._giveup:
                        log.error('This is our last connection attempt!')

                # Schedule a reconnect.  We do this by completely replacing the
                # AMQP connection object.
                self._io_loop.add_timeout(
                    self._reconnect_delay, self._replace_connection)
                log.error('Re-connecting in %s.',
                          self._reconnect_delay)
            elif self._giveup:
                log.fatal('Giving up!')
                for cb in self._on_giveup_cb:
                    try:
                        cb()
                    except Exception:
                        log.exception('Exception in callback %s', cb)
        except Exception:
            log.exception('Failure in reconnection')

    # ------------------------------------------------------------------------
    # Callback and thread-safety utilities
    # ------------------------------------------------------------------------

    @property
    def _is_own_thread(self):
        """
        Return True if we're running in our own thread.
        """
        return self._io_thread is threading.current_thread()

    def _schedule_timeout(self, timeout, future, cb_func=None,
                          cb_args=None, cb_kwargs=None):
        """
        Place a TimeoutException into the future if the future is not
        completed in time.
        """
        assert self._is_own_thread, 'This isn\'t my thread!'
        if cb_args is None:
            cb_args = ()
        if cb_kwargs is None:
            cb_kwargs = {}

        def _timeout():
            if not future.done():
                future.set_exception(TimeoutError())
            if cb_func is not None:
                cb_func(*cb_args, **cb_kwargs)

        return self._io_loop.add_timeout(timeout, _timeout)


# noinspection PyProtectedMember,PyPropertyDefinition
class AMQPChannel(AMQPObject):
    """
    An abstraction for the AMQP Channel.
    """

    def __init__(self, connection, channel_number, prefetch_count,
                 on_close_callback=None, on_return_callback=None,
                 on_cancel_callback=None, on_flow_callback=None, confirm_delivery=None):
        """
        Initialise a new channel object.
        """
        log = self._get_log('__init__')
        log.debug('New channel')
        self._connection = connection
        self._channel_number = channel_number
        self._prefetch_count = prefetch_count
        self._on_close_callback = on_close_callback
        self._on_return_callback = on_return_callback
        self._on_cancel_callback = on_cancel_callback
        self._on_flow_callback = on_flow_callback
        self._confirm_delivery = confirm_delivery
        self._channel = None

        # Queues and exchanges to re-establish
        self._exchanges = []
        self._queues = []

    _amqp_connection = property(lambda s: s._connection._amqp_connection)

    def _mark_stale(self):
        self._get_log('_mark_stale').debug(
            'Channel is now stale')
        self._channel = None

    # noinspection PyBroadException
    async def _init_channel(self):
        log = self._get_log('channel')

        log.debug('Opening new channel (number=%s)', self._channel_number)
        try:
            f = Future()

            def _callback(ch):
                f.set_result(ch)

            self._amqp_connection.channel(on_open_callback=_callback)
            channel = await f
        except Exception:
            log.exception('Failed to open channel')
            raise

        self._channel = channel

        if self._prefetch_count:
            await self.basic_qos(prefetch_count=self._prefetch_count)

        # a set of callbacks that will stay up to date even if reconnection took place

        if self._on_close_callback:
            self._channel.add_on_close_callback(self._on_close_callback)
        if self._on_return_callback:
            self._channel.add_on_return_callback(self._on_return_callback)
        if self._on_cancel_callback:
            self._channel.add_on_cancel_callback(self._on_cancel_callback)
        if self._on_flow_callback:
            self._channel.add_on_flow_callback(self._on_flow_callback)
        if self._confirm_delivery:
            self._channel.confirm_delivery(self._confirm_delivery, nowait=True)

        # If we had any exchanges, declare those now
        for ex in filter(bool, self._get_exchanges()):
            try:
                log.debug('Declaring exchange %s', ex.exchange)
                await ex.declare()
            except Exception:
                log.exception('Failed to declare exchange %s', ex.exchange)

        # If we had any queues, declare those now
        for q in filter(bool, self._get_queues()):
            try:
                log.debug('Declaring queue %s', q.routing_key)
                await q.declare()
            except Exception:
                log.exception('Failed to declare exchange %s', q.routing_key)

    # ------------------------------------------------------------------------
    # Channel status
    # ------------------------------------------------------------------------

    is_closed = property(lambda s: s._channel.is_closed)
    is_closing = property(lambda s: s._channel.is_closing)
    is_open = property(lambda s: s._channel.is_open)
    is_active = property(lambda s: bool(s._channel))

    # ------------------------------------------------------------------------
    # Channel callbacks
    # ------------------------------------------------------------------------

    def add_callback(self, callback):
        self._channel.add_callback(callback)

    def add_on_cancel_callback(self, callback):
        self._channel.add_on_cancel_callback(callback)

    def add_on_close_callback(self, callback):
        self._channel.add_on_close_callback(callback)

    def add_on_flow_callback(self, callback):
        self._channel.add_on_flow_callback(callback)

    def add_on_return_callback(self, callback):
        self._channel.add_on_return_callback(callback)

    # ------------------------------------------------------------------------
    # Channel operations
    # ------------------------------------------------------------------------

    def basic_ack(self, delivery_tag=0, multiple=False):
        self._channel.basic_ack(delivery_tag=delivery_tag, multiple=multiple)

    def basic_cancel(self, consumer_tag=''):
        return Task(self._channel.basic_cancel, consumer_tag=consumer_tag)

    def basic_consume(self, consumer_callback, queue='', no_ack=False,
                      exclusive=False, consumer_tag=None, arguments=None):
        return Task(self._channel.basic_consume,
                    on_message_callback=consumer_callback, queue=queue, auto_ack=no_ack,
                    exclusive=exclusive, consumer_tag=consumer_tag, arguments=arguments)

    def basic_get(self, queue='', no_ack=False):
        return Task(self._channel.basic_get, queue=queue, no_ack=no_ack)

    def basic_nack(self, delivery_tag=None, multiple=False, requeue=True):
        self._channel.basic_nack(delivery_tag=delivery_tag, multiple=multiple, requeue=requeue)

    def basic_publish(self, exchange, routing_key, body, properties=None,
                      mandatory=False, immediate=False):
        self._channel.basic_publish(exchange, routing_key, body, properties,
                                    mandatory, immediate)

    def basic_qos(self, prefetch_size=0, prefetch_count=0, all_channels=False):
        return Task(self._channel.basic_qos, prefetch_size=prefetch_size, prefetch_count=prefetch_count,
                    all_channels=all_channels)

    def basic_reject(self, delivery_tag=None, requeue=True):
        self._channel.basic_reject(delivery_tag, requeue)

    def basic_recover(self, requeue=False):
        return Task(self._channel.basic_recover(requeue=requeue))

    def close(self, reply_code=0, reply_text="Normal shutdown"):
        self._channel.close(reply_code=reply_code, reply_text=reply_text)

    def confirm_delivery(self, callback=None):
        return Task(self._channel.confirm_delivery, ack_nack_callback=callback)

    def open(self):
        return self._connection.open()

    # ------------------------------------------------------------------------
    # Exchange operations
    # ------------------------------------------------------------------------

    def _cleanup_exchanges(self):
        self._exchanges = list(filter(lambda er: er() is not None, self._exchanges))

    def _get_exchanges(self):
        return [er() for er in self._exchanges]

    async def exchange(self, exchange=None, exchange_type='direct',
                       passive=False, durable=False, auto_delete=False, internal=False,
                       arguments=None):

        """
        Creates a new instance of exchange, and declares it.

        WARNING: Make sure you stored this result (e) in your code, otherwise it might be gc'ted,
                 since it has a weak reference.
        """

        e = AMQPExchange(self, exchange, exchange_type, passive, durable, auto_delete, internal, arguments)

        await e.declare()
        self._cleanup_exchanges()
        self._exchanges.append(weakref.ref(e))
        return e

    def exchange_bind(self, destination=None, source=None, routing_key='', arguments=None):
        return Task(self._channel.exchange_bind, destination=destination, source=source,
                    routing_key=routing_key, arguments=arguments)

    def exchange_declare(self, exchange=None, exchange_type='direct',
                         passive=False, durable=False, auto_delete=False, internal=False,
                         arguments=None):
        return Task(self._channel.exchange_declare, exchange=exchange, exchange_type=exchange_type,
                    passive=passive, durable=durable, auto_delete=auto_delete, internal=internal,
                    arguments=arguments)

    def exchange_delete(self, exchange=None, if_unused=False):
        return Task(self._channel.exchange_delete, exchange, if_unused)

    def exchange_unbind(self, destination=None, source=None,
                        routing_key='', arguments=None):
        return Task(self._channel.exchange_unbind, destination=destination, source=source,
                    routing_key=routing_key, arguments=arguments)

    # ------------------------------------------------------------------------
    # Flow operations
    # ------------------------------------------------------------------------

    def flow(self, active):
        return Task(self._connection.flow, active)

    # ------------------------------------------------------------------------
    # Queue operations
    # ------------------------------------------------------------------------

    def _cleanup_queues(self):
        self._queues = list(filter(lambda qr: qr() is not None, self._queues))

    def _get_queues(self):
        return [qr() for qr in self._queues]

    async def queue(self, queue=None, passive=False, durable=False,
                    exclusive=False, auto_delete=False, arguments=None):

        """
        Creates a new instance of queue, and declares it.

        WARNING: Make sure you stored this result (q) in your code, otherwise it might be gc'ted,
                 since it has a weak reference.
        """

        q = AMQPQueue(self, queue, passive, durable, exclusive, auto_delete, arguments)
        await q.declare()
        self._cleanup_queues()
        self._queues.append(weakref.ref(q))
        return q

    def queue_bind(self, queue, exchange, routing_key=None, arguments=None):
        return Task(self._channel.queue_bind, queue=queue, exchange=exchange, routing_key=routing_key,
                    arguments=arguments)

    def queue_declare(self, queue='', passive=False, durable=False,
                      exclusive=False, auto_delete=False, arguments=None):
        return Task(self._channel.queue_declare, queue=queue, passive=passive, durable=durable,
                    exclusive=exclusive, auto_delete=auto_delete, arguments=arguments)

    def queue_delete(self, queue='', if_unused=False, if_empty=False):
        return Task(self._channel.queue_delete, queue=queue, if_unused=if_unused, if_empty=if_empty)

    def queue_purge(self, queue):
        return Task(self._channel.queue_purge, queue=queue)

    def queue_unbind(self, queue='', exchange=None, routing_key=None, arguments=None):
        return Task(self._channel.queue_unbind, queue=queue, exchange=exchange, routing_key=routing_key,
                    arguments=arguments)

    # ------------------------------------------------------------------------
    # Transaction operations
    # ------------------------------------------------------------------------

    def tx_commit(self):
        return Task(self._channel.tx_commit)

    def tx_rollback(self):
        return Task(self._channel.tx_rollback)

    def tx_select(self):
        return Task(self._channel.tx_select)


# noinspection PyProtectedMember,PyPropertyDefinition
class AMQPMessageDestination(AMQPObject):
    """
    A AMQPMessageDestination object is a base class for Queues and Exchanges:
    places you can send messages to.  This base class handles the transmission
    of a message.
    """

    def __init__(self, channel, exchange, routing_key):
        self._channel = channel
        self._exchange = exchange
        self._routing_key = routing_key
        self._declared = False

    _connection = property(lambda s: s._channel._connection)
    _amqp_connection = property(lambda s: s._channel._amqp_connection)

    exchange = property(lambda s: s._exchange)
    routing_key = property(lambda s: s._routing_key)

    async def basic_publish(self, body, properties=None, mandatory=False,
                            immediate=False, exchange=None, routing_key=None):

        if exchange is None:
            exchange = self.exchange
        if routing_key is None:
            routing_key = self.routing_key

        return await self._channel.basic_publish(
            exchange, routing_key, body, properties, mandatory,
            immediate)


class AMQPQueueError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return str(self.code) + ": " + self.message


# noinspection PyProtectedMember
class AMQPQueue(AMQPMessageDestination):
    """
    A representation of a message queue.

    Please use await channel.queue() instead of constructing queue directly.
    In case of reconnection this queue may be redeclared.
    """

    def __init__(self, channel, queue=None, passive=False, durable=False,
                 exclusive=False, auto_delete=False, arguments=None):

        super(AMQPQueue, self).__init__(channel, '', None)
        self._queue = queue
        self._passive = passive
        self._durable = durable
        self._exclusive = exclusive
        self._auto_delete = auto_delete
        self._arguments = arguments

        # Our bindings to instate
        # Format:
        # {
        #   exchange_name: {
        #       routing_key:    {
        #           args:           kwargs for queue_bind,
        #           bound:          boolean indicating bind state,
        #       }
        #   },
        # }
        self._bindings = {}

        # A list of consumers
        self._consumers = []

    # ------------------------------------------------------------------------
    # Queue operations
    # ------------------------------------------------------------------------

    # noinspection PyBroadException
    async def declare(self):
        log = self._get_log('declare')

        future = None

        # Are we declaring passively?
        if self._passive:
            # We are, declare using a temporary channel
            log.debug('Using temporary channel')
            ch = await self._channel._connection.channel()

            def closed(channel, code, reason):
                if code != 0:
                    future.set_exception(AMQPQueueError(code, reason))

            ch.add_on_close_callback(closed)
        else:
            # We're not, use our own channel
            log.debug('Using own channel')
            ch = self._channel

        # Declare the queue in the channel
        try:
            log.debug('Declaring queue')
            queue_res = await ch.queue_declare(
                queue=self._queue or '',
                passive=self._passive,
                durable=self._durable,
                exclusive=self._exclusive,
                auto_delete=self._auto_delete,
                arguments=self._arguments)
        finally:
            # If we opened a temporary channel, close it
            if self._passive and ch.is_open:
                await ch.close()

        self._routing_key = queue_res.method.queue
        log.debug('Queue is %s', self._routing_key)

        # If we were bound to things, re-bind
        for exchange, bindings in self._bindings.copy().items():
            for routing_key, state in bindings.copy().items():
                await self.bind(
                    exchange=exchange,
                    routing_key=routing_key,
                    nowait=state['args']['nowait'],
                    arguments=state['args']['arguments'])

        # If we have any consumers, re-start those
        for c in self._get_consumers():
            if c:
                await c.consume()

    # noinspection PyBroadException
    async def bind(self, exchange, routing_key=None,
                   nowait=False, arguments=None, timeout=None):
        log = self._get_log('bind')

        if isinstance(exchange, AMQPExchange):
            exchange = exchange.exchange

        state = {
            'args': {
                'nowait': nowait,
                'arguments': arguments,
            },
            'bound': False,
        }
        try:
            log.debug('Binding to %s:%s', exchange, routing_key)

            await self._channel.queue_bind(
                queue=self.routing_key,
                exchange=exchange or '',
                routing_key=routing_key,
                nowait=nowait,
                arguments=arguments,
                timeout=timeout)
            # We should be bound now.
            state['bound'] = True
        except:
            log.debug('Failed to bind to %s:%s', exchange, routing_key)

        if exchange not in self._bindings:
            self._bindings[exchange] = {}
        self._bindings[exchange][routing_key] = state
        return state['bound']

    async def delete(self, if_unused=False, if_empty=False, nowait=False,
                     timeout=None):
        log = self._get_log('delete')
        try:
            log.debug('Deleting queue %s', self.routing_key)
            await self._channel.queue_delete(
                queue=self.routing_key,
                if_unused=if_unused,
                if_empty=if_empty,
                nowait=nowait,
                timeout=timeout
            )
            # We're deleted
            self._routing_key = None
        except:
            log.exception('Failed to delete queue %s', self.routing_key)
            raise

    # noinspection PyUnusedLocal
    async def purge(self, queue, nowait=False, timeout=None):
        await self._channel.queue_purge(
            queue=self.routing_key,
            nowait=nowait,
            timeout=timeout)

    async def unbind(self, exchange=None, routing_key=None,
                     arguments=None, timeout=None):

        if isinstance(exchange, AMQPExchange):
            exchange = exchange.exchange

        await self._channel.queue_unbind(
            queue=self.routing_key,
            exchange=exchange,
            routing_key=routing_key,
            arguments=arguments,
            timeout=timeout)
        # If we got here, then we're unbound
        try:
            del (self._bindings[exchange][routing_key])
            if len(self._bindings[exchange]) == 0:
                del (self._bindings[exchange])
        except KeyError:
            pass

    def _cleanup_consumers(self):
        self._consumers = list(filter(lambda cr: cr() is not None, self._consumers))

    def _get_consumers(self):
        return [cr() for cr in self._consumers]

    async def consume(self, consumer_callback, no_ack=False, exclusive=False,
                      consumer_tag=None, arguments=None, channel=None):

        """
        Creates a new instance of consumer, and consumes it.

        WARNING: Make sure you stored this result (c) in your code, otherwise it might be gc'ted,
                 since it has a weak reference.
        """

        c = AMQPConsumer(channel or self._channel, consumer_callback, self,
                         no_ack, exclusive, consumer_tag, arguments)

        await c.consume()
        self._cleanup_consumers()
        self._consumers.append(weakref.ref(c))
        return c


class AMQPExchangeError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return str(self.code) + ": " + self.message


# noinspection PyProtectedMember
class AMQPExchange(AMQPMessageDestination):
    """
    A representation of a message exchange.

    Please use await channel.exchange() instead of constructing exchange directly.
    In case of reconnection this exchanger may be redeclared.

    """

    def __init__(self, channel, exchange=None, exchange_type='direct',
                 passive=False, durable=False, auto_delete=False, internal=False,
                 arguments=None):

        super().__init__(channel, exchange, None)
        self._exchange_type = exchange_type
        self._passive = passive
        self._durable = durable
        self._auto_delete = auto_delete
        self._internal = internal
        self._arguments = arguments
        self._declared = False

        # Our bindings to instate
        # Format:
        # {
        #   exchange_name: {
        #       routing_key:    {
        #           args:           kwargs for queue_bind,
        #           bound:          boolean indicating bind state,
        #       }
        #   },
        # }
        self._bindings = {}

    declared = property(lambda self: self._declared)

    # ------------------------------------------------------------------------
    # Exchange operations
    # ------------------------------------------------------------------------

    # noinspection PyBroadException
    async def declare(self):
        log = self._get_log('declare')

        future = None

        # Are we declaring passively?
        if self._passive:
            # We are, declare using a temporary channel
            log.debug('Using temporary channel')
            ch = await self._channel._connection.channel()

            def closed(channel, code, reason):
                if code != 0:
                    future.set_exception(AMQPExchangeError(code, reason))

            ch.add_on_close_callback(closed)
        else:
            # We're not, use our own channel
            log.debug('Using own channel')
            ch = self._channel

        # Declare the queue in the channel
        try:
            log.debug('Declaring exchange')
            future = ch.exchange_declare(
                exchange=self._exchange,
                exchange_type=self._exchange_type,
                passive=self._passive,
                durable=self._durable,
                auto_delete=self._auto_delete,
                internal=self._internal,
                arguments=self._arguments)

            await future
        finally:
            # If we opened a temporary channel, close it
            if self._passive and ch.is_open:
                await ch.close()

        self._declared = True

        # If we were bound to things, re-bind
        for exchange, bindings in self._bindings.copy().items():
            for routing_key, state in bindings.copy().items():
                await self.bind(
                    exchange=exchange,
                    routing_key=routing_key,
                    arguments=state['args']['arguments'])

    # noinspection PyBroadException
    async def bind(self, exchange, routing_key=None, arguments=None):
        log = self._get_log('bind')

        if isinstance(exchange, AMQPExchange):
            exchange = exchange.exchange

        state = {
            'args': {
                'arguments': arguments,
            },
            'bound': False,
        }
        try:
            log.debug('Binding to %s:%s', exchange, routing_key)

            await self._channel.exchange_bind(
                destination=self.exchange,
                source=exchange,
                routing_key=routing_key or '',
                arguments=arguments)
            # We should be bound now.
            state['bound'] = True
        except:
            log.debug('Failed to bind to %s:%s', exchange, routing_key)

        if exchange not in self._bindings:
            self._bindings[exchange] = {}
        self._bindings[exchange][routing_key] = state
        return state['bound']

    async def delete(self, if_unused=False, nowait=False, timeout=None):
        log = self._get_log('delete')
        try:
            log.debug('Deleting exchange %s', self.exchange)
            await self._channel.exchange_delete(
                exchange=self.exchange,
                if_unused=if_unused,
                nowait=nowait,
                timeout=timeout
            )
            self._declared = False
        except:
            log.exception('Failed to delete exchange %s', self.exchange)
            raise

    async def unbind(self, exchange=None, routing_key=None,
                     arguments=None, timeout=None):

        if isinstance(exchange, AMQPExchange):
            exchange = exchange.exchange

        await self._channel.exchange_unbind(
            destination=self.exchange,
            source=exchange,
            routing_key=routing_key,
            arguments=arguments,
            timeout=timeout)
        # If we got here, then we're unbound
        try:
            del (self._bindings[exchange][routing_key])
            if len(self._bindings[exchange]) == 0:
                del (self._bindings[exchange])
        except KeyError:
            pass


# noinspection PyProtectedMember,PyPropertyDefinition
class AMQPConsumer(AMQPObject):
    """
    An abstraction for a consumer.

    Please use await queue.consume() instead of constructing consumer directly.
    In case of reconnection this consumer may be reconsumed.
    """

    def __init__(self, channel, consumer_callback, queue=None, no_ack=False,
                 exclusive=False, consumer_tag=None, arguments=None):

        self._channel = channel
        self._consumer_callback = consumer_callback
        self._queue = queue
        self._no_ack = no_ack
        self._exclusive = exclusive
        self._consumer_tag_given = consumer_tag
        self._arguments = arguments
        self._consumer_tag = None

    consumer_tag = property(lambda s: s._consumer_tag)

    # noinspection PyUnusedLocal
    async def consume(self):
        log = self._get_log('_consume')
        try:
            log.debug('Consuming queue %s', self._queue.routing_key)
            response = await self._channel.basic_consume(
                consumer_callback=self._consumer_callback,
                queue=self._queue.routing_key,
                no_ack=self._no_ack,
                exclusive=self._exclusive,
                consumer_tag=self._consumer_tag_given,
                arguments=self._arguments)
        except Exception:
            log.exception('Failed to consume %s', self._queue.routing_key)
            self._consumer_tag = None
            raise
        else:
            self._consumer_tag = response.method.consumer_tag
            log.debug('Consumer tag is %s', self._consumer_tag)

    # noinspection PyUnusedLocal
    async def cancel(self, nowait=False, timeout=None):
        log = self._get_log('cancel')

        log.debug('Cancelling consumer of queue %s', self._queue.routing_key)
        await Task(self._channel.basic_cancel, consumer_tag=self._consumer_tag_given, nowait=nowait)

        self._consumer_tag = None
        log.debug('Consumer is cancelled')


class TimeoutError(Exception):
    """
    Exception raised when an operation times out.
    """
    pass
