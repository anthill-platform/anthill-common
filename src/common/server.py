import json
import logging

import tornado.httpclient
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.netutil

from tornado.gen import coroutine, Return
from options import options
from pympler import tracker

import tornado.log

import internal
import access
import discover
import admin
import pubsub
import jsonrpc
import monitoring
import handler
import traceback
import time
import signal
import threading
import validate

# just included to define things
import options.default as opts_

from . import retry, ElapsedTime

MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = 5

tornado.netutil.Resolver.configure('tornado.netutil.ThreadedResolver')


class ServerError(RuntimeError):
    def __init__(self, message, *args, **kwargs):
        super(ServerError, self).__init__(*args, **kwargs)
        self.message = message

    def __str__(self):
        return self.message


class Server(tornado.web.Application):
    _instance = None

    def __init__(self):

        Server._instance = self

        self.has_started = False
        self.started_callback = None
        self.http_server = None
        self.api_version = options.api_version

        handlers = self.get_handlers() or []

        admin_actions = self.get_admin()
        if admin_actions:
            self.actions = admin.AdminActions(admin_actions)
            handlers.extend([
                (r"/@admin", admin.AdminHandler),
                (r"/@admin_upload", admin.AdminUploadHandler)
            ])
        else:
            self.actions = None

        stream_admin_actions = self.get_admin_stream()
        if stream_admin_actions:
            self.stream_actions = admin.AdminActions(stream_admin_actions)
            handlers.append((r"/@stream_admin", admin.AdminWSHandler))
        else:
            self.stream_actions = None

        self.internal_handler = self.get_internal_handler()

        metadata = self.get_metadata()

        if isinstance(metadata, dict):
            self.metadata = self.__load_metadata__(metadata)
            logging.info("Metadata loaded")
            handlers.append((r"/@metadata", admin.AdminMetadataHandler))

        auth_callback = self.get_auth_callback()
        if auth_callback:
            handlers.append((r"/auth_callback", auth_callback))

        if options.serve_static:
            handlers.append((r'/static/(.*)', tornado.web.StaticFileHandler,
                             {'path': 'static', "default_filename": "index.html"}))

        handlers.append(('/', self.get_root_handler()))

        self.debug_mode = options.debug

        if self.debug_mode:
            self.memory_tracker = tracker.SummaryTracker()
            handlers.append(('/@memory_diff', handler.DebugMemoryDiffHandler))
        else:
            self.memory_tracker = None

        super(Server, self).__init__(
            handlers=handlers, debug=options.debug
        )

        if options.enable_monitoring:
            self.monitoring = monitoring.InfluxDBMonitoring(
                host=options.monitoring_host,
                port=options.monitoring_port,
                db=options.monitoring_db,
                username=options.monitoring_username,
                password=options.monitoring_password)
        else:
            self.monitoring = None

        if self.token_cache_enabled():
            self.token_cache = access.AccessTokenCache()
        else:
            self.token_cache = None

        self.name = None

        self.internal = None
        self.shutting_down = False

        # pub/sub
        self.subscriber = None
        self.publisher = None

        tornado.ioloop.IOLoop.current().set_blocking_log_threshold(0.5)

    @classmethod
    def instance(cls):
        return cls._instance

    def set_started_callback(self, callback):
        if self.has_started:
            callback()
        else:
            self.started_callback = callback

    def monitor_action(self, action_name, values, **tags):
        """
        Called when some action that should be monitored happens
        :param action_name: Is a name of the action. Domain-like names are appreciated, for example,
            action.sub_action. Please note then anthill.{service-name}. will be prepended to this.
        :param values: A dict of string => float of values the action carries
        :param tags: Useful tags for aggregation
        :return:
        """
        if self.monitoring is not None:
            self.monitoring.add_action("anthill." + self.name + "." + action_name, values, **tags)

    def monitor_rate(self, action_name, name_property, **tags):
        """
        Called when some "rate" action (for example, registrations per minute) should be increased.
        :param action_name: Domain-like name of the action. For example, "web", or "web.errors".
            Please note then anthill.{service-name}. will be prepended to this.
        :param name_property: A property of the action who's rate should go up
        :param tags: Useful tags for aggregation
        :return:
        """
        if self.monitoring is not None:
            self.monitoring.add_rate("anthill." + self.name + "." + action_name, name_property, **tags)

    def log_request(self, handler):
        super(Server, self).log_request(handler)

        if self.monitoring is not None:
            if handler.get_status() < 400:
                self.monitor_rate("web", "request", api=self.api_version)
            elif handler.get_status() < 500:
                self.monitor_rate("web", "request.4xx",
                                  api=self.api_version, code=handler.get_status())
            else:
                self.monitor_rate("web", "request.5xx",
                                  api=self.api_version, code=handler.get_status())

    def __load_metadata__(self, data):
        data["version"] = self.api_version
        return data

    @coroutine
    def get_auth_location(self, network):
        result = yield discover.cache.get_service("login", network)
        raise Return(result)

    def get_handlers(self):
        """
        This method is need to be overridden to return a list of tuples: one tuple for each request the
        service can process.

        For example,

        [
            ("/user", handlers.UserHandler)
        ]

        This binds a /user request to a UserHandler (overridden from AnthillRequestHandler)

        """
        return []

    def get_auth_callback(self):
        return handler.AuthCallbackHandler

    def get_admin(self):
        """
        This method is need to be overridden to return a dict of classes: AdminController's
        This allows to administrate each service in admin tool.
        Each AdminController corresponds one single action can be done in such tool.
        Please see `AdminController` for more information.
        """
        return {}

    def get_admin_stream(self):
        return {}

    def get_root_handler(self):
        return handler.RootHandler

    def get_internal_handler(self):
        """
        Object, returned by this method is used handle request from other services across environment.

        To do so, they may do yield internal.request("<service_id>", "<command_name>", <arguments>)
        In that case, a @coroutine method <command_name> would be called on that object with <arguments>

        for example, for such internal handler:

        class InternalHandler():
            @coroutine
            def hello(name):
                ... do some work ...
                raise Return({"hello, your name is": name})

        a call would be:

        result = yield internal.request("<service_id>", "hello", name="john")

        a result would be:

         {"hello, your name is": "john"}

        """

        return None

    def get_metadata(self):
        """
        Returns a location of metadata file.
        This file is used in admin tool to acquire description of the service.
        """
        return None

    def init_discovery(self):
        discover.init()

    # noinspection PyMethodMayBeStatic
    def get_models(self):
        """
        Returns a list of models in the application
        """
        return []

    def token_cache_enabled(self):
        """
        Is token cache enabled on this server or no (any use of access tokens will be disabled otherwise)
        :return:
        """
        return True

    @coroutine
    def acquire_subscriber(self):
        if self.subscriber is not None:
            raise Return(self.subscriber)

        self.subscriber = pubsub.RabbitMQSubscriber(
            name=options.name,
            broker=options.pubsub,
            channel_prefetch_count=options.internal_channel_prefetch_count)

        yield self.subscriber.start()
        raise Return(self.subscriber)

    @coroutine
    def acquire_publisher(self):
        if self.publisher is not None:
            raise Return(self.publisher)

        self.publisher = pubsub.RabbitMQPublisher(
            broker=options.pubsub,
            name=options.name,
            channel_prefetch_count=options.internal_channel_prefetch_count)

        yield self.publisher.start()
        raise Return(self.publisher)

    @coroutine
    def started(self):
        self.name = options.name

        if self.token_cache_enabled():
            yield self.token_cache.load(self)

        self.init_discovery()
        self.internal = internal.Internal()

        if self.internal_handler:
            yield self.internal.listen(self.name, self.__on_internal_receive__)

        need_account_delete_event = False

        for model in self.get_models():
            if hasattr(model, "started"):
                yield model.started(self)
            if model.has_delete_account_event():
                need_account_delete_event = True

        if need_account_delete_event:
            subscriber = yield self.acquire_subscriber()
            yield subscriber.handle("DEL", self.__account_deleted_callback__)

        if self.subscriber:
            yield self.subscriber.start()

        logging.info("Service '%s' started.", self.name)

        self.has_started = True
        if self.started_callback:
            self.started_callback()
            self.started_callback = None

    @coroutine
    def __account_deleted_callback__(self, data):
        try:
            accounts = validate.validate_value(data["accounts"], "json_list_of_ints")
            gamespace_id = data["gamespace"]
            gamespace_only = data["gamespace_only"]
        except KeyError:
            return
        except validate.ValidationError:
            return

        if not accounts:
            return

        for model in self.get_models():
            if model.has_delete_account_event():
                # noinspection PyBroadException
                try:
                    yield model.accounts_deleted(gamespace_id, accounts, gamespace_only)
                except Exception:
                    logging.exception("Failed to notify 'accounts_deleted' on '{0}'".format(model.__class__.__name__))
                else:
                    logging.info("Deleted {0} accounts on '{1}'".format(len(accounts), model.__class__.__name__))

    def listen_server(self):
        self.http_server = tornado.httpserver.HTTPServer(self, xheaders=True)

        listen_uri = options.listen
        listen_group = listen_uri.split(":")

        if len(listen_group) < 2:
            raise ServerError("Failed to listen on " + listen_uri + ": bad format")

        kind, addresses = listen_group[0], listen_group[1:]

        def listen_port(ports):
            for port in ports:
                self.http_server.listen(int(port), "127.0.0.1")

        def listen_unix(sockets):
            for sock in sockets:
                logging.info("Listening for socket: " + sock)
                unix_socket = tornado.netutil.bind_unix_socket(sock, mode=0o777)
                self.http_server.add_socket(unix_socket)

        kinds = {
            "port": listen_port,
            "unix": listen_unix
        }

        if kind not in kinds:
            raise ServerError("Failed to listen on " + listen_uri + ": unsupported kind")

        listen_method = kinds[kind]
        listen_method(addresses)

        logging.info("Listening '{0}' on '{1}'".format(kind, addresses))

    def run(self):
        # noinspection PyUnresolvedReferences,PyProtectedMember
        if isinstance(threading.current_thread(), threading._MainThread):
            signal.signal(signal.SIGPIPE, Server.__sigpipe_handler__)
            signal.signal(signal.SIGTERM, self.__sig_handler__)
            signal.signal(signal.SIGINT, self.__sig_handler__)

        self.listen_server()

        logging.info("API version is '{0}'".format(self.api_version))
        logging.info("Host is '{0}'".format(self.get_host()))

        tornado.ioloop.IOLoop.instance().add_callback(self.started)
        tornado.ioloop.IOLoop.instance().start()

    # noinspection PyMethodMayBeStatic
    def get_host(self):
        return options.host

    @coroutine
    def __on_internal_receive__(self, context, method, *args, **kwargs):
        if hasattr(self.internal_handler, method):

            if not isinstance(method, (str, unicode)):
                raise jsonrpc.JsonRPCError(-32600, "Method is not a string")

            if method.startswith("_"):
                raise jsonrpc.JsonRPCError(-32600, "No such method")

            timer = ElapsedTime("incoming request: {0}".format(method))

            # noinspection PyBroadException
            try:
                result = yield getattr(self.internal_handler, method)(*args, **kwargs)
            except internal.InternalError as e:
                raise jsonrpc.JsonRPCError(e.code, e.body, "code: " + str(e.code))
            except Exception:
                raise jsonrpc.JsonRPCError(-32603, traceback.format_exc())
            else:
                raise Return(result)
            finally:
                timer.done()

        else:
            raise jsonrpc.JsonRPCError(-32600, "No such method")

    def __sig_handler__(self, sig, frame):
        if self.shutting_down:
            return

        logging.warning('Caught signal: %s', sig)
        tornado.ioloop.IOLoop.instance().add_callback(self.shutdown)

    @staticmethod
    def __sigpipe_handler__(sig, frame):
        logging.warning('Caught SIGPIPE')

    @coroutine
    def process_shutdown(self):
        yield self.internal.stop()

        if self.subscriber:
            yield self.subscriber.release()

        for model in self.get_models():
            if hasattr(model, "stopped"):
                yield model.stopped()

        if self.publisher:
            yield self.publisher.release()

    def shutdown(self):
        self.shutting_down = True

        logging.info('Stopping server!')
        if self.http_server:
            self.http_server.stop()

        io_loop = tornado.ioloop.IOLoop.instance()

        def shutdown_callback(f):
            io_loop.stop()
            logging.info('Stopped!')

        io_loop.add_future(self.process_shutdown(), shutdown_callback)


def init():
    import options

    options.parse_command_line()
    options.parse_env()

    return options


def start(server_cls):
    application = server_cls()
    application.run()
