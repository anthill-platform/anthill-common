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

import tornado.log

import internal
import access
import discover
import admin
import jsonrpc
import handler
import traceback
import time
import signal

# just included to define things
import options.default as opts_

from . import retry, ElapsedTime

MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = 5

SERVICE_VERSION = "0.1"
tornado.netutil.Resolver.configure('tornado.netutil.ThreadedResolver')


class ServerError(RuntimeError):
    def __init__(self, message, *args, **kwargs):
        super(ServerError, self).__init__(*args, **kwargs)
        self.message = message

    def __str__(self):
        return self.message


class Server(tornado.web.Application):
    def __init__(self):

        self.http_server = None

        self.api_version = SERVICE_VERSION

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
            handlers.append((r"/callback", auth_callback))

        if options.serve_static:
            handlers.append((r'/static/(.*)', tornado.web.StaticFileHandler,
                             {'path': 'static', "default_filename": "index.html"}))

        super(Server, self).__init__(
            handlers=handlers, debug=options.debug
        )

        self.token_cache = access.AccessTokenCache()
        self.name = None

        self.internal = None
        self.shutting_down = False
        self.graceful_shutdown = options.graceful_shutdown

        tornado.ioloop.IOLoop.instance().set_blocking_log_threshold(0.5)

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

        This binds a /user request to a UserHandler (overridden from RequestHandler)

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

    @coroutine
    def get_gamespace(self, gamespace_name):
        """
        :returns a gamespace ID from given name.
        """
        internal_ = internal.Internal()

        @retry(operation="Acquiring gamespace", max=5, delay=5)
        @coroutine
        def do_try():
            response = yield internal_.request(
                "login",
                "get_gamespace",
                name=gamespace_name)
            raise Return(str(response["id"]))

        raise Return((yield do_try()))

    @coroutine
    def get_gamespace_list(self):
        """
        :returns: a list of available gamespaces across the environment.
        """
        internal_ = internal.Internal()

        @retry(operation="Acquiring gamespace list", max=5, delay=5)
        @coroutine
        def do_try():
            response = yield internal_.request("login", "get_gamespaces")
            raise Return(response)

        raise Return((yield do_try()))

    def init_discovery(self):
        discover.init()

    # noinspection PyMethodMayBeStatic
    def get_models(self):
        """
        Returns a list of models in the application
        """
        return []

    @coroutine
    def started(self):
        self.name = options.name

        self.token_cache.load()
        self.init_discovery()
        self.internal = internal.Internal()

        if self.internal_handler:
            yield self.internal.listen(self.name, self.__on_internal_receive__)

        for model in self.get_models():
            if hasattr(model, "started"):
                yield model.started()

        logging.info("Service '%s' started.", self.name)

    def run(self):
        signal.signal(signal.SIGTERM, self.__sig_handler__)
        signal.signal(signal.SIGINT, self.__sig_handler__)

        self.http_server = tornado.httpserver.HTTPServer(self, xheaders=True)

        listen_uri = options.listen
        listen_group = listen_uri.split(":")

        if len(listen_group) < 2:
            raise ServerError("Failed to listen on " + listen_uri + ": bad format")

        kind, address = listen_group[0], listen_group[1]

        def listen_port(port):
            self.http_server.listen(int(port), "127.0.0.1")

        def listen_unix(sockets):
            for sock in sockets.split(":"):
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
        listen_method(address)

        logging.info("Listening '{0}' on '{1}'".format(kind, address))
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

    def shutdown(self):
        self.shutting_down = True

        logging.info('Stopping server!')
        self.http_server.stop()

        if self.graceful_shutdown:

            for model in self.get_models():
                if hasattr(model, "stopped"):
                    tornado.ioloop.IOLoop.instance().add_callback(model.stopped)

            logging.info('Will shutdown in %s seconds ...', MAX_WAIT_SECONDS_BEFORE_SHUTDOWN)
            io_loop = tornado.ioloop.IOLoop.instance()

            deadline = time.time() + MAX_WAIT_SECONDS_BEFORE_SHUTDOWN

            # noinspection PyProtectedMember
            def stop_loop():
                now = time.time()
                if now < deadline and (io_loop._callbacks or io_loop._timeouts):
                    io_loop.add_timeout(now + 1, stop_loop)
                else:
                    io_loop.stop()
                    logging.info('Stopped!')
            stop_loop()

        else:
            tornado.ioloop.IOLoop.instance().stop()
            logging.info('Stopped!')


def init():
    import options

    options.parse_command_line()
    options.parse_env()

    return options


def start(server_cls):
    application = server_cls()
    application.run()
