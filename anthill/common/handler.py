
import tornado.escape
import tornado.httpclient
import tornado.websocket
import tornado.ioloop

from tornado.web import HTTPError, RequestHandler
from tornado.websocket import WebSocketClosedError, StreamClosedError
from .validate import ValidationError
from asyncio import iscoroutine

from . import access
from . import internal
from . import jsonrpc
from . import ujson

from urllib import parse
import base64
import os
import psutil
import logging
import gc


class JsonHandlerMixin(object):
    # noinspection PyUnresolvedReferences
    def dumps(self, data):
        self.set_header("Content-Type", "application/json")
        self.write(ujson.dumps(data, escape_forward_slashes=False))


class AnthillRequestHandler(RequestHandler):
    def set_default_headers(self):
        self.set_header("API-Version", self.application.api_version)
        self.set_header("Access-Control-Allow-Origin", "*")


class JsonHandler(JsonHandlerMixin, AnthillRequestHandler):
    pass


class AuthCallbackHandler(AnthillRequestHandler):
    def access_required(self):
        return []

    def authorize_error(self, error):
        pass

    async def authorize_success(self, token):
        pass

    def data_received(self, chunk):
        pass

    async def get(self):
        await self.process()

    async def post(self):
        await self.process()

    async def process(self):
        error = self.get_argument("error", None)

        if error:
            error_text = base64.b64decode(error)
            try:
                error_obj = ujson.loads(error_text)
            except ValueError:
                error_obj = {
                    "result_id": "Internal server error: " + error_text
                }

            self.authorize_error(error_obj)
            return

        after = self.get_argument("after", "/")

        token_string = self.get_argument("token")
        access_token = base64.b64decode(token_string)

        token = access.AccessToken(access_token)

        if token.is_valid():
            if token.has_scopes(self.access_required()):
                self.set_cookie("access_token", access_token)
                await self.authorize_success(token)
                self.redirect(after)

            else:
                self.authorize_error({
                    "result_id": "forbidden"
                })
        else:
            self.authorize_error({
                "result_id": "forbidden"
            })


# noinspection PyUnresolvedReferences
class AuthenticatedHandlerMixin(object):
    def __init__(self, application):
        self.token = None
        self.application = application

    def external_auth_location(self):
        raise NotImplementedError()

    def access_restricted(self, scopes=None, ask_also=None):

        user = self.current_user

        needed_scopes = list(user.token.scopes) if (user is not None) else []
        if scopes is not None:
            needed_scopes.extend(scopes)

        if ask_also is not None:
            should_have = ",".join(needed_scopes)
            needed_scopes.extend(ask_also)
        else:
            should_have = None

        auth_location = self.external_auth_location()

        if auth_location is None:
            raise HTTPError(500, "Authorisation service unknown")

        args = {
            "scopes": ",".join(set(needed_scopes)),
            "gamespace": self.get_gamespace(),
            "redirect": self.application.get_host() + "/auth_callback?" + parse.urlencode({
                "after": self.request.uri
            }),
            "as": (self.authorize_as() or "")
        }

        if user is not None:
            args["access_token"] = user.token.key

        if should_have:
            args["should_have"] = should_have

        self.redirect(auth_location + "/authform?" + parse.urlencode(args))

    def authorize_as(self):
        return ""

    def data_received(self, chunk):
        pass

    def get_current_user(self):
        if self.token is None:
            return None

        return AuthorizedUser(self.token)

    def get_gamespace(self):
        raise NotImplementedError()

    def logout(self):
        self.clear_cookie("access_token")

    def has_scopes(self, scopes):
        """
        Check if the user has access to the specified scopes
        """
        current_user = self.current_user
        return (current_user is not None) and (current_user.token.has_scopes(scopes))

    async def __token_needs_refresh__(self, token, db):
        internal_ = internal.Internal()

        token_cache = self.application.token_cache

        if not token_cache:
            return

        try:
            response = await internal_.request(
                token.get(access.AccessToken.ISSUER, "login"),
                "refresh_token",
                access_token=token.key)

        except internal.InternalError as e:
            logging.error(
                "Failed to refresh an access token for user '{0}': {1} {2}".format(
                    token.name,
                    e.code,
                    e.body))
        else:

            token = access.AccessToken(response["access_token"])

            if token.is_valid():

                if db is None:
                    db = token_cache.acquire()

                await token_cache.store_token(db, token)

                self.token_refreshed(token)

                logging.info(
                    "Refreshed an access token for user '{0}'".format(
                        token.name))
            else:
                logging.error(
                    "Refreshed token we've just got is not valid: {0}".format(
                        token.key))

    async def prepare(self):

        token_cache = self.application.token_cache
        if token_cache:

            token = AuthenticatedHandlerMixin.validate(
                self.get_argument("access_token", None))

            if token is None:
                token = AuthenticatedHandlerMixin.validate(
                    self.get_cookie("access_token", None))

            if token:
                async with token_cache.acquire() as db:
                    valid = await token_cache.validate_db(token, db=db)

                    if valid:
                        self.token = token
                    else:
                        self.token_invalidated(token)
                        token = None

                    if token:
                        time_left = token.time_left()

                        self.set_header("Access-Token-Time-Left", str(time_left))

                        if token.needs_refresh():
                            await self.__token_needs_refresh__(token, db)

        result = self.prepared(*self.path_args, **self.path_kwargs)
        if iscoroutine(result):
            await result

    async def prepared(self, *args, **kwargs):
        """
        Called when handler is completely prepared for processing
        :param args: a list of path arguments as it would go to 'get' or corresponding request method
        :param kwargs: a dict of path arguments as it would go to 'get' or corresponding request method
        """
        pass

    def token_invalidated(self, token):
        pass

    def token_refreshed(self, token):
        self.set_header("Access-Token", token.key)

    @staticmethod
    def validate(token):
        if token is None:
            return None

        token = access.AccessToken(token)

        if token.is_valid():
            return token

        return None


class AuthenticatedHandler(JsonHandlerMixin, AuthenticatedHandlerMixin, AnthillRequestHandler):
    """
    A handler that deals with access tokens internally. Parses and validates access_token field,
    if passed, and makes possible to reference token object by self.token
    """
    def __init__(self, application, request, **kwargs):
        AnthillRequestHandler.__init__(
            self,
            application,
            request,
            **kwargs)

        AuthenticatedHandlerMixin.__init__(self, application)


class AuthorizedUser:
    def __init__(self, token):
        self.token = token
        self.profile = None


class AuthenticatedWSHandler(JsonHandlerMixin, AuthenticatedHandlerMixin,
                             tornado.websocket.WebSocketHandler):
    """
    A handler like the one above, but used for the web sockets
    """
    def __init__(self, application, request, **kwargs):
        tornado.websocket.WebSocketHandler.__init__(
            self,
            application,
            request,
            **kwargs)

        AuthenticatedHandlerMixin.__init__(self, application)

        self._pingcb = None

    # noinspection PyMethodMayBeStatic
    async def prepared(self, *args, **kwargs):
        user = self.current_user
        scopes = self.required_scopes()

        if scopes and ((user is None) or (not user.token.has_scopes(scopes))):
            raise HTTPError(
                403,
                "Access denied ('{0}' required)".format(
                    ", ".join(scopes or []))
                if scopes else "Access denied")

    def __do_ping__(self):
        if self.ws_connection:
            self.ping("")

    def open(self, *args, **kwargs):
        tornado.ioloop.IOLoop.current().add_callback(self.__process_opened__, *args, **kwargs)
        if self.enable_ping():
            self._pingcb = tornado.ioloop.PeriodicCallback(self.__do_ping__, 10000)
            self._pingcb.start()

    def on_close(self):
        if self._pingcb:
            self._pingcb.stop()
        tornado.ioloop.IOLoop.current().add_callback(self.on_closed)

    async def on_opened(self, *args, **kwargs):
        pass

    async def on_closed(self):
        pass

    async def __process_opened__(self, *args, **kwargs):
        try:
            await self.on_opened(*args, **kwargs)
        except ValidationError as e:
            self.close(400, e.message)
        except HTTPError as e:
            self.close(e.status_code, e.reason)
        except BaseException as e:
            self.close(500, str(e))

    def enable_ping(self):
        return True

    def required_scopes(self):
        """
        Should return a list of scopes the user should have. Otherwise, 403 Forbidden is returned.
        Empty list means no restriction is required.
        """
        return []


class JsonRPCWSHandler(AuthenticatedWSHandler, jsonrpc.JsonRPC):

    """
    Authenticated web socket handler, but with JSONRPC protocol.
    Allows to setup JSONRPC communication with client. Please see http://www.jsonrpc.org/specification for detail.

    To send an rpc command (without expecting a response), call await self.rpc(self, 'method', .. arguments ..)
    To send a request command (with response), call await self.request(self, 'method', .. arguments ..),
        the result of such instruction is a response from a client, or JsonRPCTimeout exception.

    To receive a command, just define appropriate method in a subclass:

    async def hello(name):
        return "Hello, your name is " + name

    To deny a method from calling, start if with underscore.

    """

    def __init__(self, application, request, **kwargs):
        AuthenticatedWSHandler.__init__(self, application, request, **kwargs)
        jsonrpc.JsonRPC.__init__(self)

        self.set_receive(self.command_received)

    async def command_received(self, context, action, *args, **kwargs):
        if hasattr(self, action):
            if action.startswith("_"):
                raise jsonrpc.JsonRPCError(400, "No such action!")

            try:
                response = await getattr(self, action)(*args, **kwargs)
            except TypeError as e:
                logging.exception("JsonRPCWSHandler TypeError")
                raise jsonrpc.JsonRPCError(400, "Bad arguments: " + e.args[0])
            except jsonrpc.JsonRPCError:
                raise
            except ValidationError as e:
                raise jsonrpc.JsonRPCError(400, e.message)
            except Exception as e:
                logging.exception("JsonRPCWSHandler exception")
                raise jsonrpc.JsonRPCError(500, str(e.__class__.__name__) + ": " + str(e))

            return response
        else:
            raise jsonrpc.JsonRPCError(400, "No such action!")

    async def on_message(self, message):
        try:
            await self.received(self, message)
        except jsonrpc.JsonRPCError as e:
            pass

    # noinspection PyMethodOverriding
    async def write_data(self, context, data):

        if not self.stream or self.stream.closed():
            return

        try:
            f = self.write_message(data)

            if not f:
                return

            await f
        except StreamClosedError:
            raise jsonrpc.JsonRPCError(599, "WebSockets is closed")
        except WebSocketClosedError:
            raise jsonrpc.JsonRPCError(599, "WebSockets closed")


class CookieAuthenticatedHandler(AuthenticatedHandler):
    def __init__(self, application, request, **kwargs):
        super(CookieAuthenticatedHandler, self).__init__(
            application,
            request,
            **kwargs)

    def token_invalidated(self, token):
        self.clear_cookie("access_token")

    def token_refreshed(self, token):
        self.set_cookie("access_token", token.key)


class CookieAuthenticatedWSHandler(AuthenticatedWSHandler):
    def __init__(self, application, request, **kwargs):
        super(CookieAuthenticatedWSHandler, self).__init__(
            application,
            request,
            **kwargs)

    def token_invalidated(self, token):
        self.clear_cookie("access_token")

    def token_refreshed(self, token):
        self.set_cookie("access_token", token.key)


class LogoutHandler(AuthenticatedHandler):
    def authorize_as(self):
        return "admin"

    def data_received(self, chunk):
        pass

    def get(self):
        self.logout()
        self.redirect("/")


class DebugMemoryDiffHandler(AuthenticatedHandler):
    @access.internal
    def get(self):

        self.set_header("Content-Type", "text/html")

        process = psutil.Process(os.getpid())
        self.write("Used memory: " + str(process.memory_info().rss) + "\n\n")

        gc.collect()
        
        diff = self.application.memory_tracker.format_diff()

        for fmt in diff:
            self.write(fmt + "\n")


class RootHandler(AnthillRequestHandler, JsonHandlerMixin):
    def get(self):
        if self.application.debug_mode:
            self.set_header("X-Service-Name", self.application.name)
            self.set_header("X-Service-Host", self.application.get_host())

            if hasattr(self.application, "metadata"):
                self.dumps(self.application.metadata)

            return

        super(RootHandler, self).get()
