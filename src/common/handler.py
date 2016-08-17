import base64
import json
import logging
import urllib

import tornado.escape
import tornado.httpclient
import tornado.websocket

from tornado.gen import coroutine, Return, is_future
from tornado.web import HTTPError, RequestHandler

import access
import internal


class AuthCallbackHandler(RequestHandler):
    def access_required(self):
        return []

    def authorize_error(self, error):
        pass

    @coroutine
    def authorize_success(self, token):
        pass

    def data_received(self, chunk):
        pass

    @coroutine
    def get(self):
        yield self.process()

    @coroutine
    def post(self):
        yield self.process()

    @coroutine
    def process(self):
        error = self.get_argument("error", None)

        if error:
            error_text = base64.b64decode(error)
            try:
                error_obj = json.loads(error_text)
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
                yield self.authorize_success(token)
                self.redirect(after)

            else:
                self.authorize_error({
                    "result_id": "forbidden"
                })
        else:
            self.authorize_error({
                "result_id": "forbidden"
            })


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
            "redirect": self.application.get_host() + "/authcallback?" + urllib.urlencode({
                "after": self.request.uri
            }),
            "as": (self.authorize_as() or "")
        }

        if user is not None:
            args["access_token"] = user.token.key

        if should_have:
            args["should_have"] = should_have

        self.redirect(auth_location + "/authform?" + urllib.urlencode(args))

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

    @coroutine
    def prepare(self):
        token = AuthenticatedHandlerMixin.validate(
            self.get_argument("access_token", None))

        if token is None:
            token = AuthenticatedHandlerMixin.validate(
                self.get_cookie("access_token", None))

        token_cache = self.application.token_cache
        db = None

        try:
            if token:
                db = token_cache.acquire()

                valid = yield token_cache.validate_db(token, db=db)

                if valid:
                    self.token = token
                else:
                    self.token_invalidated(token)
                    token = None

            if token and token.needs_refresh():
                internal_ = internal.Internal()

                try:
                    response = yield internal_.request(
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

                        token_cache = self.application.token_cache
                        if db is None:
                            db = token_cache.acquire()

                        yield token_cache.store_token(db, token)

                        self.token_refreshed(token)

                        logging.info(
                            "Refreshed an access token for user '{0}'".format(
                                token.name))
                    else:
                        logging.error(
                            "Refreshed token we've just got is not valid: {0}".format(
                                token.key))

        finally:
            if db is not None:
                yield db.release()

        result = self.prepared()
        if is_future(result):
            yield result

    @coroutine
    def prepared(self):
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


class AuthenticatedHandler(AuthenticatedHandlerMixin, RequestHandler):
    """
    A handler that deals with access tokens internally. Parses and validates access_token field,
    if passed, and makes possible to reference token object by self.token
    """
    def __init__(self, application, request, **kwargs):
        RequestHandler.__init__(
            self,
            application,
            request,
            **kwargs)

        AuthenticatedHandlerMixin.__init__(self, application)


class AuthorizedUser:
    def __init__(self, token):
        self.token = token
        self.profile = None


class AuthenticatedWSHandler(AuthenticatedHandlerMixin, tornado.websocket.WebSocketHandler):
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

    # noinspection PyMethodMayBeStatic
    @coroutine
    def prepared(self):
        user = self.current_user
        scopes = self.required_scopes()

        if scopes and ((user is None) or (not user.token.has_scopes(scopes))):
            raise HTTPError(
                403,
                "Access denied ('{0}' required)".format(
                    ", ".join(scopes or []))
                if scopes else "Access denied")

    def required_scopes(self):
        """
        Should return a list of scopes the user should have. Otherwise, 403 Forbidden is returned.
        Empty list means no restriction is required.
        """
        return []


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
