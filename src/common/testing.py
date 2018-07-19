from tornado.gen import coroutine, Return, sleep
from tornado.testing import AsyncTestCase, bind_unused_port, get_async_test_timeout
from tornado.httpserver import HTTPServer
from tornado.httpclient import AsyncHTTPClient, HTTPError, HTTPRequest
from tornado.websocket import websocket_connect
from tornado.ioloop import IOLoop

import sign
import ujson
import unittest

from admin import Redirect
from access import AccessToken
from database import Database, DatabaseError
from environment import EnvironmentClient, ApplicationInfoAdapter
from login import LoginClient, GamespaceAdapter
from jsonrpc import JsonRPC
from gen import AccessTokenGenerator
from options import options

import urllib

TEST_DATABASE_NAME = "test"


class TestError(Exception):
    pass


class OptionsLoader(object):
    loaded = False

    @staticmethod
    def load():
        if OptionsLoader.loaded:
            return

        from server import init
        init()

        OptionsLoader.loaded = True


class ServerTestCase(AsyncTestCase):
    """
    Use this test case for testing models of a service without actually testing external REST api.

    Override get_server_instance to return common.Server instance, any test will be performed on that instance,
    available on self.application

    If you need to test external REST api, use AcceptanceTestCase instead
    """

    @classmethod
    def need_test_db(cls):
        """
        Return True if you need a test database (will be available as self.test_db)
        """
        return False

    @classmethod
    def get_server_instance(cls, db=None):
        """
        Override this method to return common.Server instance
        :param db: if need_test_db() returns True, this argument will contain a Database reference
        """
        raise NotImplementedError()

    @classmethod
    @coroutine
    def co_setup_server_tests(cls):
        """
        Coroutine-friendly analog of the setUpClass method, override this one to setup things before any test
        """
        pass

    def get_new_ioloop(self):
        return IOLoop.current()

    def tearDown(self):
        pass

    @classmethod
    @coroutine
    def co_setup_class(cls):
        if cls.need_test_db():
            cls.test_db = yield ServerTestCase.get_test_db()
        else:
            cls.test_db = None

        OptionsLoader.load()

        cls.application = cls.get_server_instance(cls.test_db)

        IOLoop.current().set_blocking_log_threshold(0)

        yield cls.application.started()

    @classmethod
    @coroutine
    def co_tear_down_class(cls):
        # noinspection PyUnresolvedReferences
        yield cls.application.process_shutdown()

    @classmethod
    def setUpClass(cls):
        IOLoop.current().run_sync(cls.co_setup_class)

    @classmethod
    def tearDownClass(cls):
        IOLoop.current().run_sync(cls.co_tear_down_class)

    @classmethod
    @coroutine
    def get_test_db(cls):

        database = Database(
            host=options.db_host,
            database=TEST_DATABASE_NAME,
            user=options.db_username,
            password=options.db_password
        )

        try:
            with (yield database.acquire()) as db:
                yield db.execute(
                    """
                        DROP DATABASE IF EXISTS `{0}`;
                    """.format(TEST_DATABASE_NAME))

                yield db.execute(
                    """
                        CREATE DATABASE IF NOT EXISTS `{0}` CHARACTER SET utf8;
                    """.format(TEST_DATABASE_NAME))

                yield db.execute(
                    """
                        USE `{0}`;
                    """.format(TEST_DATABASE_NAME))

                db.conn._kwargs["db"] = TEST_DATABASE_NAME

        except DatabaseError as e:
            raise TestError("Failed to initialize database. Please make sure "
                            "you've configured access to a test database. Reason: " + e.args[1])

        # since we have the database now, use this dirty hack to set up a database
        # for each new connection in the connection pool
        database.pool._kwargs["db"] = TEST_DATABASE_NAME

        raise Return(database)


class TestWebsocketJsonRPC(JsonRPC):
    def __init__(self, websocket_connection):
        super(TestWebsocketJsonRPC, self).__init__()
        self.websocket_connection = websocket_connection
        self.close_expected = False

    def close(self):
        self.close_expected = True
        self.websocket_connection.close()

    @coroutine
    def write_data(self, context, data):
        self.websocket_connection.write_message(data)


class AcceptanceTestCase(AsyncTestCase):
    """
    Use this test case for testing external REST api of a service.

    Override get_server_instance to return common.Server instance, any test will be performed on that instance,
    available on self.application

    Do not test models in that test case, use ServerTestCase instead
    """

    TESTING_KEY = "m233TJDKgFdW8HbSwFh3B5DgatTMZDgH"
    TOKEN_GAMESPACE = "1"
    TOKEN_GAMESPACE_NAME = "root"
    TOKEN_ACCOUNT = "1"

    APPLICATION_ID = "1"
    APPLICATION_NAME = "test"
    APPLICATION_VERSION_NAME = "0.1"
    APPLICATION_VERSION_ID = "1"

    @classmethod
    def need_access_token(cls):
        """
        In case this whole test needs an access token, this method should be overridden, returning a list of scopes.
        Access token would be created out of this and will be available with "self.access_token"
        """
        return None

    @classmethod
    def need_test_db(cls):
        """
        Return True if you need a test database (will be available as self.test_db)
        """
        return False

    @classmethod
    def get_server_instance(cls, db=None):
        """
        Override this method to return common.Server instance
        :param db: if need_test_db() returns True, this argument will contain a Database reference
        """
        raise NotImplementedError()

    @classmethod
    @coroutine
    def co_setup_acceptance_tests(cls):
        """
        Coroutine-friendly analog of the setUpClass method, override this one to setup things before any test
        """
        pass

    def get_new_ioloop(self):
        return IOLoop.current()

    def tearDown(self):
        pass

    @classmethod
    def get_http_client(cls):
        return AsyncHTTPClient()

    @classmethod
    def get_httpserver_options(cls):
        """May be overridden by subclasses to return additional
        keyword arguments for the server.
        """
        return {}

    @classmethod
    def get_http_server(cls):
        # noinspection PyUnresolvedReferences
        return HTTPServer(cls.application, **cls.get_httpserver_options())

    @classmethod
    @coroutine
    def co_setup_class(cls):
        sock, port = bind_unused_port()
        cls.__port = port

        cls.http_client = cls.get_http_client()

        if cls.need_test_db():
            cls.test_db = yield ServerTestCase.get_test_db()
        else:
            cls.test_db = None

        OptionsLoader.load()

        cls.application = cls.get_server_instance(cls.test_db)

        cls.http_server = cls.get_http_server()
        cls.http_server.add_sockets([sock])

        cls.host_name = "http://127.0.0.1:{0}".format(cls.get_http_port())
        cls.websocket_host_name = "ws://127.0.0.1:{0}".format(cls.get_http_port())

        yield cls.application.started()

        # hack the environment / gamespace info as this information is not available on the current service
        if hasattr(cls.application, "cache"):
            environment_client = EnvironmentClient(cls.application.cache)
            yield environment_client.set_app_info(AcceptanceTestCase.APPLICATION_NAME, ApplicationInfoAdapter({
                "id": AcceptanceTestCase.APPLICATION_ID,
                "name": AcceptanceTestCase.APPLICATION_NAME,
                "title": "Test",
                "versions": {
                    AcceptanceTestCase.APPLICATION_VERSION_NAME: AcceptanceTestCase.APPLICATION_VERSION_ID
                }
            }))

            login_client = LoginClient(cls.application.cache)
            yield login_client.set_gamespace(AcceptanceTestCase.TOKEN_GAMESPACE_NAME, GamespaceAdapter({
                "id": AcceptanceTestCase.TOKEN_GAMESPACE,
                "name": AcceptanceTestCase.TOKEN_GAMESPACE_NAME,
                "title": "Test"
            }))

        scopes = cls.need_access_token()
        if scopes:
            cls.access_token = yield cls.acquire_access_token(scopes)

        IOLoop.current().set_blocking_log_threshold(0)
        
        yield cls.co_setup_acceptance_tests()

    @classmethod
    def setup_access_token(cls):
        """
        Override this to initialize AccessToken differently
        """
        AccessToken.init([sign.HMACAccessTokenSignature(key=AcceptanceTestCase.TESTING_KEY)])

    @classmethod
    def setUpClass(cls):
        super(AcceptanceTestCase, cls).setUpClass()
        cls.setup_access_token()
        IOLoop.current().run_sync(cls.co_setup_class)

    @classmethod
    @coroutine
    def co_tear_down_class(cls):
        # noinspection PyUnresolvedReferences
        yield cls.application.process_shutdown()

    # noinspection PyUnresolvedReferences
    @classmethod
    def tearDownClass(cls):
        super(AcceptanceTestCase, cls).tearDownClass()

        IOLoop.current().run_sync(cls.co_tear_down_class)

        cls.http_server.stop()
        IOLoop.current().run_sync(cls.http_server.close_all_connections, timeout=get_async_test_timeout())
        cls.http_client.close()

    # noinspection PyUnresolvedReferences
    @classmethod
    def get_http_port(cls):
        return cls.__port

    @classmethod
    @coroutine
    def acquire_access_token(cls, scopes, account=TOKEN_ACCOUNT, gamespace_id=TOKEN_GAMESPACE):
        token = AccessTokenGenerator.generate(sign.TOKEN_SIGNATURE_HMAC, scopes, additional_containers={
            AccessToken.ACCOUNT: str(account),
            AccessToken.GAMESPACE: str(gamespace_id)
        }, token_only=True)

        # noinspection PyUnresolvedReferences
        if hasattr(cls.application, "token_cache"):
            # noinspection PyUnresolvedReferences
            token_cache = cls.application.token_cache
            token_cache.store_token_no_db(AccessToken(token))

        return token

    @classmethod
    @coroutine
    def admin_action(cls, action_name, method_name, context, *args, **kwargs):
        # noinspection PyUnresolvedReferences
        action_class = cls.application.actions.action(action_name)
        if action_class is None:
            raise AssertionError("No such admin action: {0}".format(action_name))

        token = AccessToken(AccessTokenGenerator.generate(sign.TOKEN_SIGNATURE_HMAC, [], additional_containers={
            AccessToken.ACCOUNT: str(AcceptanceTestCase.TOKEN_ACCOUNT),
            AccessToken.GAMESPACE: str(AcceptanceTestCase.TOKEN_GAMESPACE)
        }, token_only=True))

        # noinspection PyUnresolvedReferences
        action = action_class(cls.application, token)
        action.context = context

        if not hasattr(action, method_name):
            raise AssertionError("No such method {0} in action {1}".format(method_name, action_name))

        method = getattr(action, method_name)
        try:
            result = yield method(*args, **kwargs)
        except Redirect as e:
            raise Return(e)

        raise Return(result)

    @coroutine
    def json_rpc(self, path, connection_opened, query_args=None, do_await=False, expect_error_code=0,
                 pass_access_token=True):
        """
        Opens a JSONRPC session over Websocket
        :param path: URL to open the JSONRPC on, excluding the hostname
        :param connection_opened: a coroutine that will be opened once a session has been established
        :param query_args: dict with additional arguments (?xxx=xxx&...) to a path
        :param do_await: if True, the message loop will wait for connection_opened coroutine completion
        :param expect_error_code: If an error of the call is expected, the call will fail otherwise
        :param pass_access_token: pass self.access_token to the query_args automatically
        """
        if query_args is None:
            query_args = {}

        if pass_access_token:
            query_args["access_token"] = self.access_token

        url = "{0}/{1}?{2}".format(self.websocket_host_name, path, urllib.urlencode(query_args))

        request = HTTPRequest(url=url)

        try:
            connection = yield websocket_connect(request)
        except HTTPError as e:
            if expect_error_code != 0:
                if e.code == expect_error_code:
                    return
                else:
                    self.fail("Websocket connection expected to fail with {0}, failed with {1} {2}".format(
                        expect_error_code, e.code, e.message))
            else:
                self.fail("Failed to establish websocket connection: {0} {1}".format(e.code, e.message))
        else:
            if expect_error_code != 0:
                self.fail("Websocket connection expected to fail with {0}, but succeeded".format(expect_error_code))

        rpc = TestWebsocketJsonRPC(connection)

        if do_await:
            try:
                yield connection_opened(rpc)
            except Exception:
                connection.close()
                raise
        else:
            IOLoop.instance().add_callback(connection_opened, rpc)

        while True:
            msg = yield connection.read_message()
            if msg is None:
                if not rpc.close_expected:
                    self.fail("Websocket connection unexpectedly closed")
                break
            yield rpc.received(connection, msg)

    def get_success(self, path, query_args=None, headers=None, pass_access_token=True, json_response=True):
        return self._request_success(path, "GET", query_args=query_args, headers=headers,
                                     pass_access_token=pass_access_token, json_response=json_response)

    def post_success(self, path, body, query_args=None, headers=None, pass_access_token=True, json_response=True):
        return self._request_success(path, "POST", body=body, query_args=query_args, headers=headers,
                                     pass_access_token=pass_access_token, json_response=json_response)

    def get_fail(self, path, expected_code, query_args=None, expected_body=None, pass_access_token=True):
        return self._request_fail(path, "GET", expected_code=expected_code, query_args=query_args,
                                  expected_body=expected_body, pass_access_token=pass_access_token)

    def post_fail(self, path, body, expected_code, query_args=None, expected_body=None, pass_access_token=True):
        return self._request_fail(path, "POST", body=body, expected_code=expected_code,
                                  query_args=query_args, expected_body=expected_body,
                                  pass_access_token=pass_access_token)

    @coroutine
    def _request_success(self, path, method, query_args=None, body=None, headers=None,
                         pass_access_token=True, json_response=True):
        if query_args is None:
            url = "{0}/{1}".format(self.host_name, path)
        else:
            url = "{0}/{1}?{2}".format(self.host_name, path, urllib.urlencode(query_args))

        if pass_access_token and hasattr(self, "access_token"):
            if body is None:
                body = {}
            body["access_token"] = self.access_token

        if body is not None:
            body = urllib.urlencode(body)

        request = HTTPRequest(url=url, method=method, body=body, headers=headers)

        try:
            result = yield self.http_client.fetch(request)
        except HTTPError as e:
            self.fail("Failed to perform a {0} request at path /{1}: {2} {3}".format(method, path, e.code, e.response.body))
        else:
            if json_response:
                parsed = ujson.loads(result.body)
                raise Return(parsed)
            else:
                raise Return(result.body)

    @coroutine
    def _request_fail(self, path, method, expected_code, query_args=None, expected_body=None, body=None,
                      pass_access_token=True):
        if query_args is None:
            url = "{0}/{1}".format(self.host_name, path)
        else:
            url = "{0}/{1}?{2}".format(self.host_name, path, urllib.urlencode(query_args))

        if pass_access_token and hasattr(self, "access_token"):
            if body is None:
                body = {}
            body["access_token"] = self.access_token

        if body is not None:
            body = urllib.urlencode(body)

        request = HTTPRequest(url=url, method=method, body=body)

        try:
            yield self.http_client.fetch(request)
        except HTTPError as e:
            if e.code != expected_code:
                self.fail("Request /{0} is expected to fail with code {1}, but failed with {2} instead".format(
                    path, expected_code, e.code
                ))
            if expected_body is not None:
                if e.response.body is None or len(e.response.body) == 0:
                    self.fail("The response is expected to have this body: {0}".format(expected_body))
                if expected_body not in e.response.body:
                    self.fail("The response is expected to have this body: {0}".format(expected_body))
        else:
            self.fail("Request /{0} is expected to fail with {1} {2}, succseeded instead".format(
                path, expected_code, expected_body
            ))


if __name__ == "__main__":
    loader = unittest.TestLoader()
    tests = loader.discover(".")
    runner = unittest.TextTestRunner()
    result = runner.run(tests)
    if not result.wasSuccessful():
        exit(1)
