
from tornado.gen import coroutine, Return, Future, with_timeout, TimeoutError

import ujson
import logging
import datetime


JSONRPC_TIMEOUT = 10


class JsonRPC(object):
    """
    Asynchronous JSON-RPC protocol implementation. See http://www.jsonrpc.org/specification
    """
    @staticmethod
    def __generate_request__(method, *args, **kwargs):

        data = {
            "jsonrpc": "2.0",
            "method": method,
        }

        params = JsonRPC.__serialize_params__(args, kwargs)

        if params:
            data["params"] = params

        return data, params

    def __get_next_id__(self):
        self.next_id += 1
        return self.next_id

    def __init__(self):
        super(JsonRPC, self).__init__()
        self.on_receive = None
        self.next_id = 0
        self.handlers = {}

    @staticmethod
    def __log_error__(code, message, data):
        logging.error("Error " + str(code) + " " + str(message) + ((". " + str(data)) if data else ""))

    @staticmethod
    def __parse_error__(error):
        if not isinstance(error, dict):
            raise JsonRPCError(-32600, "Invalid Request", "Bad 'error' field.")

        code = error.get("code", None)
        message = error.get("message", None)
        data = error.get("data", None)

        if code is None or message is None:
            raise JsonRPCError(-32600, "Invalid Request", "Bad 'error' field.")

        return code, message, data

    @staticmethod
    def __parse_params__(params):
        if params is None:
            return [], {}
        if isinstance(params, dict):
            return [], params
        return params, {}

    @staticmethod
    def __serialize_error__(code, message, data=None):
        to_write = {
            "code": code,
            "message": message
        }

        if data:
            to_write["data"] = data

        return to_write

    @staticmethod
    def __serialize_params__(args, kwargs):
        if args:
            return args
        if kwargs:
            return kwargs
        return None

    @coroutine
    def __write_error__(self, context, code, message, data=None, msg_id=None):
        JsonRPC.__log_error__(code, message, data)

        to_write = {
            "jsonrpc": "2.0",
            "error": JsonRPC.__serialize_error__(code, message, data)
        }

        yield self.write_object(context, to_write, id=msg_id)

    @coroutine
    def received(self, context, msg, **payload):

        try:
            msg = ujson.loads(msg)
        except (KeyError, ValueError) as e:
            yield self.__write_error__(context, -32700, "Parse error")
            return

        if "jsonrpc" not in msg:
            yield self.__write_error__(context, -32600, "Invalid Request", "No 'jsonrpc' field.")
            return

        if msg["jsonrpc"] != "2.0":
            yield self.__write_error__(context, -32600, "Bad version of 'jsonrpc': " + msg["jsonrpc"] + ".")
            return

        msg.update(payload)

        logging.debug("Received: {0}".format(ujson.dumps(msg)))

        has_id = "id" in msg
        has_method = ("method" in msg) and msg["method"] is not None
        has_params = "params" in msg
        has_result = "result" in msg
        has_error = "error" in msg

        params = msg["params"] if has_params else None
        method = msg["method"] if has_method else None
        msg_id = msg["id"] if has_id else None
        error = msg["error"] if has_error else None
        result = msg["result"] if has_result else None

        if has_error:
            # parse an error into an exception object

            try:
                code, message, data = JsonRPC.__parse_error__(msg["error"])
            except JsonRPCError as e:
                # ironically, error parsing may cause an error
                yield self.__write_error__(e.code, e.message, e.data)
                return
            else:
                error = JsonRPCError(code, message, data)

        if has_id and has_method:
            # a request
            payload["id"] = msg_id

            args, kwargs = JsonRPC.__parse_params__(params)
            if self.on_receive:
                try:
                    response = yield self.on_receive(context, method, *args, **kwargs)
                except JsonRPCError as e:
                    yield self.__write_error__(context, e.code, e.message, e.data, msg_id)
                    return
                else:
                    yield self.respond(context, response, **payload)
            else:
                yield self.__write_error__(context, -32603,
                                           "Internal error", "Receive handler is not assigned", msg_id)
                return
        elif has_id:
            # a response

            if has_error == has_result:
                yield self.__write_error__(context, -32600,
                                           "Invalid Request",
                                           "Should be (only) one 'result' or 'error' field.")
                return

            if msg_id in self.handlers:
                future = self.handlers.pop(msg_id)
            else:
                yield self.__write_error__(context, -32600, "Invalid Request", "Unknown message id")
                return

            if has_result:
                # successful response
                future.set_result(result)
            else:
                # a fail
                future.set_exception(error)
        elif has_method:
            args, kwargs = JsonRPC.__parse_params__(params)
            try:
                yield self.on_receive(context, method, *args, **kwargs)
            except JsonRPCError as e:
                JsonRPC.__log_error__(e.code, e.message, None)

        elif has_error:
            JsonRPC.__log_error__(error.code, error.message, error.data)
        else:
            yield self.__write_error__(context, -32600, "Invalid Request", "No 'method' nor 'id' field.")

    @coroutine
    def release(self):
        pass

    @coroutine
    def request(self, context, method, timeout=JSONRPC_TIMEOUT, *args, **kwargs):
        msg_id = self.__get_next_id__()
        data, params = self.__generate_request__(method, *args, **kwargs)

        future = Future()
        self.handlers[msg_id] = future

        # send it out
        yield self.write_object(context, data, id=msg_id)

        # and wait for the response
        try:
            result = yield with_timeout(datetime.timedelta(seconds=timeout), future)
        except TimeoutError:
            # remove the handler if timed out
            self.handlers.pop(msg_id)
            raise JsonRPCTimeout()
        else:

            raise Return(result)

    @coroutine
    def respond(self, context, msg, **payload):
        yield self.write_object(context, {
            "jsonrpc": "2.0",
            "result": msg
        }, **payload)

    @coroutine
    def rpc(self, context, method, *args, **kwargs):
        to_write = {
            "jsonrpc": "2.0",
            "method": method
        }
        params = JsonRPC.__serialize_params__(args, kwargs)
        if params:
            to_write["params"] = params

        yield self.write_object(context, to_write)

    def set_receive(self, handler):
        self.on_receive = handler

    @coroutine
    def write_data(self, context, data):
        raise NotImplementedError()

    @coroutine
    def write_object(self, context, data, **payload):
        data.update(payload)
        yield self.write_data(context, ujson.dumps(data))


class JsonRPCError(Exception):
    def __init__(self, code, message, data=None):
        self.code = code
        self.message = message
        self.data = data


class JsonRPCTimeout(Exception):
    pass


