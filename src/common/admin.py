
import tornado.websocket
import tornado.ioloop
import handler
import access
import jsonrpc
import ujson
import logging
import base64

from tornado.gen import coroutine, Return
from tornado.web import HTTPError, stream_request_body
from access import scoped, internal
from validate import ValidationError

"""
  This module allows an 'admin' service to proceed administration on each service available across the environment.

  It goes like this:

          | firewall
  (user) --> (admin service) --> (other services) -> (back to admin) -> (back to user)
          |

  So user has no direct access to any of services, but to the admin service, and each request to the admin service
  is being forwarded to target service after validation.

  In terms of code, it goes like this:

  (user) --| requests a page admin/<service>/<action> |--> (admin) --| calls action <action> of the <service> |--.
    ^                                                                                                             |
    |                                                                                                             v
    `--| renders html page, buttons, forms etc |-- (admin) <--| does the request, returns simplified UI |--  (service)

  Class `AdminController` is used to process those requests. Please refer `AdminController` for more information.

"""


REDIRECT = 444
ACTION_ERROR = 445


class ActionError(Exception):
    """
    Raise in AdminController to notify user about error happened.
    :param title An error reason
    :param error_links A list of links (see 'link' method below) to give user choice.

    If no links is passed, error will go like a small popup and user will be redirected to a previous page, is possible.
    """
    def __init__(self, title, error_links=None):
        self.title = str(title)
        self.links = error_links


class AdminController(object):
    """
    Single administration point. Allows user do administrate one action (bound by method app.get_admin).

    Please override methods 'get', 'render', 'access_scopes' and other custom.

    Additional information about this action (for example, account number we editing) is stored in `context`.
    When method 'get' is called, the whole context arguments is passed to is.
    From other methods, it can be acquired by self.context.get("account")

    Scheme goes like this:

    1. User requests a page, method 'get' gets called, with appropriate arguments from the context.
    2. Method 'get' proceeds the request and collect data about this action, returning a k/v dict.
    3. Result from method 'get' is passed to method 'render'. This method renders the UI by returning a list
       of simplified UI elements (like form, links, a notice).
    4. Result from method 'render' is used to convert in into a html page with styles and scripts.
    5. When users makes action (posts a form), the @coroutine method <form_method> is called, with user arguments.
    6. Result of this method is used to render a html page.

    If 'get' or '<form_method>' raise Redirect exception, user will be redirected to such direction.

    A form with method 'update_account' and fields 'username' and 'age' could be implemented like this:

    @coroutine
    def update_account(username, age):
        ... update account in the db ...
        raise Redirect("account", account=self.context.get("account))

    """

    def __init__(self, app, token):
        self.application = app
        self.token = token
        self.context = {}

        if self.token:
            self.gamespace = self.token.get(access.AccessToken.GAMESPACE)

    @coroutine
    def get(self, **context):
        """
        A @coroutine method uses to gather data required to render the page.

        For example:

        {
            "username": "john",
            "age": 21
        }

        If 'Redirect' exception is raised, user will be redirected to such direction.
        :returns: A dict of data being gathered.
        """
        raise Return({})

    def get_context(self, key):
        try:
            return self.context[key]
        except KeyError:
            raise ActionError(title="Missing context")

    def render(self, data):
        """
        This method returns a list of UI elements to be returned to the users. Please see functions at the bottom of
        this file for each of them.

        For example,

        [
            links("Navigate", links=[
                link("account", "John account", account=5),
                link("account", "Suisie account", account=24),
            ])
        ]

        Normally, no logic goes here.
        """
        return []

    @staticmethod
    def render_error(title, links):
        result = [{
            "class": "error",
            "title": title
        }]

        if links:
            result.append({
                "class": "links",
                "title": "Navigate",
                "links": links
            })

        return result

    def access_scopes(self):
        """
        :returns: a list of scopes required to show this action.
        """
        return []


class UploadAdminController(AdminController):
    """
    Same as above, but with upload stream support
    """
    @coroutine
    def receive_started(self, filename):
        pass

    @coroutine
    def receive_completed(self):
        pass

    @coroutine
    def receive_data(self, chunk):
        pass


class AdminActions(object):
    def __init__(self, actions):
        self.actions = actions

    def action(self, action):
        try:
            return self.actions[action]
        except KeyError:
            return None

    def list(self):
        return {action_id: action.scheme() for action_id, action in self.actions.iteritems()}


class AdminFile(object):
    def __init__(self, name, data):
        self.name = name
        self.data = base64.b64decode(data)


@stream_request_body
class AdminUploadHandler(handler.AuthenticatedHandler):

    def __init__(self, application, request, **kwargs):
        super(AdminUploadHandler, self).__init__(application, request, **kwargs)
        self.action = None
        self.actions = None
        self.filename = ""
        self.total_received = 0

    @coroutine
    @internal
    @scoped(scopes=["admin"])
    def put(self):
        try:
            result = yield self.action.receive_completed()
        except ValidationError as e:
            result = AdminController.render_error(e.message, [])
            self.set_status(ACTION_ERROR, "Action-Error")
        except ActionError as e:
            result = AdminController.render_error(e.title, e.links)
            self.set_status(ACTION_ERROR, "Action-Error")
        except TypeError as e:
            logging.exception("TypeError")
            raise HTTPError(400, e.message)
        except Redirect as e:

            # special status 470 means redirect
            self.set_status(REDIRECT, "Redirect-To")

            result = {
                "context": e.context,
                "redirect-to": e.action
            }

            if e.notice:
                result["notice"] = e.notice

        self.dumps(result)

    def get_action(self, action_id):
        action_class = self.actions.action(action_id)

        if action_class is None:
            raise HTTPError(404, "No such action: " + action_id)

        if not issubclass(action_class, UploadAdminController):
            raise HTTPError(400, "Action does not support uploading")

        return action_class(self.application, self.token)

    @coroutine
    def prepare(self):
        self.request.connection.set_max_body_size(1073741824)
        yield super(AdminUploadHandler, self).prepare()

    @coroutine
    @internal
    @scoped(scopes=["admin"])
    def prepared(self, *args, **kwargs):
        self.actions = self.application.actions
        self.action = self.get_action(self.get_argument("action"))
        self.filename = self.request.headers.get("X-File-Name", "")

        self.action.context = ujson.loads(self.get_argument("context"))

        scopes = self.action.access_scopes()
        token = self.current_user.token

        if not token.has_scopes(scopes):
            self.set_header("Need-Scopes", ",".join(scopes))
            raise HTTPError(401, "Need to authorize.")

        try:
            yield self.action.receive_started(self.filename)
        except ValidationError as e:
            self.set_status(ACTION_ERROR, "Action-Error")
            self.finish(e.message)
        except ActionError as e:
            self.set_status(ACTION_ERROR, "Action-Error")
            self.finish(e.title)
        except TypeError as e:
            logging.exception("TypeError")
            raise HTTPError(400, e.message)
        except Redirect as e:

            # special status 470 means redirect
            self.set_status(REDIRECT, "Redirect-To")

            result = {
                "context": e.context,
                "redirect-to": e.action
            }

            if e.notice:
                result["notice"] = e.notice

            self.dumps(result)
            self.finish()

    @coroutine
    def data_received(self, chunk):
        self.total_received += len(chunk)
        yield self.action.receive_data(chunk)


class AdminHandler(handler.AuthenticatedHandler):
    def __init__(self, application, request, **kwargs):
        handler.AuthenticatedHandler.__init__(self, application, request, **kwargs)
        self.actions = None
        self.action = None

    @coroutine
    @internal
    @scoped(scopes=["admin"])
    def get(self):
        scopes = self.action.access_scopes()
        token = self.current_user.token

        if not token.has_scopes(scopes):
            self.set_header("Need-Scopes", ",".join(scopes))
            self.set_status(401)
            self.write("Need to authorize.")
            return

        self.action.context = ujson.loads(self.get_argument("context"))

        try:
            data = yield self.action.get(**self.action.context)
        except NotImplementedError:
            raise HTTPError(405, "Method not allowed.")
        except ValidationError as e:
            result = AdminController.render_error(e.message, [])
            self.set_status(ACTION_ERROR, "Action-Error")
        except ActionError as e:
            result = AdminController.render_error(e.title, e.links)
            self.set_status(ACTION_ERROR, "Action-Error")
        except TypeError as e:
            logging.exception("TypeError")
            raise HTTPError(400, e.message)
        except Redirect as e:

            # special status 470 means redirect
            self.set_status(REDIRECT, "Redirect-To")

            result = {
                "context": e.context,
                "redirect-to": e.action
            }

            if e.notice:
                result["notice"] = e.notice
        else:
            result = self.action.render(data)

        self.dumps(result)

    def get_action(self, action_id):
        action_class = self.actions.action(action_id)

        if action_class is None:
            raise HTTPError(404, "No such action: " + action_id)

        return action_class(self.application, self.token)

    @coroutine
    @internal
    @scoped(scopes=["admin"])
    def post(self):
        scopes = self.action.access_scopes()
        token = self.current_user.token

        if not token.has_scopes(scopes):
            self.set_header("Need-Scopes", ",".join(scopes))
            self.set_status(401)
            self.write("Need to authorize.")
            return

        try:
            self.action.context = ujson.loads(self.get_argument("context"))
            data = ujson.loads(self.get_argument("data"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Bad request")

        if not isinstance(data, dict):
            raise HTTPError(400, "Bad request")

        method_name = self.get_argument("method")

        try:
            action_method = getattr(self.action, method_name)
        except AttributeError:
            raise HTTPError(405, "No such method: " + method_name)

        def process(name, value):

            if isinstance(value, dict):
                if "@files" in value:
                    return [AdminFile(name, d) for name, d in value["@files"].iteritems()]

            return value

        arguments = {
            name: process(name, value)
            for name, value in data.iteritems()
        }

        try:
            data = yield action_method(**arguments)
        except ActionError as e:
            result = AdminController.render_error(e.title, e.links)
            self.set_status(ACTION_ERROR, "Action-Error")
        except ValidationError as e:
            result = AdminController.render_error(e.message, [])
            self.set_status(ACTION_ERROR, "Action-Error")
        except Redirect as e:

            # special status 470 means redirect
            self.set_status(REDIRECT, "Redirect-To")

            result = {
                "context": e.context,
                "redirect-to": e.action
            }

            if e.notice:
                result["notice"] = e.notice
        else:
            result = self.action.render(data)

        self.dumps(result)

    @coroutine
    def prepare(self):
        yield super(AdminHandler, self).prepare()

        self.actions = self.application.actions
        self.action = self.get_action(self.get_argument("action"))


class AdminMetadataHandler(handler.AuthenticatedHandler):
    @coroutine
    @internal
    def get(self):
        if not self.application.metadata:
            raise HTTPError(404, "No metadata")

        self.dumps(self.application.metadata)


class AdminWSActionConnection(handler.AuthenticatedWSHandler):
    @internal
    @coroutine
    def prepared(self, *args, **kwargs):
        yield super(AdminWSActionConnection, self).prepared()

        action = self.get_argument("action")

        if hasattr(self, action):
            try:
                yield getattr(self, action)()
            except ActionError as e:
                raise HTTPError(ACTION_ERROR, e.title)
            except ValidationError as e:
                raise HTTPError(ACTION_ERROR, e.message)
        else:
            raise HTTPError(400, "Bad action: " + action)


class AdminWSConnection(handler.AuthenticatedWSHandler):
    @internal
    @coroutine
    def prepared(self, *args, **kwargs):
        yield super(AdminWSConnection, self).prepared()


class AdminWSHandler(handler.AuthenticatedWSHandler):
    def __init__(self, application, request, **kwargs):
        super(AdminWSHandler, self).__init__(application, request, **kwargs)
        self.actions = self.application.stream_actions
        self.action = None

    def get_action(self, action_id):
        action_class = self.actions.action(action_id)

        if action_class is None:
            raise HTTPError(404, "No such action: " + action_id)

        return action_class(self.application, self.token, self)

    @coroutine
    def closed(self):
        if self.action:
            self.action.on_close()

    def on_message(self, message):
        tornado.ioloop.IOLoop.current().add_callback(self.action.on_message, message)

    @coroutine
    def opened(self, *args, **kwargs):
        self.action = self.get_action(self.get_argument("action"))

        token = self.current_user.token

        scopes = self.action.scopes_stream()

        if not token.has_scopes(scopes):
            self.close(401, ",".join(scopes))
            return

        self.action.context = ujson.loads(self.get_argument("context"))

        try:
            yield self.action.prepared(**self.action.context)
        except NotImplementedError:
            self.close(105, "Method not allowed.")
            return
        except RedirectStream as e:

            result = {
                "action": e.action,
                "context": e.context,
                "host": e.host
            }

            self.set_status(REDIRECT, "Redirect-To")
            self.dumps(result)
            self.finish()
        except ActionError as e:
            self.close(400, e.title)
            return
        except StreamCommandError as e:
            self.close(e.code, e.message)
            return
        except ValidationError as e:
            self.close(400, e.message)
            return

    def required_scopes(self):
        return ["admin"]


class Redirect(Exception):
    """
    Raise in AdminController to redirect into another action.
    :param action Another action id,
    :param message (optional) A message to display to the user after redirect,
    :param context: context of the redirection
    """
    def __init__(self, action, message=None, **context):
        self.action = action
        self.context = context
        self.notice = message


class RedirectStream(Exception):
    def __init__(self, action, host, **context):
        self.action = action
        self.context = context
        self.host = host


class StreamCommandError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return str(self.code) + ": " + self.message


# noinspection PyMethodMayBeStatic
class StreamAdminController(AdminController, jsonrpc.JsonRPC):
    def __init__(self, app, token, handler):
        AdminController.__init__(self, app, token)
        jsonrpc.JsonRPC.__init__(self)
        self.handler = handler
        self.set_receive(self.command_received)

    def close(self, code, reason):
        self.handler.close(code, reason)

    @coroutine
    def command_received(self, context, action, *args, **kwargs):
        if hasattr(self, action):
            if action.startswith("_"):
                raise jsonrpc.JsonRPCError(400, "Actions starting with underscore are not allowed!")

            try:
                response = yield getattr(self, action)(*args, **kwargs)
            except TypeError as e:
                raise jsonrpc.JsonRPCError(400, "Bad arguments: " + e.args[0])
            except StreamCommandError as e:
                raise jsonrpc.JsonRPCError(e.code, e.message)
            except ValidationError as e:
                raise jsonrpc.JsonRPCError(400, e.message)
            except Exception as e:
                raise jsonrpc.JsonRPCError(500, "Error: " + str(e))

            raise Return(response)

    def on_close(self):
        pass

    @coroutine
    def on_message(self, message):
        yield self.received(self, message)

    @coroutine
    def prepared(self, *args, **kwargs):
        """
        Called when the action is prepared.
        It's the last resort to throw an error here.
        It's the websockets now, not a http connection, raising HTTPError won't help
        """
        pass

    def scopes_stream(self):
        return []

    @coroutine
    def write_data(self, context, data):
        if self.handler:
            yield self.handler.write_message(data)


def link(url, title, icon=None, badge=None, **context):
    """
    A single link (usually used in a bundle with 'links' method.
    :param url: An action this link leads to. 'account', 'event' etc

                If the url starts with a slash, an looks like this: /aaa/bbb, then this link will lead to
                action <bbb> of the service <aaa>.

                Absolute external http(s) links also allowed.

    :param title: Link's title
    :param icon: (optional) An icon next to the link (font-awesome)
    :param badge: (optional) A small badge next to the link, like: See this(github)
    :param context: A list of arguments making context.

    link("account", "John's account", icon="account", account=5)
    link("index", "Home page")
    link("profile", "See profile", badge="profile", account=14)
    link("/environment/test", "See profile at 'environment'", badge="profile", account=14)

    """

    return {
        "url": url,
        "title": title,
        "context": context,
        "badge": badge,
        "class": "link",
        "icon": icon
    }


def status(title, style, icon=None):
    """
    A status line, with possible icon.
    :param title: Status title
    :param style: Status style (primary, danger etc)
    :param icon: (optional) An icon next to the status (font-awesome)

    status("Loading", "refresh fa-spin")
    status("Complete", "check")
    """

    return {
        "style": style,
        "title": title,
        "class": "status",
        "icon": icon
    }


def script(script_file, **context):
    """
    User-defined script to be loaded into administrative page.
    :param script_file: location of javascript file (in the service's working directory).
        Must be a javascript function.

    :param context: context will be passed to the called script.
    """
    with open(script_file) as f:
        return {
            "class": "script",
            "script": f.read(),
            "context": context
        }


def links(links_title, links=None, **kwargs):
    """
    A section of the list. Please see 'link'.
    :param links_title: A title of the section.
    :param links: list of the links
    :param kwargs: additional links defined is simplified form:
                   links(..., home="Go home", index="Main page")
    """

    l = links or []
    for url, title in kwargs.iteritems():
        l.append({"title": title, "url": url})

    return {
        "class": "links",
        "title": links_title,
        "links": l
    }


def pages(count, key="page"):
    """
    Renders a pages block [<<] [1] [2] [3] [>>]
    :param count: Maximum of pages
    :param key: A key from the context to determine the current page.
    """

    return {
        "class": "pages",
        "key": key,
        "count": count
    }


def field(title, _type, style, validation=None, **data):
    """
    A single field in a form. Please see 'form'.
    :param title: A title of the form.
    :param _type: Kind of the field.

        'text' - single text input
        'switch' - an on-off button
        'date' - date selection
        'kv' - key/value edit. requires additional argument 'values' to display keys to the user.
        'notice' - read-only notice
        'readonly' - read-only text input
        'file' - file selection (base-64 encoded contents will be passed as an argument)
        'json' - raw JSON edit
        'dorn' - tempated edit (see https://github.com/jdorn/json-editor/)
        'select' - selection. requires additional afrument 'values' to display options titles
        'hidden' - hidden field
        'tags' - tags edit [abc] [other_tag] [etc]

    :param style: Style of the field (if applicable): primary|danger|info|warning|success
    :param validation: (optional) field validation

        'non-empty' - field must be set
        'number' - must be a number

    :param data: Additional data may be required for fields.

        'order' - since 'field' is used in a dict, using such argument may be possible to order fields.
    """
    result = {
        "title": title,
        "type": _type,
        "style": style,
        "validation": validation
    }
    result.update(data)
    return result


def method(title, style, **data):
    """
    A single method button in a form. Please see 'form'.
    :param title: A button's title
    :param style: Style of the button (if applicable): primary|danger|info|warning|success
    :param data: Additional data (if applicable)

        'order' - since 'method' is used in a dict, using such argument may be possible to order methods.

    :return:
    """
    result = {
        "title": title,
        "style": style
    }
    result.update(data)
    return result


def notice(title, text):
    """
    A notice panel.
    :param title: Notice title
    :param text: Notice information
    """
    return {
        "class": "notice",
        "title": title,
        "text": text
    }


def split(items):
    """
    Usually everything in admin tool is ordered one-by-one from top to bottom

    <form1>
    --------
    <form2>
    --------
    <links>

    This may be space insufficient. Place two items inside this split, and result will be

    <form1> | <form2>
    -----------------
         <links>

    :param items:
    :return:
    """
    return {
        "class": "split",
        "items": items
    }


def breadcrumbs(items, title):
    """
    Breadcrumbs widget, to help user navigate.

    Home > Users > John > Privacy

    :param items: list of the links, from root to last child
    :param title: A title (non-clickable) of the last child
    """
    return {
        "class": "breadcrumbs",
        "links": items,
        "title": title
    }


def file_upload(title, action=""):
    """
    File upload form. Has streaming support so big files can be uploaded.
    :param title: A title of the upload form
    :param action: Upload target action that will receive the file. Empty for current action.
            Please note, such action should be inherited from UploadAdminController
    """
    return {
        "class": "file_upload",
        "title": title,
        "action": action
    }


def form(title, fields, methods, data, icon=None, **context):
    """
    Used by user to edit some data.

    If user presses the button <method>, the appropriate method <method> will be called at AdminController.
    Values from the methods will be passed to the <method>'s arguments.

    Typical approach:

    [
        a.form("Edit user", fields={
            "username": a.field("Username", "text", "primary", "non-empty"),
            "age": a.field("Age", "text", "primary", "number")
        }, methods={
            "update": a.method("Update", "primary")
        }, data=data)
    ]

    After user action, the method update(username, action) will be called.
    To fill-up this form, method get() should return {"username": "John", "age": 21}

    :param title: Title of the form.
    :param fields: Dict of fields, each key represents field ID
    :param methods: Dict of method buttons, each key represents form method
    :param data: (dict) Data passed to this field will be used by fields to fill-up the form.
    :param icon: (optional) Form icon
    :param context: (optional) Context may be used by fields.
    """
    f = {}

    for field_id, _field in fields.iteritems():
        f[field_id] = {"value": data.get(field_id, None)}
        f[field_id].update(_field)

    return {
        "class": "form",
        "title": title,
        "fields": f,
        "icon": icon,

        "methods": {method_id: _method for method_id, _method in methods.iteritems()},

        "context": context
    }


def button(url, title, style, _method="get", **context):
    """
    A single button like [EDIT]
    :param url: Action this button goes to.
    :param title: Title of the button.
    :param style: Style of the button (if applicable): primary|danger|info|warning|success
    :param _method: (optional) A method. If "get" is passed, AdminController's get will be use. Otherwise, a method
                    <method> will be called, with arguments from context
    :param context: Context of the button. See 'link'.
    :return:
    """
    return {
        "class": "button",
        "title": title,
        "style": style,
        "url": url,
        "method": _method,
        "context": context
    }


def content(title, headers, items, style, **context):
    """
    A table.
    :param title: Table's title.
    :param headers: List of headers [..., {"id": "<header_id>", "title": "<header_title>"}, ...]
    :param items: List of content (dicts). Each header will take <header_id> part.
    :param style: Style of the form (if applicable): primary|danger|info|warning|success
    :return:
    """
    return {
        "class": "content",
        "title": title,
        "headers": headers,
        "items": items,
        "style": style,
        "context": context
    }


def json_view(contents):
    """
    Read-only json
    :param contents: raw JSON object
    """
    return {
        "class": "json_view",
        "contents": contents
    }
