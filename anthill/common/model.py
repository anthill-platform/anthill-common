
from tornado.gen import coroutine
from .database import DatabaseError
from .server import Server
import logging


class Model(object):

    async def __setup_event__(self, event_name):
        events = await self.get_setup_db().get(
            """
                SHOW EVENTS LIKE %s;
            """, event_name)

        if events:
            if event_name in events.values():
                return

        with (open("sql/{0}.sql".format(event_name))) as f:
            sql = f.read()

        try:
            await self.get_setup_db().execute(sql)
        except DatabaseError as e:
            logging.error("Failed to create event '{0}': {1}".format(event_name, e.args[1]))
        else:
            logging.warn("Created event '{0}'".format(event_name))

            method_name = "setup_event_" + event_name

            if hasattr(self, method_name):
                await getattr(self, method_name)()

    async def __setup_trigger__(self, trigger_name):
        triggers = await self.get_setup_db().get(
            """
                SHOW TRIGGERS WHERE `Trigger`=%s;
            """, trigger_name)

        if triggers:
            return

        with (open("sql/{0}.sql".format(trigger_name))) as f:
            sql = f.read()

        try:
            await self.get_setup_db().execute(sql)
        except DatabaseError as e:
            logging.error("Failed to create trigger '{0}': {1}".format(trigger_name, e.args[1]))
        else:
            logging.warn("Created trigger '{0}'".format(trigger_name))

            method_name = "setup_trigger_" + trigger_name

            if hasattr(self, method_name):
                await getattr(self, method_name)()

    async def __setup_table__(self, table_name):
        tables = await self.get_setup_db().get(
            """
                SHOW TABLES LIKE %s;
            """, table_name)

        if tables:
            if table_name in tables.values():
                return

        with (open(Server.module_path("sql", table_name + ".sql"))) as f:
            sql = f.read()

        try:
            await self.get_setup_db().execute(sql)
        except DatabaseError as e:
            logging.error("Failed to create table '{0}': {1}".format(table_name, e.args[1]))
        else:
            logging.warning("Created table '{0}'".format(table_name))

            method_name = "setup_table_" + table_name

            if hasattr(self, method_name):
                await getattr(self, method_name)()

    # noinspection PyMethodMayBeStatic
    def has_delete_account_event(self):
        return False

    async def accounts_deleted(self, gamespace, accounts, gamespace_only):
        raise NotImplementedError()

    def get_setup_tables(self):
        return []

    def get_setup_events(self):
        return []

    def get_setup_triggers(self):
        return []

    def get_setup_db(self):
        raise NotImplementedError()

    async def started(self, application):
        for table in self.get_setup_tables():
            await self.__setup_table__(table)

        for event in self.get_setup_events():
            await self.__setup_event__(event)

        for trigger in self.get_setup_triggers():
            await self.__setup_trigger__(trigger)

        logging.info("Model '{0}' started".format(self.__class__.__name__))

    async def stopped(self):
        """
        Called when a server is about to shutdown.
        Please not that within 5 second the server will be shut down anyway.
        """
        logging.info("Model '{0}' stopped".format(self.__class__.__name__))
