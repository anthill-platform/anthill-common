
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.gen import coroutine

import logging


# noinspection PyMethodMayBeStatic
class Schedule(PeriodicCallback):
    """
    An abstract scheduler, that performs an update every period of time.

    Also, has a method 'call' to call some coroutine in some period of time later.

    """
    def __init__(self, check_period):

        super(Schedule, self).__init__(
            self.__update__,
            check_period * 1000,
            IOLoop.current())

        self.calls = {}

    def start(self):
        super(Schedule, self).start()
        IOLoop.current().add_callback(self.update)

    def __update__(self):

        # clear the calls list every update
        self.calls = {}
        IOLoop.current().add_callback(self.update)

    @coroutine
    def stop(self):
        logging.info("Stopping schedule...")

        super(Schedule, self).stop()

        for (timeout, call_name, args, kwargs) in self.calls.values():
            IOLoop.current().remove_timeout(timeout)
            yield self.cancelled(call_name, *args, **kwargs)

    @coroutine
    def cancelled(self, call_name, *args, **kwargs):
        """
        Called when some action is cancelled and should be rolled back
        :param call_name: A reference passed into the call method
        """
        raise NotImplementedError()

    @coroutine
    def update(self):
        raise NotImplementedError()

    def __run_coroutine__(self, callback, *args, **kwargs):

        self.calls.pop(id(callback))

        IOLoop.current().add_callback(callback, *args, **kwargs)

    def call(self, call_name, callback, when, *args, **kwargs):
        """
        Calls a coroutine within a time 'delay'.
        :param call_name: A reference will be passed in case the system will be shut down
        :param callback: A @coroutine function
        :param when: Time when to call (for example, datetime.timedelta or an actual time)
        :param args: *args to be passed into coroutine
        :param kwargs: **kwargs to be passed into coroutine
        :return:
        """
        timeout = IOLoop.current().add_timeout(
            when,
            self.__run_coroutine__, callback, *args, **kwargs)

        self.calls[id(callback)] = (timeout, call_name, args, kwargs)
