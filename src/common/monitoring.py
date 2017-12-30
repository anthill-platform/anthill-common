
from tornado.gen import coroutine, Return
from tornado.ioloop import PeriodicCallback
from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPError

from collections import deque
from access import utc_time

import ujson
import logging
import sprockets_influxdb as influx


class MonitoringAction(object):
    def __init__(self, name, value, tags):
        self.name = name
        self.value = value
        self.tags = tags
        self.time = utc_time()


class MonitoringRate(object):
    def __init__(self, tags):
        self.value = 1
        self.tags = tags


class Monitoring(object):
    def __init__(self):
        pass

    def add_rate(self, name, name_property, **tags):
        """
        This method increments a "group.name" point of name actions per minute, that is flushed every minute.
        For example, to track number of errors per minute, you can do add_rate("web", "error") for every error occurred.
        Tags are used only the first occurrence per minute.
        """

        raise NotImplementedError()

    def add_action(self, name, values, **tags):
        """
        This method registers a single point of action with certain value.
        Tags are used for aggregation.
        """

        raise NotImplementedError()


class InfluxDBMonitoringRateMeasurement(object):
    def __init__(self, name, tags):
        self.values = {
            name: 1
        }
        self.tags = tags


class InfluxDBMonitoring(Monitoring):
    def __init__(self, host="127.0.0.1", port=8086, db="dev", username="",
                 password="", flush_period=10000, flush_size=10000):

        super(InfluxDBMonitoring, self).__init__()

        self.db = db
        self.rates = {}
        self.flush_rates = PeriodicCallback(self.__flush_rates__, 60000)

        influx.install(
            url="http://{0}:{1}/write".format(host, port),
            submission_interval=flush_period,
            max_batch_size=flush_size,
            auth_username=username,
            auth_password=password
        )

        self.flush_rates.start()

    def add_rate(self, name, name_property, **tags):
        existing_group = self.rates.get(name, None)
        if existing_group is None:
            self.rates[name] = InfluxDBMonitoringRateMeasurement(name, tags)
        else:
            existing_entry = existing_group.values.get(name_property, None)
            if existing_entry is None:
                existing_group.values[name_property] = 1
            else:
                existing_group.values[name_property] = existing_entry + 1

    def __flush_rates__(self):
        if len(self.rates) == 0:
            return

        for group_name, group in self.rates.iteritems():
            measurement = influx.Measurement(self.db, name=group_name)
            measurement.set_tags(group.tags)
            measurement.fields = group.values

            influx.add_measurement(measurement)

        self.rates = {}

    def add_action(self, name, values, **tags):
        measurement = influx.Measurement(self.db, name=name)
        measurement.set_tags(tags)
        measurement.fields = values

        influx.add_measurement(measurement)


