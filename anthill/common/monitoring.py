
from tornado.ioloop import PeriodicCallback

import sprockets_influxdb as influx

from .access import utc_time


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
        """

        raise NotImplementedError()

    def add_action(self, name, values, **tags):
        """
        This method registers a single point of action with certain value.
        Tags are used for aggregation.
        """

        raise NotImplementedError()


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
        bundled_tags = frozenset(tags.iteritems())

        if existing_group is None:
            measurement = influx.Measurement(self.db, name=name)
            measurement.set_tags(tags)
            self.rates[name] = {
                bundled_tags: measurement
            }
        else:
            measurement = existing_group.get(bundled_tags, None)

            if measurement is None:
                measurement = influx.Measurement(self.db, name=name)
                measurement.set_tags(tags)
                existing_group[bundled_tags] = measurement

        existing_entry = measurement.fields.get(name_property, None)
        if existing_entry is None:
            measurement.fields[name_property] = 1
        else:
            measurement.fields[name_property] = existing_entry + 1

    def __flush_rates__(self):
        if len(self.rates) == 0:
            return

        for group_name, measurements in self.rates.iteritems():
            for measurement in measurements.itervalues():
                influx.add_measurement(measurement)

        self.rates = {}

    def add_action(self, name, values, **tags):
        measurement = influx.Measurement(self.db, name=name)
        measurement.set_tags(tags)
        measurement.fields = values

        influx.add_measurement(measurement)


