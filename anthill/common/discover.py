
from anthill.common.options import define, options

from . import internal
from . import singleton


class Discovery(object):
    __metaclass__ = singleton.Singleton

    def __init__(self):
        self.discovery_service = options.discovery_service
        self.services = {
            "internal:discovery": self.discovery_service
        }
        self.internal = internal.Internal()

    async def get_services(self, services, network="internal"):

        if all([(network + ":" + service_id) in self.services for service_id in services]):
            return {service_id: self.services[network + ":" + service_id] for service_id in services}

        if self.discovery_service is None:
            raise DiscoveryError(400, "Cannon fetch discovery service because it's None")

        required_also = [service_id for service_id in services if service_id not in self.services]

        try:
            response_services = await self.internal.get(
                self.discovery_service,
                "services/" + ",".join(required_also) + "/" + network, {},
                discover_service=False)

        except internal.InternalError as e:
            raise DiscoveryError(e.code, "Discovery error: {0}.".format(e.code))

        self.services.update({
            (network + ":" + service_id): service_location
            for service_id, service_location in response_services.items()
        })

        return {
            service_id: self.services[network + ":" + service_id]
            for service_id in services
        }

    async def get_service(self, service_id, network="internal"):
        record_id = network + ":" + service_id

        if record_id in self.services:
            return self.services[record_id]

        if self.discovery_service is None:
            raise DiscoveryError(400, "Cannon fetch discovery service because it's None")

        try:
            response = await self.internal.get(
                self.discovery_service,
                "service/" + service_id + "/" + network, {},
                use_json=False,
                discover_service=False)

        except internal.InternalError as e:
            if e.code == 404:
                raise DiscoveryError(404, "No such service")
            elif e.code == 502:
                raise DiscoveryError(502, "Service is down")
            else:
                raise DiscoveryError(e.code, "Discovery error: {0}.".format(e.body))

        self.services[record_id] = response

        return response

    def location(self):
        return self.discovery_service


class DiscoveryError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return self.message


cache = None


def init():
    global cache
    if cache is None:
        cache = Discovery()
