
import tornado.ioloop


class Publisher(object):
    def __init__(self):
        self.listeners = {}

    def notify(self, event, *args, **kwargs):
        if event not in self.listeners:
            return

        for listener in self.listeners[event]:
            if hasattr(listener, event):
                tornado.ioloop.IOLoop.current().add_callback(getattr(listener, event), *args, **kwargs)

    def subscribe(self, events, listener):
        for event in events:
            if event in self.listeners:
                self.listeners[event].add(listener)
            else:
                listeners = set()
                self.listeners[event] = listeners
                listeners.add(listener)

    def unsubscribe(self, events, listener):
        for event in events:
            even_listeners = self.listeners.get(event, None)
            if even_listeners is not None:
                even_listeners.discard(listener)


class EventsSubscription(object):
    def __init__(self, publisher):
        self.publisher = publisher
        self.events = set()

    def add_events(self, events):
        self.events |= set(events)

    def remove_events(self, events):
        self.events -= set(events)
        return bool(self.events)


class Subscriber(object):
    def __init__(self, listener):
        self.subscriptions = {}
        self.listener = listener

    def subscribe(self, publisher, events):
        pub_id = id(publisher)
        subscription = self.subscriptions.get(pub_id, None)

        if subscription is None:
            subscription = EventsSubscription(publisher)
            self.subscriptions[pub_id] = subscription

        subscription.add_events(events)
        publisher.subscribe(events, self.listener)

    def unsubscribe(self, publisher, events):
        publisher.unsubscribe(list(events), self.listener)

        pub_id = id(publisher)
        subscription = self.subscriptions.get(pub_id, None)

        if subscription is not None:
            if not subscription.remove_events(events):
                self.subscriptions.pop(pub_id, None)

    def unsubscribe_all(self):
        for pub_id, subscription in self.subscriptions.items():
            publisher = subscription.publisher
            publisher.unsubscribe(list(subscription.events), self.listener)
