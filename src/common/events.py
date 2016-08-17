
import tornado.ioloop


class Publisher(object):
    def __init__(self):
        self.listeners = {}

    def notify(self, event, *args, **kwargs):
        if event not in self.listeners:
            return

        for listener in self.listeners[event]:
            if hasattr(listener, event):
                tornado.ioloop.IOLoop.current().spawn_callback(getattr(listener, event), *args, **kwargs)

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
            if event in self.listeners:
                self.listeners[event].discard(listener)


class Subscriber(object):
    def __init__(self, listener):
        self.events = {}
        self.publishers = {}
        self.listener = listener

    def subscribe(self, publisher, events):
        pub_id = id(publisher)
        if pub_id not in self.events:
            pub_events = set()
            self.events[pub_id] = pub_events
            self.publishers[pub_id] = publisher
        else:
            pub_events = self.events[pub_id]

        pub_events |= set(events)
        publisher.subscribe(events, self.listener)

    def unsubscribe(self, publisher, events):
        publisher.unsubscribe(list(events), self.listener)

        pub_id = id(publisher)
        if pub_id in self.events:
            self.events[pub_id] -= set(events)

    def unsubscribe_all(self):
        for pub_id, events in self.events.iteritems():
            publisher = self.publishers[pub_id]
            publisher.unsubscribe(list(events), self.listener)
