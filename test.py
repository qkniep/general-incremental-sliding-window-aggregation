#!/usr/bin/python
# -*- coding: utf-8 -*-
"""."""

from datetime import timedelta
import random
from threading import Thread
import time

from pubsub import pub

from operators import Count, Max
from ra import ReactiveAggregator


class InputStreamGenerator(Thread):
    """A thread that periodically emits events from an event generator function.
    These events are published into a pubsub topic, for other threads to subscribe to.
    """

    def __init__(self, topic, event_generator):
        """Creates a new event stream based on a publishing topic and event generator function."""
        super().__init__()
        self.topic = topic
        self.generate_new_event = event_generator

    def run(self):  # called by Thread.start()
        """The thread runs for as long as the generator emits new events.
        After publishing an event the thread sleeps for 100 ms.
        """
        for event in self.generate_new_event():
            pub.sendMessage(self.topic, event=event)
            time.sleep(0.1)


def random_event():
    """Yields an infinite stream of random independent events."""
    while True:
        yield ({
            'id': random.randint(0, 1000),
            'value': random.randint(0, 1000),
            'hash': random.getrandbits(128),
            'category': random.randint(0, 10),
        }, time.time())


def incremental_event():
    """Yields an infinite stream of events with monotonically increasing values."""
    value = 0
    while True:
        value += random.randint(0, 5)
        yield ({
            'id': random.randint(0, 1000),
            'value': value,
            'hash': random.getrandbits(128),
            'category': random.randint(0, 10),
        }, time.time())


def simple_callback(event):
    """."""
    print('[ECHO]:', event)


if __name__ == '__main__':
    pub.subscribe(simple_callback, 'base')
    # operator = ArgMax(arg='id', max_over='value')
    operator = Max(max_over='value')
    ra = ReactiveAggregator(timedelta(seconds=10), timedelta(seconds=1), 'base', operator)
    ra.run()
    # InputStreamGenerator('base', incremental_event).start()
    pub.sendMessage('windowInsert', event={'value': 0})
    pub.sendMessage('windowInsert', event={'value': 1})
    pub.sendMessage('windowInsert', event={'value': 2})
    pub.sendMessage('windowInsert', event={'value': 3})
    pub.sendMessage('windowInsert', event={'value': 4})
    pub.sendMessage('windowInsert', event={'value': 5})
    pub.sendMessage('windowInsert', event={'value': 6})
    pub.sendMessage('windowInsert', event={'value': 7})
    pub.sendMessage('windowTrigger')
    print(ra.tree.data)
    pub.sendMessage('windowEvict', event={'value': 2})
    pub.sendMessage('windowEvict', event={'value': 3})
    pub.sendMessage('windowEvict', event={'value': 4})
    pub.sendMessage('windowEvict', event={'value': 5})
    pub.sendMessage('windowTrigger')
    print(ra.tree.data)
    pub.sendMessage('windowInsert', event={'value': 8})
    pub.sendMessage('windowInsert', event={'value': 9})
    pub.sendMessage('windowInsert', event={'value': 10})
    pub.sendMessage('windowTrigger')
    print(ra.tree.data)
