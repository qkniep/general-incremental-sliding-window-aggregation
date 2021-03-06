#!/usr/bin/python
# -*- coding: utf-8 -*-
"""."""

from datetime import timedelta
import random
from threading import Thread
import time

from pubsub import pub

from operators import Count
from ra import ReactiveAggregator


class InputStreamGenerator(Thread):
    """A thread that periodically emits events from an event generator function.
    These events are published into a pubsub topic, for other threads to subscribe to.
    """

    EVENTS_PER_SECOND = 10000

    def __init__(self, topic, event_generator):
        """Creates a new event stream based on a publishing topic and event generator function."""
        super().__init__()
        self.topic = topic
        self.generate_new_event = event_generator
        self.last_event = time.time()
        self.starttime = time.time()

    def run(self):  # called by Thread.start()
        """The thread runs for as long as the generator emits new events."""
        for event in self.generate_new_event():
            current_time = time.time()
            if current_time - self.last_event >= 1 / self.EVENTS_PER_SECOND:
                self.last_event = time.time()
                pub.sendMessage(self.topic, event=event)


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
        }, time.time() + random.uniform(0, 0.5))


def echo_callback(event):
    """Echoes an event to stdout."""
    print('[ECHO]:', event)


if __name__ == '__main__':
    # pub.subscribe(echo_callback, 'base')
    # operator = ArgMax(arg='id', max_over='value')
    operator = Count()
    ra = ReactiveAggregator(timedelta(seconds=20), timedelta(seconds=5), 'base', operator)
    ra.run()
    InputStreamGenerator('base', incremental_event).start()
