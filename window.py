# -*- coding: utf-8 -*-
"""Windowing library for stream processing, only implementing a sliding window
with time-based eviction and trigger policies.
Based on: http://yoksis.bilkent.edu.tr/pdf/files/10.1002-spe.2194.pdf
"""

from datetime import datetime
from queue import PriorityQueue
import queue
import random
from threading import Thread

from pubsub import pub


class SlidingTimeWindow(Thread):
    """Maintains a sliding window with time-based eviction and triggering.

    Insertions are done via an event handler on the input stream.
    Evictions and triggering of the window are done on the thread represented by this class.
    """

    def __init__(self, size, slide, in_stream):
        """Creates a new sliding window over events from in_stream.

        Args:
            size: the timedelta until an event is evicted from the window
            slide: the timedelta between the window's trigger events
        """
        super().__init__()  # Thread constructor
        self.size = size
        self.slide = slide
        self._window = PriorityQueue()
        self._last_trigger = datetime.now()
        pub.subscribe(self._on_incoming_event, in_stream)

    def run(self):  # called by Thread.start()
        """Window thread main loop.

        Checks whether the window is due for triggering.
        Checks the oldest event and evicts it from the window if needed.
        """
        while True:
            current_time = datetime.now()
            if current_time - self._last_trigger > self.slide:
                pub.sendMessage('windowTrigger')
                self._last_trigger = current_time
            try:
                timestamp, event_id = self._window.get_nowait()
                if current_time - datetime.fromtimestamp(timestamp) > self.size:
                    pub.sendMessage('windowEvict', window_event_id=event_id)
                else:
                    self._window.put((timestamp, event_id))
            except queue.Empty:
                pass

    def _on_incoming_event(self, event):
        """Handles an event coming from the input stream."""
        event_tuple, timestamp = event
        event_id = random.getrandbits(128)
        pub.sendMessage('windowInsert', event_tuple=event_tuple, window_event_id=event_id)
        self._window.put((timestamp, event_id))
