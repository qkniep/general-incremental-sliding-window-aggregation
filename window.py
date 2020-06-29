# -*- coding: utf-8 -*-
"""Windowing library for stream processing, only implementing a sliding window
with time-based eviction and trigger policies.
Based on: http://yoksis.bilkent.edu.tr/pdf/files/10.1002-spe.2194.pdf
"""

from datetime import datetime
from queue import PriorityQueue
import queue
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
        self.window = PriorityQueue()
        self.size = size
        self.slide = slide
        self.last_trigger = datetime.now()
        pub.subscribe(self.on_incoming_event, in_stream)

    def run(self):  # called by Thread.start()
        """Window thread main loop.

        Checks whether the window is due for triggering.
        Checks the oldest event and evicts it from the window if needed.
        """
        while True:
            current_time = datetime.now()
            if current_time - self.last_trigger > self.slide:
                pub.sendMessage('windowTrigger')
                self.last_trigger = current_time
            try:
                event = self.window.get_nowait()
                if current_time - datetime.fromtimestamp(event[0]) > self.size:
                    pub.sendMessage('windowEvict', event=event[1])
                else:
                    self.window.put(event)
            except queue.Empty:
                pass

    def on_incoming_event(self, event):
        """Handles an event coming from the input stream."""
        pub.sendMessage('windowInsert', event=event[0])
        self.window.put((event[1], event[0]))
