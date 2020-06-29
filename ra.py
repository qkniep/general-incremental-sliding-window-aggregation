# -*- coding: utf-8 -*-
"""Reactive Aggregator operator implementation for stream processing.
Based on: http://www.vldb.org/pvldb/vol8/p702-tangwongsan.pdf
"""

from threading import Thread

from pubsub import pub

from flatfat import FlatFAT
from window import SlidingTimeWindow


class ReactiveAggregator(Thread):
    """

    Maintains a sliding window over the input stream,
    a FlatFAT data structure containing the partial aggregates.
    The leaves of FlatFAT are also accessed as a circular buffer.
    """

    def __init__(self, window_size, window_slide, in_stream, operator):
        """Creates new window with parameters: size=window_size, slide=window_slide
        """
        super().__init__()
        self.window = SlidingTimeWindow(window_size, window_slide, in_stream)
        pub.subscribe(self.on_insert, 'windowInsert')
        pub.subscribe(self.on_evict, 'windowEvict')
        pub.subscribe(self.on_trigger, 'windowTrigger')

        self.operator = operator

        self.tree = FlatFAT([None] * 2, self.operator)
        self.tree_changes = []
        self.buffer_front = 0
        self.buffer_back = 0
        self.num_tuples = 0

    def run(self):
        self.window.start()

    def on_insert(self, event):
        """Handles a window insert event by adding the event to the end of the circular buffer."""
        self.handle_full_buffer()
        agg = self.operator.lift(event)
        self.tree_changes.append((self.buffer_back, agg))
        self.buffer_back += 1
        self.num_tuples += 1
        self.buffer_back %= self.tree.capacity()

    def on_evict(self, event):  # TODO: change to support non-FIFO windows
        """Handles a window evict event by removing the event from the circular buffer."""
        if self.num_tuples < self.tree.capacity() // 4:
            self.tree.resize(self.tree.capacity() // 2, False)

        # TODO: maybe replace this with lookup table (window.event_id -> flatfat.index)
        '''
        index = self.tree.find_leaf(self.operator.lift(event))
        self.tree_changes.append((index, None))
        if index == self.buffer_front:
            self.buffer_front += 1
            self.buffer_front %= self.tree.capacity()
        self.num_tuples -= 1
        '''

    def on_trigger(self):
        """Handles a window trigger event by computing the current aggregate."""
        self.tree.update(self.tree_changes)
        self.tree_changes = []
        agg = self.tree.aggregate()
        print('[RA,%s]: %s' % (self.operator.NAME, self.operator.lower(agg)))

    def handle_full_buffer(self):
        """Checks whether the circular buffer is full and makes space if it is.

        Uses either FlatFAT.compact() to remove 'holes' in the circular buffer,
        or FlatFAT.resize() to double the allocated capacity,
        depending on whether compact would free a significant amount of space.
        """
        if self.num_tuples == 0 or self.buffer_front != self.buffer_back:
            return  # buffer is not full
        # if self.num_tuples <= self.tree.capacity() * 3 // 4:
        #    self.tree.compact()
        # else:
        self.buffer_back = self.tree.capacity()
        self.tree.resize(self.tree.capacity() * 2)
