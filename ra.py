# -*- coding: utf-8 -*-
"""Reactive Aggregator operator implementation for stream processing.
Based on: http://www.vldb.org/pvldb/vol8/p702-tangwongsan.pdf
"""

from threading import Lock
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

    INITIAL_CAPACITY = 4


    def __init__(self, window_size, window_slide, in_stream, operator):
        """Creates new window with parameters: size=window_size, slide=window_slide."""
        super().__init__()
        self.window = SlidingTimeWindow(window_size, window_slide, in_stream)
        pub.subscribe(self.on_insert, 'windowInsert')
        pub.subscribe(self.on_evict, 'windowEvict')
        pub.subscribe(self.on_trigger, 'windowTrigger')

        self.operator = operator

        self.tree = FlatFAT([None] * self.INITIAL_CAPACITY, self.operator)
        self.tree_changes = []
        self.window_to_tree = {}
        self.buffer_front = 0
        self.buffer_back = 0
        self.num_tuples = 0

        self.lock = Lock()  # TODO: think more about thread synchronization with pubsub

    def run(self):
        self.window.start()

    def on_insert(self, event_tuple, window_event_id):
        """Handles a window insert event by adding the event to the end of the circular buffer.
        Increases the size of the circular buffer if needed.
        """
        with self.lock:
            self.handle_full_buffer()
            agg = self.operator.lift(event_tuple)
            self.tree_changes.append((self.buffer_back, agg))
            self.window_to_tree[window_event_id] = self.buffer_back
            self.buffer_back = (self.buffer_back + 1) % self.tree.capacity()
            self.num_tuples += 1

    def on_evict(self, window_event_id):
        """Handles a window evict event by removing the event from the circular buffer.
        Shrinks the circular buffer if appropriate.
        """
        with self.lock:
            if self.num_tuples < self.tree.capacity() // 4:
                self._apply_tree_changes()
                index_changes = self.tree.resize(self.buffer_front, self.tree.capacity() // 2)
                self._update_indices(index_changes)

            index = self.window_to_tree[window_event_id]
            del self.window_to_tree[window_event_id]
            self.tree_changes.append((index, None))
            if index == self.buffer_front:
                self.buffer_front = (self.buffer_front + 1) % self.tree.capacity()
            self.num_tuples -= 1

    def on_trigger(self):
        """Handles a window trigger event by computing the current aggregate."""
        with self.lock:
            self._apply_tree_changes()
            agg = self.tree.aggregate(self.buffer_front, self.buffer_back)
            print('[RA,%s]: %s' % (self.operator.NAME, self.operator.lower(agg)))

    def handle_full_buffer(self):
        """Checks whether the circular buffer is full and makes space if it is.

        Uses either FlatFAT.compact() to remove 'holes' in the circular buffer,
        or FlatFAT.resize() to double the allocated capacity,
        depending on whether compact would free a significant amount of space.
        """
        if self.num_tuples == 0 or self.buffer_front != self.buffer_back:
            return  # buffer is not full

        self._apply_tree_changes()
        if self.num_tuples <= self.tree.capacity() * 3 // 4:
            index_changes = self.tree.compact(self.buffer_front)
        else:
            index_changes = self.tree.resize(self.buffer_front, self.tree.capacity() * 2)
            self.buffer_front = 0
        self.buffer_back = (self.buffer_front + self.num_tuples) % self.tree.capacity()
        self._update_indices(index_changes)

    def _apply_tree_changes(self):
        self.tree.update(self.tree_changes)
        self.tree_changes = []

    def _update_indices(self, index_changes):
        for (wid, old_index) in self.window_to_tree.items():
            self.window_to_tree[wid] = index_changes[old_index]
        assert -1 not in self.window_to_tree.values()
