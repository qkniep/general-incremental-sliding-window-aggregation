# -*- coding: utf-8 -*-
"""Implementations of different aggregation operators.

These are implemented according to the Reactive Aggregator framework, i.e. they provide 3 methods:
lift(event) -> agg -- Returns the partial aggregate corresponding to an event.
combine(agg, agg) -> agg -- Returns the combination of two partial aggregates.
lower(agg) -> value -- Returns the final value from a partial aggregate.
"""


class Operator:
    """Abstract base class for aggregation operators."""

    def lift(self, event):
        """."""

    def combine(self, agg1, agg2):
        """."""

    def lower(self, agg):
        """."""


class Count(Operator):
    """Aggregate value is the number of events."""
    NAME = 'COUNT'

    def lift(self, event):
        """A single event has a count of 1."""
        return 1

    def combine(self, agg1, agg2):
        """Returns the aggregate of the two counts, i.e. their sum."""
        return agg1 + agg2

    def lower(self, agg):
        """The final value is simply the last running count."""
        return agg


class Max(Operator):
    """."""
    NAME = 'MAX'

    def __init__(self, max_over):
        self.max_over = max_over

    def lift(self, event):
        """."""
        return event[self.max_over]

    def combine(self, agg1, agg2):
        """."""
        return max(agg1, agg2)

    def lower(self, agg):
        """The final value is simply the last running max."""
        return agg


class Min(Operator):
    """."""
    NAME = 'MIN'

    def __init__(self, min_over):
        self.min_over = min_over

    def lift(self, event):
        """."""
        return event[self.min_over]

    def combine(self, agg1, agg2):
        """."""
        return min(agg1, agg2)

    def lower(self, agg):
        """The final value is simply the last running min."""
        return agg
