# -*- coding: utf-8 -*-
"""Implementations of different aggregation operators."""


class Operator:
    """Abstract base class for aggregation operators.

    These are implemented according to the Reactive Aggregator framework
    i.e. they provide 3 methods as described below.
    """

    def lift(self, event):
        """Should return the partial aggregate corresponding to an event."""

    def combine(self, agg1, agg2):
        """Should return the combination of two partial aggregates."""

    def lower(self, agg):
        """Should return the final value from a partial aggregate."""


class Count(Operator):
    """Aggregate value is the number of events."""

    NAME = 'COUNT'

    def lift(self, event):
        return 1

    def combine(self, agg1, agg2):
        return agg1 + agg2

    def lower(self, agg):
        return agg


class Max(Operator):
    """Aggregate value is the maximum value of one specified key over events.

    Attributes:
        max_over -- The key we want to maximize.
    """

    NAME = 'MAX'

    def __init__(self, max_over):
        self.max_over = max_over

    def lift(self, event):
        return event[self.max_over]

    def combine(self, agg1, agg2):
        return max(agg1, agg2)

    def lower(self, agg):
        return agg


class Min(Operator):
    """Aggregate value is the minimum value of one specified key over events.

    Attributes:
        min_over -- The key we want to minimize.
    """

    NAME = 'MIN'

    def __init__(self, min_over):
        self.min_over = min_over

    def lift(self, event):
        return event[self.min_over]

    def combine(self, agg1, agg2):
        return min(agg1, agg2)

    def lower(self, agg):
        return agg


class ArgMax(Operator):
    """Aggregate is the argument which maximizes the value of some key over events.

    Attributes:
        arg -- The key we want from the maximum element.
        max_over -- The key we want to maximize.
    """

    NAME = 'ARGMAX'

    def __init__(self, arg, max_over):
        self.arg = arg
        self.max_over = max_over

    def lift(self, event):
        return {'arg': event[self.arg], 'value': event[self.max_over]}

    def combine(self, agg1, agg2):
        if agg2['value'] > agg1['value']:
            return agg2
        return agg1

    def lower(self, agg):
        return agg['arg']
