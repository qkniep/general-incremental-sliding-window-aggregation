# -*- coding: utf-8 -*-

event1 = {'id':(123),'bla':(345), 'year':(1995)}

event2 = {'id':(456),'bla':(334), 'year':(1996)}

class Operator:
    """Abstract base class for aggregation operators."""

    def lift(self, event):
        """."""

    def combine(self, agg1, agg2):
        """."""

    def lower(self, agg):
        """."""

class ArgMax(Operator):
  NAME = 'ARG_MAX'
  def lift(self,event):
    n_keys = ['id', 'bla']
    event_new = {n_key: event[n_key] for n_key in n_keys}
    return event_new

  def combine(self,agg1, agg2):
    if agg1['bla'] > agg2['bla']:
      return agg1
    elif agg1['bla'] < agg2['bla']:
      return agg2
    elif agg1['id'] > agg2['id']:
      return agg1
    else:
      return agg2

  def lower(self,agg):
    return agg['id']