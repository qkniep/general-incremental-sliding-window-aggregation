# -*- coding: utf-8 -*-
"""."""


class FlatFAT:
    """."""

    def __init__(self, values, operator):
        """."""
        self.operator = operator
        self.data = [None] * (len(values)*2-1)
        self.update(list(enumerate(values)))

    def update(self, changes):
        """Performs changes to the leaves and updates intermediate values as needed."""
        for pos, value in changes:
            self.data[self.leaf(pos)] = value
        dirty = {parent(self.leaf(pos)) for pos, _ in changes}
        while dirty:
            next_level_dirty = set()
            for i in dirty:
                self.data[i] = self.combine(left(i), right(i))
                if i != 0:
                    next_level_dirty.add(parent(i))
            dirty = next_level_dirty

    def aggregate(self):
        """Returns the current root-level partial aggregate."""
        return self.data[0]

    def resize(self, new_capacity):
        """."""
        old_leaves = self.data[self.leaf(0):]
        self.data = [None] * (new_capacity*2-1)
        self.update(list(enumerate(old_leaves)))

    def compact(self):
        """."""
        # changes = []
        # for i in range(leaf(0), len(self.data)-1):
        #     if self.data[i] is None:

    def combine(self, index1, index2):
        """."""
        agg1 = self.data[index1]
        agg2 = self.data[index2]
        if agg1 is None:
            return agg2
        if agg2 is None:
            return agg1
        return self.operator.combine(agg1, agg2)

    def capacity(self):
        """Returns the number of leaves the tree structure can hold."""
        return len(self.data) // 2 + 1

    def leaf(self, leaf_pos):
        """Converts position in leaves array to index in the tree array."""
        return len(self.data) // 2 + leaf_pos


def parent(i):
    """Returns the parent's index of the node with index i."""
    return (i-1)//2


def left(i):
    """Returns root index of the left subtree below node with index i."""
    return 2*i+1


def right(i):
    """Returns root index of the right subtree below node with index i."""
    return 2*i+2
