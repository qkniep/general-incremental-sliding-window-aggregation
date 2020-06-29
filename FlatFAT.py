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

    # TODO: remove this (see ra.py)
    def find_leaf(self, value):
        """."""
        for i in range(self.capacity()):
            if self.data[self.leaf(i)] == value:
                return i
        return -1

    def aggregate(self):
        """Returns the current root-level partial aggregate."""
        return self.data[0]

    def prefix(self, index):
        """."""
        node = self.leaf(index)
        agg = self.data[node]
        while node != 0:
            p = parent(node)
            if node == right(p):
                agg = self.combine_values(self.data[left(p)], agg)
            node = p
        return agg

    def suffix(self, index):
        """."""
        node = self.leaf(index)
        agg = self.data[node]
        while node != 0:
            p = parent(node)
            if node == left(p):
                agg = self.combine_values(agg, self.data[right(p)])
            node = p
        return agg

    def resize(self, new_capacity):
        """."""
        old_leaves = self.data[self.leaf(0):]
        self.data = [None] * (new_capacity*2-1)
        self.update(list(enumerate(old_leaves)))

    def compact(self):
        """."""
        changes = []
        shift_count = 0
        for i in range(self.capacity()):
            if self.data[self.leaf(i)] is None:
                shift_count += 1
            else:
                changes.append((i-shift_count, self.data[self.leaf(i)]))
        last_value_index = self.capacity() - shift_count
        changes += [(last_value_index+i, None) for i in range(shift_count)]
        self.update(changes)

    def combine(self, index1, index2):
        """."""
        agg1 = self.data[index1]
        agg2 = self.data[index2]
        return self.combine_values(agg1, agg2)

    def combine_values(self, agg1, agg2):
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
