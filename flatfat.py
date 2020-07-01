# -*- coding: utf-8 -*-
"""."""


class FlatFAT:
    """A tree data structure for fast aggregation."""

    def __init__(self, values, operator):
        """Create a new FlatFAT containing values, using operator to combine partial aggregates."""
        self._operator = operator
        self._data = [None] * (len(values)*2-1)
        self.update(list(enumerate(values)))

    def update(self, changes):
        """Performs changes to the leaves and updates intermediate values as needed.

        Args:
            changes -- A list of leaf changes of the form (leaf_num, new_value).
        """
        for pos, value in changes:
            self._data[self._leaf(pos)] = value
        dirty = {_parent(self._leaf(pos)) for pos, _ in changes}
        while dirty:
            next_level_dirty = set()
            for i in dirty:
                self._data[i] = self.combine(_left(i), _right(i))
                if i != 0:
                    next_level_dirty.add(_parent(i))
            dirty = next_level_dirty

    def aggregate(self, front, back):
        """Returns the current partial aggregate over the whole tree."""
        if front < back:
            return self._data[0]
        return self._combine_values(self.suffix(front), self.prefix(back))

    def prefix(self, index):
        """Calculates the partial aggregate of all leaves until index (inclusive)."""
        node = self._leaf(index)
        agg = self._data[node]
        while node != 0:
            parent_node = _parent(node)
            if node == _right(parent_node):
                agg = self._combine_values(self._data[_left(parent_node)], agg)
            node = parent_node
        return agg

    def suffix(self, index):
        """Calculates the partial aggregate of all leaves starting from index (inclusive)."""
        node = self._leaf(index)
        agg = self._data[node]
        while node != 0:
            parent_node = _parent(node)
            if node == _left(parent_node):
                agg = self._combine_values(agg, self._data[_right(parent_node)])
            node = parent_node
        return agg

    def resize(self, front, new_capacity):
        """Resizes the data structure to a new capacity (number of possible leaves)."""
        index_changes = [-1] * max(new_capacity, self.capacity())

        leaves, indices = self._not_none_leaves(front)
        self._data = [None] * (new_capacity*2-1)
        self.update(list(enumerate(leaves)))

        for i, index in enumerate(indices):
            index_changes[index] = i
        return index_changes

    def compact(self, front):
        """Fills holes of None values by shifting leaves to the left."""
        leaves, indices = self._not_none_leaves(front)
        changes = [((front+i)%self.capacity(), v) for i, v in enumerate(leaves)]
        changes += [((front+len(changes)+i) % self.capacity(), None)
                    for i in range(self.capacity()-len(changes))]
        self.update(changes)

        index_changes = [-1] * self.capacity()
        for i, index in enumerate(indices):
            index_changes[index] = (front+i)%self.capacity()
        return index_changes

    def _not_none_leaves(self, front):
        """Returns: List of all leaves that are not None starting from front, wrapping indices."""
        leaves = []
        old_indices = []
        for i in range(self.capacity()):
            leaf = (front + i) % self.capacity()
            index = self._leaf(leaf)
            value = self._data[index]
            if value is not None:
                leaves.append(value)
                old_indices.append(leaf)
        return leaves, old_indices

    def combine(self, index1, index2):
        """Combines two partial aggregates in the data structure.

        Returns:
            The combined partial aggregate value.
        """
        agg1 = self._data[index1]
        agg2 = self._data[index2]
        return self._combine_values(agg1, agg2)

    def _combine_values(self, agg1, agg2):
        """Combines two partial aggregates using the combine function from the operator.
        Short circuits the binary operator: If on of agg1, agg2 is None the other is returned.
        """
        if agg1 is None:
            return agg2
        if agg2 is None:
            return agg1
        return self._operator.combine(agg1, agg2)

    def capacity(self):
        """Returns: The number of leaves the tree structure can hold."""
        return len(self._data) // 2 + 1

    def _leaf(self, leaf_pos):
        """Converts position in leaves array to index in the tree array."""
        return len(self._data) // 2 + leaf_pos


def _parent(i):
    """Returns the parent's index of the node with index i."""
    return (i-1)//2


def _left(i):
    """Returns root index of the left subtree below node with index i."""
    return 2*i+1


def _right(i):
    """Returns root index of the right subtree below node with index i."""
    return 2*i+2
