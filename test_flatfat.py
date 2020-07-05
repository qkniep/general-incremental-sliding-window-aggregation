#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""Unit Testing for the FlatFAT data structure."""

import unittest

import flatfat
import operators


class TestFlatfatDataStructure(unittest.TestCase):

    op = operators.Count()


    def test_resize(self):
        capacity = 16

        tree = flatfat.FlatFAT([None] * capacity, self.op)
        tree.update([(0, 0), (3, 3), (7, 7)])
        index_changes = tree.resize(3, capacity * 2)
        self.assertEqual(tree._data[tree._leaf(0):], [3, 7, 0] + [None] * (2*capacity-3))
        self.assertEqual(index_changes, [2, -1, -1, 0, -1, -1, -1, 1] + [-1] * (32-8))
        index_changes = tree.resize(1, capacity)
        self.assertEqual(tree._data[tree._leaf(0):], [7, 0, 3] + [None] * (capacity-3))
        self.assertEqual(index_changes, [2, 0, 1] + [-1] * (32-3))


    def test_compact(self):
        tree = flatfat.FlatFAT([None] * 4, self.op)
        tree.update([(0, 0), (2, 2)])
        tree.compact(0)
        self.assertEqual(tree._data[tree._leaf(0):], [0, 2, None, None])

        tree = flatfat.FlatFAT([None] * 4, self.op)
        tree.update([(0, 0), (2, 2)])
        tree.compact(2)
        self.assertEqual(tree._data[tree._leaf(0):], [None, None, 2, 0])

        tree = flatfat.FlatFAT([None] * 8, self.op)
        tree.update([(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)])
        tree.compact(4)
        self.assertEqual(tree._data[tree._leaf(0):], [3, None, None, None, 4, 0, 1, 2])


if __name__ == '__main__':
    unittest.main()
