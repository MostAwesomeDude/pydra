#!/usr/bin/env python

import itertools
import unittest

from pydra.cluster.tasks.datasource import DataSource
from pydra.cluster.tasks.datasource.slicer import IterSlicer

class DelayableTest(unittest.TestCase):

    def test_iterslicer(self):
        ds = DataSource(IterSlicer, range(5))
        self.assertFalse(ds.delayable)

class ValidateTest(unittest.TestCase):

    def test_none(self):
        ds = DataSource(None)
        self.assertEqual(ds.selector, IterSlicer)
        self.assertEqual(ds.args, [None])

    def test_string(self):
        s = "Make it so, Number One!"
        ds = DataSource(s)
        self.assertEqual(ds.selector, IterSlicer)
        self.assertEqual(ds.args, s)

    def test_iterslicer_tuple(self):
        ds = DataSource((IterSlicer, [1, 2, 3, 4, 5]))
        self.assertEqual(ds.selector, IterSlicer)
        self.assertEqual(ds.args, ([1, 2, 3, 4, 5],))

    def test_iterslicer_args(self):
        ds = DataSource(IterSlicer, [1, 2, 3, 4, 5])
        self.assertEqual(ds.selector, IterSlicer)
        self.assertEqual(ds.args, ([1, 2, 3, 4, 5],))

class UnpackTest(unittest.TestCase):

    def test_iterslicer(self):
        l = [chr(i) for i in range(255)]
        u = u"\u03c0 \u042f \u97f3 \u00e6 \u221e"
        s = "Aye aye, Cap'n."
        t = (True, False, None)
        x = xrange(10)

        for i in l, u, s, t, x:
            ds = DataSource(IterSlicer, i)
            for expected, unpacked in itertools.izip_longest(i, ds.unpack()):
                self.assertEqual(expected, unpacked)

if __name__ == "__main__":
    unittest.main()
