#!/usr/bin/env python

import unittest

from pydra.cluster.tasks.datasource.slicer import IterSlicer, CursorSlicer, CursorDumbSlicer, MapSlicer, LineSlicer

class _CursorDumb(object):
    
    def __init__(self, l):
        self.l = l
        self.i = 0
    
    def fetchone(self):
        i = self.i
        self.i += 1
        if len(self.l) > i:
            return self.l[i]
        else:
            return None

class _CursorSmart(_CursorDumb):
    
    def __iter__(self):
        return self
    
    def next(self):
        item = self.fetchone()
        if item is None:
            raise StopIteration
        else:
            return item

class IterSlicerTest(unittest.TestCase):

    def setUp(self):

        self.l = [1, 2, 3]
        self.slicer = IterSlicer(self.l)

    def test_trivial(self):

        self.assertEqual(self.l, [i for i in self.slicer])

class CursorSlicerDumbTest(IterSlicerTest):
    
    def setUp(self):
        
        self.l = [(1,2), (1,3), (4,2), (1,), (0,), (None,), ()]
        self.cursor = _CursorDumb(self.l)
        self.slicer = CursorSlicer(self.cursor)

class CursorSlicerSmartTest(IterSlicerTest):
    
    def setUp(self):
        
        self.l = [(1,2), (1,3), (4,2), (2,), (0,), (None,), ()]
        self.cursor = _CursorSmart(self.l)
        self.slicer = CursorSlicer(self.cursor)

class CursorSlicerRealTest(IterSlicerTest):
    
    def setUp(self):
        
        from pydra.cluster.tasks.datasource.backend import SQLBackend
        
        self.l = [('leicester',), ('quark',), (1,), (0,), (None,)]
        
        self.backend = SQLBackend("sqlite3", ":memory:")
        self.backend.connect()
        
        db = self.backend.handle
        db.execute("CREATE TABLE CHEESES (NAME)")
        db.executemany("INSERT INTO CHEESES VALUES (?)", self.l)
        db.commit()
        
        self.cursor = db.execute("SELECT * FROM CHEESES")
        self.slicer = CursorSlicer(self.cursor)

class MapSlicerTest(unittest.TestCase):

    def setUp(self):

        self.d = {1 : 2, 3 : 4}
        self.slicer = MapSlicer(self.d)

    def test_trivial(self):

        self.assertEqual(self.d.keys(), [k for k in self.slicer])

class LineSlicerTest(unittest.TestCase):

    def setUp(self):

        self.s = """
            Jackdaws love my big sphinx of quartz.
            The quick brown fox jumps over the lazy dog.
            Pack my box with five dozen liquor jugs.
            """
        self.slicer = LineSlicer(self.s)

    def test_trivial(self):

        self.assertEqual([51, 108, 161], [pos for pos in self.slicer])

    def test_state_slice(self):

        self.slicer.state = slice(50, 150)
        self.assertEqual([51, 108], [pos for pos in self.slicer])
        self.slicer.state = slice(100, 200)
        self.assertEqual([108, 161], [pos for pos in self.slicer])

    def test_getitem(self):

        ls = self.slicer[50:100]
        self.assertEqual([51, 108], [pos for pos in ls])

if __name__ == "__main__":
    unittest.main()
