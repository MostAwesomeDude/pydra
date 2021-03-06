#!/usr/bin/env python

import os.path
import unittest

from pydra.cluster.tasks.datasource.selector import DirSelector, FileSelector, SQLSelector

# Odds are very good that you don't want to touch this. Both trial and
# unittest have path quirks, and this seems to correctly handle both of them.
cheesedir = os.path.abspath(os.path.join(os.path.dirname(__file__),
    "cheeses"))

class DirSelectorCheeseTest(unittest.TestCase):

    def setUp(self):

        self.ds = DirSelector(cheesedir)

    def test_trivial(self):

        pass

    def test_length(self):

        self.assertEqual(len(self.ds), 2)

class FileSelectorTest(unittest.TestCase):

    def setUp(self):

        self.fs = FileSelector(os.path.join(cheesedir, "cheddar.txt"))

    def test_trivial(self):

        pass

    def test_handle(self):

        handle = self.fs.handle
        self.assertTrue(len(handle))

        handle2 = self.fs.handle
        self.assertEqual(handle, handle2)

class SQLSelectorTest(unittest.TestCase):

    def setUp(self):
        #TODO: test the selector without using a real backend(?)
        from pydra.cluster.tasks.datasource.backend import SQLBackend

        self.l = [('leicester',), ('quark',), (1,), (0,), (None,)]

        self.backend = SQLBackend("sqlite3", ":memory:")
        self.backend.connect()

        db = self.backend.handle
        db.execute("CREATE TABLE CHEESES (NAME)")
        db.executemany("INSERT INTO CHEESES VALUES (?)", self.l)
        db.commit()

        self.selector = SQLSelector(self.backend, "SELECT * FROM CHEESES")

    def test_trivial(self):

        pass

    def test_select(self):

        query = "SELECT * FROM CHEESES"
        selector = SQLSelector(self.backend, query)
        self.assertEqual(self.l, [k for k in selector])

    def test_args(self):

        query = "SELECT * FROM CHEESES WHERE NAME IN (?, ?)"
        selector = SQLSelector(self.backend, query, "quark", "leicester")
        self.assertEqual(self.l[:2], [k for k in selector])

    def test_kwargs(self):

        query = "SELECT * FROM CHEESES WHERE NAME IN (:cheeseA, :cheeseB)"
        selector = SQLSelector(self.backend, query, cheeseA="quark", cheeseB="leicester")
        self.assertEqual(self.l[:2], [k for k in selector])

    def test_syntax(self):

        query = "SELECT * FROM CHEESES WHERE NAME IN (?, ?)"
        self.assertRaises(ValueError, SQLSelector, self.backend, query, "quark", "leicester", it="ni")

if __name__ == "__main__":
    unittest.main()
