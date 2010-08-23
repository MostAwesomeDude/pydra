#!/usr/bin/env python

import unittest

from pydra.util.test import env_args
from pydra.cluster.tasks.datasource.tokyo.backend import TokyoBackend
from pydra.cluster.tasks.datasource.tokyo.selector import TokyoSelector


class TokyoBackendTest(unittest.TestCase):

    """
    Tokyo Cabinet sanity checks.

    The backend configuration tuple is read from a given environment variable,
    something like PYDRA_TEST_TOKYO='host:port'. The test server should not have
    any data, and it should expose a table database.
    """

    env_args = env_args("PYDRA_TEST_TOKYO")

    def setUp(self):

        if self.env_args is None:
            self.skipTest("No test database.")

        self.l = [('a1', {'n':'one',          'i':'1'}),
                  ('a2', {'n':'two',          'i':'2'}),
                  ('b1', {'n':'two_one',      'i':'2', 'j':'1'}),
                  ('c1', {'n':'two_one_four', 'i':'2', 'j':'1', 'k':'4'}),
                  ('c2', {'n':'four_one_two', 'i':'4', 'j':'1', 'k':'2'})]

        args = list(self.env_args)
        if len(args) > 1 and args[1] is not None:
            args[1] = int(args[1])

        self.b = TokyoBackend(*args)

        self.assertEqual(self.b.handle.keys(), [])
        self.b.handle.multi_set(self.l)

    def tearDown(self):
        self.b.handle.clear()

    def skipTest(self, reason):

        try:
            unittest.TestCase.skipTest(self, reason)
        except AttributeError:
            self.fail(reason)

    def run_query(self, query):
        selector = TokyoSelector(self.b, *query)
        selected = sorted([r for r in selector])
        return selected

    def test_basic(self):

        query = []
        self.assertEqual(self.run_query(query), self.l)

    def test_prefix(self):

        query = [(None, 'startswith', 'a')]
        self.assertEqual(self.run_query(query), self.l[:2])

    def test_startswith(self):

        query = [('n', 'startswith', 'two')]
        self.assertEqual(self.run_query(query), self.l[1:4])

    def test_exists_t(self):

        query = [('j', 'exists', True)]
        self.assertEqual(self.run_query(query), self.l[2:])

    def test_exists_f(self):

        query = [('k', 'exists', False)]
        self.assertEqual(self.run_query(query), self.l[:3])

    def test_exists_n(self):

        query = [('k', 'not exists', True)]
        self.assertEqual(self.run_query(query), self.l[:3])

    def test_gt(self):

        query = [('i', 'gt', 1)]
        self.assertEqual(self.run_query(query), self.l[1:])

    def test_lt(self):

        query = [('j', 'lt', 2)]
        self.assertEqual(self.run_query(query), self.l[2:])

    def test_lt_not(self):

        query = [('j', 'not lt', 2)]
        self.assertEqual(self.run_query(query), self.l[:2])


if __name__ == "__main__":
    unittest.main()
