#!/usr/bin/env python

import unittest

from pydra.util.test import env_args
from pydra.cluster.tasks.datasource.backend import SQLBackend
from pydra.cluster.tasks.datasource.selector import SQLSelector


class SQLiteTest(unittest.TestCase):

    def setUp(self):
        self.sb = SQLBackend("sqlite", ":memory:")

    def test_connect(self):
        self.assertTrue(self.sb.connected)

    def test_reuse(self):
        self.assertTrue(self.sb.connected)
        self.sb.disconnect()
        self.assertFalse(self.sb.connected)
        self.sb.connect()
        self.assertTrue(self.sb.connected)


class SQLBackendTest(unittest.TestCase):

    """
    A base class for backend sanity checks.

    The backend configuration tuple is read from a given environment variable,
    something like PYDRA_TEST_BACKEND='host:user:pass:database'. If this isn't
    defined, tests are skipped (or fail, if skipping is not available).
    """

    database = ("sqlite", "sqlite3")
    env_args = env_args("PYDRA_TEST_SQLITE", (":memory:",))

    def setUp(self):

        class TestBackend(SQLBackend):
            backends = {}
        try:
            name, module = self.database
            TestBackend.backends[name] = __import__(module)
        except ImportError:
            self.skipTest("Couldn't import: %s" % module)

        if self.env_args is None:
            self.skipTest("No test database.")

        self.l = [('caerphilly', 0), ('lancashire', 0), ('quark', 1)]
        self.b = TestBackend(name, *self.env_args)

        create = "CREATE TABLE TEST (CHEESE TEXT, STOCKS INT)"
        insert = "INSERT INTO TEST VALUES (%s,%s)" % (self.marker,self.marker)

        cursor = self.b.handle.cursor()
        cursor.execute(create)
        cursor.executemany(insert, self.l)
        self.b.handle.commit()

    def tearDown(self):

        remove = "DROP TABLE TEST"

        cursor = self.b.handle.cursor()
        cursor.execute(remove)
        self.b.handle.commit()

    def skipTest(self, reason):

        try:
            unittest.TestCase.skipTest(self, reason)
        except AttributeError:
            self.fail(reason)

    @property
    def marker(self):

        style = self.b.dbapi.paramstyle
        return ("?" if style == "qmark" else "%s")

    def test_select_basic(self):

        select = "SELECT * FROM TEST"

        selector = SQLSelector(self.b, select)
        selected = sorted([r for r in selector])
        self.assertEqual(self.l, selected)

    def test_select_param(self):

        select = "SELECT * FROM TEST WHERE STOCKS=%s" % (self.marker,)

        selector = SQLSelector(self.b, select, 0)
        selected = sorted([r for r in selector])
        self.assertEqual(self.l[:2], selected)


class MySQLdbRealTest(SQLBackendTest):
    database = ("mysql", "MySQLdb")
    env_args = env_args("PYDRA_TEST_MYSQL")

class OurSQLRealTest(SQLBackendTest):
    database = ("mysql", "oursql")
    env_args = env_args("PYDRA_TEST_MYSQL")

class PostgreSQLRealTest(SQLBackendTest):
    database = ("postgres", "psycopg2")
    env_args = env_args("PYDRA_TEST_PGSQL")


try:
    from pydra.cluster.tasks.datasource.tokyo.tests.backend import *
except ImportError:
    pass


if __name__ == "__main__":
    unittest.main()
