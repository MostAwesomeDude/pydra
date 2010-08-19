database_names = {
    "sqlite": ("sqlite3", "pysqlite2.dbapi2",),
    "sqlite2": ("pysqlite2.dbapi2",),
    "sqlite3": ("sqlite3",),
    "mysql": ("oursql", "MySQLdb",),
    "oursql": ("oursql",),
    "postgres": ("psycopg2",),
}

database_args = {
    "sqlite"    : ("database",),
    "sqlite2"   : ("database",),
    "sqlite3"   : ("database",),
    "mysql"     : ("host", "user", "passwd", "db", "port"),
    "postgres"  : ("host", "user", "password", "database", "port"),
}

databases = {}  # will map names to first available module from database_names


for name, modules in database_names.items():
    for module in modules:
        try:
            databases[name] = __import__(module, fromlist=["bogus"])
        except ImportError:
            print "Warning: Couldn't import %s!" % module
        else:
            break
    if name not in databases:
        print "Warning: Disabling support for %s databases." % name


class SQLBackend(object):
    """
    Backend for interfacing with DBAPI-compliant SQL databases.
    """

    backends = databases.copy()
    handle = None

    def __init__(self, db_name, *args):
        """
        Initialize a backend object for the given SQL database.

        Passed *args are paired up with keywords depending on the requested database
        module, where None is treated like the default value (the related kwarg pair
        is not passed to .connect() at all).
        """

        if db_name in self.backends:
            self.dbapi = self.backends[db_name]
            argzip = zip(database_args[db_name], args)
        else:
            raise ValueError, "Database %s not supported" % db_name

        self.kwargs = dict((k,v) for (k,v) in argzip if v is not None)
        self.connect()

    def __del__(self):
        self.disconnect()

    def connect(self, *args, **kwargs):
        """
        Open a database connection.

        SQLBackend can only have one connection open per instance.
        Arguments given at .init are used by default.
        """

        if not self.handle:
            argnew = self.kwargs.copy()
            argnew.update(kwargs)
            self.handle = self.dbapi.connect(*args, **argnew)

    def disconnect(self):
        """
        Disconnect from the current database, if connected.
        """

        if self.handle:
            self.handle.close()
        self.handle = None

    @property
    def connected(self):
        """
        Is this instance currently connected?
        """

        return bool(self.handle)


try:
    from pydra.cluster.tasks.datasource.tokyo.backend import *
except ImportError:
    pass
