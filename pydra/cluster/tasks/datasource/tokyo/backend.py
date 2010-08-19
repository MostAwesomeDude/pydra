import pyrant


class TokyoBackend(object):
    """
    Backend for interfacing with Tokyo Cabinet databases (through Tyrant).
    """

    handle = None

    def __init__(self, host='127.0.0.1', port=1978, separator=None, literal=False):
        """
        Initialize the backend.

        Separator can be used to transparently put / retrieve lists from the database.
        Literal toggles encoding records as Unicode - if it's set, we pass them as-is.
        """

        self.kwargs = locals()
        del self.kwargs['self']

        for k,v in self.kwargs.items():
            if v is None:
                del self.kwargs[k]

        self.connect()

    def __del__(self):
        self.disconnect()

    def connect(self, *args, **kwargs):
        """
        Open a database connection.
        """

        if not self.handle:
            argnew = self.kwargs.copy()
            argnew.update(kwargs)
            self.handle = pyrant.Tyrant(*args, **argnew)

    def disconnect(self):
        """
        Disconnect from the current database, if connected.
        """

        self.handle = None

    @property
    def connected(self):
        """
        Is this instance currently connected?
        """

        return bool(self.handle)

    def __iter__(self):
        """
        Iterate over all records in the database.

        With this, you can use the backend as a working datasource without a selector.
        """

        return self.handle.iteritems()

