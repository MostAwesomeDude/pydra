import pyrant.query


class TokyoSelector(object):
    """
    Selects rows from a Tokyo Cabinet database, based on the given query.

    An iterator for items is created.

    The conditions are specified through *args, using pyrant's keywords.
    For example, the folowing conditions are equivalent:
    [('column', 'between', (2,3))] and [('column', 'gt', 1), ('column', 'lte', 3)]

    The condition keyword can be given as 'not keyword' to negate it.

    Queries are only supported on table databases, except for a special condition
    in the form (None, 'startswith', 'foo'), which returns all records whose keys
    start with 'foo'.
    """

    delayable = True

    def __init__(self, db, *args):

        if hasattr(db, "handle"):
            self.handle = db.handle
        else:
            self.handle = db

        self.query = [TokyoCondition(*cond) for cond in args]

    @property
    def is_prefix(self):
        """Is the query a key prefix filter?"""

        if len(self.query) != 1:
            return False
        else:
            cond = self.query[0]

        if cond.name is not None:
            return False

        if cond.lookup != "startswith" or cond.negate:
            return False

        if not isinstance(cond.expr, (str, unicode)):
            return False

        return True

    def __iter__(self):

        prefix = self.is_prefix

        if hasattr(self.handle, "query") and not prefix:
            return iter(self.handle.query.filter(*self.query))

        if prefix:
            keys = self.handle.prefix_keys(self.query[0].expr)
            return iter(self.handle.multi_get(keys))
        else:
            raise ValueError, "only 'startswith' conditions work with non-table databases"


class TokyoCondition(pyrant.query.Condition):
    """
    Representation of a query condition. Maps lookups to protocol constants.

    This verions skips some of pyrant's automagic (for Pydra's convenience).
    """
    def __init__(self, name, lookup, expr, negate=False):

        # works with pyrant 0.6.4

        if lookup.startswith('not '):
            lookup = lookup[4:]
            negate = not negate

        self.name = name
        self.lookup = lookup
        self.expr = expr
        self.negate = negate
