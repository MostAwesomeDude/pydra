"""
Datasource API main classes.

These are all Pydra core API and may be called by users.

Datasource descriptions are simple objects full of information used to
instantiate datasources.
"""

from pydra.cluster.tasks.datasource.slicer import IterSlicer

def iterable(i):
    """
    Test for iterability.

    :return: Whether `i` is iterable.
    """

    try:
        iter(i)
        return True
    except TypeError:
        return False

class DataSource(object):
    """
    A description of an external data source.

    Datasource objects are technically only descriptions of the external data
    they reflect. They are fairly lightweight and easy to serialize.
    """

    def __init__(self, *args):
        """
        Initialize and validate the datasource description.
        """

        self.validate(args)

    def delayable(self):
        """
        Test whether a datasource description can have its unpacking delayed
        until the last minute.

        Some datasources, like SQL databases and distributed filesystems, will
        definitely want to be delayable; other datasources, like simple
        iterables, will definitely be slower under this scheme.
        """

        return False

    def validate(self, ds):
        """
        Given a potential datasource description tuple, initialize this
        datasource description appropriately.

        Datasource descriptions should generally look like this tuple:

        >>> (BreakfastSelector, spam, eggs, spam, spam, bacon, eggs, spam)

        The general idiom is to have a selector or slicer first, and then the
        arguments to be passed to that selector.

        This function can and will make guesses in order to always be valid.

        :Parameters:
            ds : tuple
                The datasource description, or some object vaguely similar to
                a datasource description, or perhaps something having nothing
                at all to do with datasources
        """

        # This is really not a bad guess, apparently.
        self.selector = IterSlicer

        if len(ds) == 0:
            # Yeah, yeah, you're cute.
            self.args = [None]
            return
        elif len(ds) == 1:
            # Did not read the docs, or passed in an iterable without
            # unpacking; i.e. DS(args) instead of DS(*args).
            if iterable(ds[0]) and len(ds[0]) > 1:
                # Odds are good that they just forgot to unpack. Help them
                # out a bit.
                ds = ds[0]
            elif callable(ds[0]):
                # I can see myself getting lots of flak for this. On one hand,
                # this provides incredible amounts of power to application
                # authors wanting to pass in completely custom datasource
                # slicers and selectors. On the other hand, it could be a
                # security problem, and also a compromise of our serialization
                # routines.
                #
                # This might turn out to be a massive non-problem if we create
                # a common parent class for all selectors, like we have for
                # all slicers. ~ C.
                self.selector = ds[0]
                self.args = tuple()
                return
            else:
                self.args = [ds[0]]
                return

        # XXX deal with nested stuff too plz
        if callable(ds[0]):
            # Excellent.
            self.selector = ds[0]
            self.args = ds[1:]
        else:
            self.args = ds

    def unpack(self):
        """
        Instantiate this datasource.

        :returns: Generator yielding slices of data
        """

        for s in self.selector(*self.args):
            yield s
