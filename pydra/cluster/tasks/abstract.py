"""
    Copyright 2009 Oregon State University

    This file is part of Pydra.

    Pydra is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    Pydra is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with Pydra.  If not, see <http://www.gnu.org/licenses/>.
"""

class AbstractMethod (object):
    """Defines a class to create abstract methods

    @example:
        class Foo:
            foo = AbstractMethod('foo')
    """
    def __init__(self, func):
        """Constructor

        @params func: name of the function (used when raising an
            exception).
        @type func: str
        """
        self._function = func

    def __get__(self, obj, type):
        """Get callable object

        @returns An instance of AbstractMethodHelper.

        This trickery is needed to get the name of the class for which
        an abstract method was requested, otherwise it would be
        sufficient to include a __call__ method in the AbstractMethod
        class itself.
        """
        return self.AbstractMethodHelper(self._function, type)

    class AbstractMethodHelper (object):
        """Abstract method helper class

        An AbstractMethodHelper instance is a callable object that
        represents an abstract method.
        """
        def __init__(self, func, cls):
            self._function = func
            self._class = cls

        def __call__(self, *args, **kwargs):
            """Call abstract method

            Raises a TypeError, because abstract methods can not be
            called.
            """
            raise TypeError('Abstract method `' + self._class.__name__ \
                            + '.' + self._function + '\' called')


class Metaclass (type):
    def __init__(cls, name, bases, *args, **kwargs):
        """Configure a new class

        @param cls: Class object
        @param name: Name of the class
        @param bases: All base classes for cls
        """
        super(Metaclass, cls).__init__(cls, name, bases, *args, **kwargs)

        # Detach cls.new() from class Metaclass, and make it a method
        # of cls.
        cls.__new__ = staticmethod(cls.new)

        # Find all abstract methods, and assign the resulting list to
        # cls.__abstractmethods__, so we can read that variable when a
        # request for allocation (__new__) is done.
        abstractmethods = []
        ancestors = list(cls.__mro__)
        ancestors.reverse()  # Start with __builtin__.object
        for ancestor in ancestors:
            for clsname, clst in ancestor.__dict__.items():
                if isinstance(clst, AbstractMethod):
                    abstractmethods.append(clsname)
                else:
                    if clsname in abstractmethods:
                        abstractmethods.remove(clsname)

        abstractmethods.sort()
        setattr(cls, '__abstractmethods__', abstractmethods)

    def new(self, cls):
        """Allocator for class cls

        @param self: Class object for which an instance should be
            created.

        @param cls: Same as self.
        """
        if len(cls.__abstractmethods__):
            raise NotImplementedError('Can\'t instantiate class `' + \
                                      cls.__name__ + '\';\n' + \
                                      'Abstract methods: ' + \
                                      ", ".join(cls.__abstractmethods__))

        return object.__new__(self)
6