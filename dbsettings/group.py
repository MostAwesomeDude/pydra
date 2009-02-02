import sys

from dbsettings.models import Setting
from dbsettings.values import Value
from dbsettings.loading import register_setting

__all__ = ['Group']

class GroupBase(type):
    def __init__(cls, name, bases, attrs):
        if not bases or bases == (object,):
            return
        attrs.pop('__module__', None)
        attrs.pop('__doc__', None)
        for attribute_name, attr in attrs.items():
            if not isinstance(attr, Value):
                raise TypeError('The type of %s (%s) is not a valid Value.' % (attribute_name, attr.__class__.__name__))
            cls.add_to_class(attribute_name, attr)

def install_permission(cls, permission):
    if permission not in cls._meta.permissions:
        # Add a permission for the setting editor
        try:
            cls._meta.permissions.append(permission)
        except AttributeError:
            # Permissions were supplied as a tuple, so preserve that
            cls._meta.permissions = tuple(cls._meta.permissions + (permission,))

class GroupDescriptor(object):
    def __init__(self, group, attribute_name):
        self.group = group
        self.attribute_name = attribute_name

    def __get__(self, instance=None, type=None):
        if instance != None:
            raise AttributeError, "%r is not accessible from %s instances." % (self.attribute_name, type.__name__)
        return self.group

class Group(object):
    __metaclass__ = GroupBase

    def __new__(cls, copy=True):
        # If not otherwise provided, set the module to where it was executed
        if '__module__' in cls.__dict__:
            module_name = cls.__dict__['__module__']
        else:
            module_name = sys._getframe(1).f_globals['__name__']

        attrs = [(k, v) for (k, v) in cls.__dict__.items() if isinstance(v, Value)]
        if copy:
            attrs = [(k, v.copy()) for (k, v) in attrs]
        attrs.sort(lambda a, b: cmp(a[1], b[1]))

        for key, attr in attrs:
            attr.creation_counter = Value.creation_counter
            Value.creation_counter += 1
            register_setting(attr)

        attr_dict = dict(attrs + [('__module__', module_name)])

        # A new class is created so descriptors work properly
        # object.__new__ is necessary here to avoid recursion
        group = object.__new__(type('Group', (cls,), attr_dict))
        group._settings = attrs

        from django.contrib.auth.models import Permission

        return group

    def contribute_to_class(self, cls, name):
        # Override module_name and class_name of all registered settings
        for attr in self.__class__.__dict__.values():
            if isinstance(attr, Value):
                attr.module_name = cls.__module__
                attr.class_name = cls.__name__
                register_setting(attr)

        # Create permission for editing settings on the model
        permission = (
            'can_edit_%s_settings' % cls.__name__.lower(),
            'Can edit %s settings' % cls._meta.verbose_name_raw,
        )
        if permission not in cls._meta.permissions:
            # Add a permission for the setting editor
            try:
                cls._meta.permissions.append(permission)
            except AttributeError:
                # Permissions were supplied as a tuple, so preserve that
                cls._meta.permissions = tuple(cls._meta.permissions + (permission,))

        # Finally, place the attribute on the class
        setattr(cls, name, GroupDescriptor(self, name))

    def add_to_class(cls, attribute_name, value):
        value.contribute_to_class(cls, attribute_name)
    add_to_class = classmethod(add_to_class)

    def __add__(self, other):
        if not isinstance(other, Group):
            raise NotImplementedError('Groups may only be added to other groups.')

        attrs = dict(self._settings + other._settings)
        attrs['__module__'] = sys._getframe(1).f_globals['__name__']
        return type('Group', (Group,), attrs)(copy=False)

    def __iter__(self):
        for attribute_name, setting in self._settings:
            yield attribute_name, getattr(self, attribute_name)

    def keys(self):
        return [k for (k, v) in self]

    def values(self):
        return [v for (k, v) in self]