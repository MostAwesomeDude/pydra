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

import unittest

from pydra.cluster.module import ModuleManager, Module, InterfaceModule
from pydra.tests.proxies import CallProxy


class Foo(Module):
    """ modules for testing """
    
    def __init__(self):
        self.foo = CallProxy(None, False)
        self.bar = CallProxy(None, False)
        self.xoo = 123
        self.zoo = 'abc'


class Bar(Module):
    """ module for testing """
    
    def __init__(self, param=None):
        self.param = param
        self.xoo = 123
        self.zoo = 'abc'
        
        self._listeners={'SIGNAL0':self.foo}
        self._remotes=[('REMOTE0',self.foo)]
        self._interfaces=[self.foo]
        self._services=[self.foo]
    
    def foo(self, *args, **kwargs):
        return 'foo return'
    
    def bar(self, *args, **kwargs):
        return 'bar return'


class Xoo(Foo):
    """ module for testing """
    pass


class Zoo(Bar):
    """ module for testing """
    pass


class TestAPI(InterfaceModule):
    pass


class CustomModuleManager(ModuleManager):
    def __init__(self, modules=[]):
        self.modules = [Xoo, Zoo()]
        super(CustomModuleManager, self).__init__(modules)


class ModuleManagerTestCase(unittest.TestCase):
    
    def test_register_module(self):
        """
        Registering and deregistering modules
        
        Verifies:
            * only specified modules are added and removed
            * listeners, remotes, interfaces, interface_modules, services are
              registered and deregistered
        """
        manager = ModuleManager()
        module = Bar()
        api = TestAPI()
        
        # verify everything is registered
        manager.register_module(module)
        manager.register_module(api)
        self.assert_(module in manager._modules)
        self.assert_('REMOTE0' in manager._remotes, manager._remotes)
        self.assert_('foo' in manager._remotes['REMOTE0'])
        self.assert_('SIGNAL0' in manager._listeners)
        self.assert_(module.foo in manager._listeners['SIGNAL0'])
        self.assert_((module, module.foo) in manager._interfaces)
        self.assert_(module.foo in manager._services)
        self.assert_(api in manager._interface_modules)
        
        # verify everything is deregistered
        manager.deregister_module(module)
        manager.deregister_module(api)
        self.assertFalse(module in manager._modules)
        self.assertFalse('REMOTE0' in manager._remotes, manager._remotes)
        self.assertFalse('SIGNAL0' in manager._listeners)
        self.assertFalse((module, module.foo) in manager._interfaces)
        self.assertFalse(module.foo in manager._services)
        self.assertFalse(api in manager._interface_modules)
    
    def test_register_listener(self):
        """
        Registering and deregistering signal listeners
        
        Verifies:
            * only specified functions are added
            * removing the last function for a signal removes the signal
        """
        manager = ModuleManager()
        module = Foo()
        manager.register_listener('SIGNAL', module.foo)
        manager.register_listener('SIGNAL', module.bar)
        manager.register_listener('SIGNAL2', module.xoo)
        self.assert_('SIGNAL' in manager._listeners)
        self.assert_(module.foo in manager._listeners['SIGNAL'])
        self.assert_(module.bar in manager._listeners['SIGNAL'])
        self.assert_('SIGNAL2' in manager._listeners)
        self.assert_(module.xoo in manager._listeners['SIGNAL2'])
        
        manager.deregister_listener('SIGNAL', module.foo)
        self.assert_('SIGNAL' in manager._listeners)
        self.assert_(module.bar in manager._listeners['SIGNAL'])
        self.assert_('SIGNAL2' in manager._listeners)
        self.assert_(module.xoo in manager._listeners['SIGNAL2'])
        
        manager.deregister_listener('SIGNAL', module.bar)
        manager.deregister_listener('SIGNAL2', module.xoo)
        self.assertFalse('SIGNAL' in manager._listeners, "signal should be removed when no listeners")
        self.assertFalse('SIGNAL2' in manager._listeners, "signal should be removed when no listeners")
    
    def test_register_interface(self):
        """
        Registering and deregistering attributes and functions exposed to the
        API
        
        Verifies:
            * only specified functions and attributes are added and removed
            * functions are added to all registered interfaces
            * name can be given as a param
            * functions can be replaced
        """
        manager = ModuleManager()
        api0 = TestAPI()
        api1 = TestAPI()
        module = Bar()
        
        manager.register_interface_module(api0)
        manager.register_interface_module(api1)
        
        interfaces = (
            (module, module.foo),
            (module, module.bar),
            (module, 'xoo'),
            (module, 'zoo')
        )
        
        interface_names = ('foo','bar','xoo','zoo')
        
        for interface in interfaces:
            manager.register_interface(*interface)
        
        for interface in interfaces:
            self.assert_(interface in manager._interfaces)
        
        for name in interface_names:
            self.assert_(name in api0._registered_interfaces)
            self.assert_(name in api1._registered_interfaces)
        
        for interface in interfaces:
            manager.deregister_interface(*interface)
        
        for interface in interfaces:
            self.assertFalse(interface in manager._interfaces)
        
        self.assertFalse(api0._registered_interfaces)
        self.assertFalse(api1._registered_interfaces)
    
    def test_register_interface_module(self):
        """
        Registering and deregistering API interfaces
        
        Verifies:
            * only specified API interfaces are added and removed
            * existing attributes and functions are added to the new API
        """
        manager = ModuleManager()
        api0 = TestAPI()
        api1 = TestAPI()
        module = Bar()
        
        interfaces = (
            (module, module.foo),
            (module, module.bar),
            (module, 'xoo'),
            (module, 'zoo')
        )
        interface_names = ('foo','bar','xoo','zoo')
        
        for interface in interfaces:
            manager.register_interface(*interface)
        
        manager.register_interface_module(api0)
        manager.register_interface_module(api1)
        
        self.assert_(api0 in manager._interface_modules)
        for name in interface_names:
            self.assert_(name in api0._registered_interfaces)
            self.assert_(name in api1._registered_interfaces)
    
    def test_register_remote(self):
        """
        Registering and deregistering remotely exposed attributes and functions
        
        Verifies:
            * only specified remote is added and removed
            * attributes are wrapped
            * functions are mappable
            * methods can have secure param
            * remote is removed if no more mapped functions exist
        """
        manager = ModuleManager()
        module = Bar()
        manager.register_remote(module, 'REMOTE0', module.foo)
        manager.register_remote(module, 'REMOTE0', (module.bar, True))
        manager.register_remote(module, 'REMOTE1', 'xoo')
        manager.register_remote(module, 'REMOTE1', ('zoo', True))
        
        self.assert_('REMOTE0' in manager._remotes)
        self.assert_('REMOTE1' in manager._remotes)
        
        remotes0 = manager._remotes['REMOTE0']
        remotes1 = manager._remotes['REMOTE1']
        self.assert_('foo' in remotes0)
        self.assert_('bar' in remotes0)
        self.assert_('xoo' in remotes1)
        self.assert_('zoo' in remotes1)
        
        manager.deregister_remote(module, 'REMOTE0', module.foo)
        manager.deregister_remote(module, 'REMOTE0', (module.bar, True))
        manager.deregister_remote(module, 'REMOTE1', 'xoo')
        manager.deregister_remote(module, 'REMOTE1', ('zoo', True))
        
        self.assertFalse('REMOTE0' in manager._remotes)
        self.assertFalse('REMOTE1' in manager._remotes)
        self.assertFalse(manager._remotes)
    
    def test_register_service(self):
        """
        Registering and deregistering twisted services
        
        Verifies:
            * only specified service is added and removed
        """
        manager = ModuleManager()
        module = Bar()
        
        manager.register_service(module.foo)
        self.assert_(module.foo in manager._services)
        
        manager.register_service(module.bar)
        self.assert_(module.foo in manager._services)
        self.assert_(module.bar in manager._services)
        
        services = manager.get_services()
        self.assert_('foo return' in services)
        self.assert_('bar return' in services)
        
        manager.deregister_service(module.bar)
        self.assert_(module.foo in manager._services)
        self.assertFalse(module.bar in manager._services)
        
        manager.deregister_service(module.foo)
        self.assertFalse(module.foo in manager._services)
    
    def test_init(self):
        """
        Tests creating an empty ModuleManager
        """
        ModuleManager()
    
    def test_init_configured_manager(self):
        """
        Tests creating a subclass of ModuleManager that is preconfigured with
        modules
        
        Verifies
            * all modules are registered
            * modules may be classes or instances
        """
        manager = CustomModuleManager()
        names = [module.__class__.__name__ for module in manager._modules]
        self.assert_('Xoo' in names)
        self.assert_('Zoo' in names)
        
        # all registered objects are modules
        for module in manager._modules:
            self.assert_(isinstance(module,(Module,)), "registered object is not a module: %s" % module)
    
    def test_init_runtime_modules(self):
        """
        Tests creating a ModuleManager passing modules to the constructor
        
        Verifies:
            * Modules can be added to vanilla manager
            * Modules can be added to a subclass of ModuleManager configured
              with modules
            * parameters can be passed into the construction of the module
            * runtime modules may be Module classes or instances
        """
        manager = CustomModuleManager([Foo, Bar()])
        names = [module.__class__.__name__ for module in manager._modules]
        
        # all modules are registered
        self.assert_('Foo' in names, names)
        self.assert_('Bar' in names, names)
        self.assert_('Xoo' in names, names)
        self.assert_('Zoo' in names, names)
        
        # all registered objects are modules
        for module in manager._modules:
            self.assert_(isinstance(module,(Module,)), "registered object is not a module: %s" % module)
    
    def test_emit(self):
        """
        Tests signals
        
        Verifies:
            * emitting registered signal calls mapped functions
            * emitting unregistered signal doesn't raise an exception
        """
        manager = ModuleManager()
        module0 = Foo()
        module1 = Xoo()
        manager.register_listener('FOO', module0.foo)
        manager.register_listener('FOO', module1.foo)
        manager.register_listener('BAR', module0.bar)
        
        args = (1, 2, 3)
        kwargs = {'a':1, 'b':2}
        manager.emit_signal('FOO')
        manager.emit_signal('BAR', *args, **kwargs)
        
        module0.foo.assertCalled(self)
        module1.foo.assertCalled(self)
        module0.bar.assertCalled(self, *args, **kwargs)