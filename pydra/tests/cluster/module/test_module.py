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

from pydra.cluster.module import Module, ModuleManager
from pydra.tests.proxies import ModuleManagerProxy


class TestModule(Module):
    _shared = ['foo']


class TestModule2(Module):
    _shared = ['foo']


class ModuleTestCase(unittest.TestCase):
    
    def test_shared_attributes(self):
        """
        tests shared attributes:
        
        Verifies:
            * setting and getting attribute from different modules
        """
        manager = ModuleManager()
        module0 = TestModule()
        module1 = TestModule2()
        
        manager.register(module0)
        manager.register(module1)
        
        module0.foo = 123
        self.assertEqual(module0.foo, 123)
        self.assertEqual(module1.foo, 123)
        
        module1.foo = 456
        self.assertEqual(module0.foo, 456)
        self.assertEqual(module1.foo, 456)
    
    def test_register(self):
        """
        Tests register callback
        
        Verifies:
            * manager is set and unset
        """
        manager = ModuleManager()
        module = TestModule()
        
        module._register(manager)
        self.assertEqual(module.manager, manager)
        
        module._deregister()
        self.assertEqual(module.manager, None)
    
    def test_emit(self):
        """
        Tests emitting a signal
        """
        manager = ModuleManagerProxy(testcase=self, emit=False)
        module = TestModule()
        manager.register(module)
        
        module.emit('SIGNAL!')
        manager.assertEmitted('SIGNAL!')