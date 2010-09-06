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

from pydra.tests import setup_test_environment
setup_test_environment()

from pydra.cluster.module import ModuleManager
from pydra.cluster.controller.web.interface import deferred_response, \
    FunctionResource, InterfaceResource, TwistedWebInterface

class TwistedWebInterfaceTestCase(unittest.TestCase):
    
    def test_trivial(self):
        """
        Trivial test that just instantiates class
        """
        module = TwistedWebInterface()
    
    def test_register(self):
        """
        Tests registering the module with a manager
        """
        manager = ModuleManager()
        module = TwistedWebInterface()
        manager.register(module)
        self.assert_(module in manager._modules)
    
    
