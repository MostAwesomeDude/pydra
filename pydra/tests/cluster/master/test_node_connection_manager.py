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
from pydra.tests import setup_test_environment
setup_test_environment()

from pydra.cluster.module import ModuleManager
from pydra.cluster.master.node_connection_manager import NodeConnectionManager
from pydra.tests import django_testcase
from pydra.tests.cluster.module.test_module_manager import TestAPI

class NodeConnectionManagerTestCase(django_testcase.TestCase):
    
    def test_trivial(self):
        """
        Trivial test that just instantiates class
        """
        module = NodeConnectionManager()
    
    def test_register(self):
        """
        Tests registering the module with a manager
        """
        manager = ModuleManager()
        module = NodeConnectionManager()
        api = TestAPI()
        manager.register(api)
        manager.register(module)
        self.assert_(module in manager._modules)
