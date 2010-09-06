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
from pydra.cluster.node import master_connection_manager
from pydra.tests.cluster.module.test_module_manager import TestAPI
from pydra.tests.mixin_testcases import ModuleTestCaseMixIn
from pydra.tests.proxies import RemoteProxy


class MasterConnectionManagerMixin():
    def setUp(self):
        self.master_connection_manager = master_connection_manager.MasterConnectionManager()
        self.manager.register(self.master_connection_manager)

class MasterConnectionManager(unittest.TestCase, ModuleTestCaseMixIn, MasterConnectionManagerMixin):
    
    def setUp(self):
        self.tearDown()
        ModuleTestCaseMixIn.setUp(self)
        MasterConnectionManagerMixin.setUp(self)
    
    def tearDown(self):
        pass

    def test_trivial(self):
        """
        Trivial test that just instantiates class
        """
        module = master_connection_manager.MasterConnectionManager()
    
    def test_register(self):
        """
        Tests registering the module with a manager
        """
        manager = ModuleManager()
        module = master_connection_manager.MasterConnectionManager()
        api = TestAPI()
        manager.register(api)
        manager.register(module)
        self.assert_(module in manager._modules)

    def test_master_authenticated(self):
        """
        The master is authenticating
        
        Verifies:
            * MASTER_CONNECTED is emitted.
        """
        mcm = self.master_connection_manager
        master = RemoteProxy('master:0')
        mcm.master_authenticated(master)
        
        self.manager.assertEmitted('MASTER_CONNECTED')