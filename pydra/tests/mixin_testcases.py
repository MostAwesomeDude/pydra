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

from pydra.tests.proxies import ModuleManagerProxy, RemoteProxy

class ModuleTestCaseMixIn():
    """
    Provides functions for asserting common things that happen within a pydra
    module.
    """
    
    def setUp(self):
        self.manager = ModuleManagerProxy()
        self.manager.testcase = self
        self.callbacks = []
    
    def assertCalled(self, remote, function, *args, **kwargs):
        return RemoteProxy.assertCalled(remote, self, function, *args, **kwargs)
    
    def callback(self, results, key='callback', *args, **kwargs):
        """ generic function for use as a callback. records that it was called """
        self.callbacks.append(key)
    
    def assertCallback(self, key):
        self.assert_(key in self.callbacks, 'callback key (%s) was not found' % key)
