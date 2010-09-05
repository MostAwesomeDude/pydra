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

from pydra.cluster.module import ModuleReferenceable
from pydra.tests.proxies import CallProxy

class ModuleReferenceableTest(unittest.TestCase):
    
    
    def test_module_referenceable(self):
        """
        Tests a module referenceable
        
        Verifies:
            * mapped methods are callable
            * non-mapped methods raise Error
            * args and kwargs are passed in
        """
        foo = CallProxy(None, False)
        bar = CallProxy(None, False)
        xoo = CallProxy(None, False)
        
        remotes = {'foo':foo, 'bar':bar, 'xoo':xoo}
        referenceable = ModuleReferenceable(remotes)
        
        # verify calls work
        referenceable.remote_foo()
        foo.assertCalled(self)
        bar.assertNotCalled(self)
        referenceable.remote_bar()
        bar.assertCalled(self)
        
        # verify args work
        args = (1, 2, 3)
        kwargs = {'a':1, 'b':2}
        referenceable.remote_xoo(*args, **kwargs)
        xoo.assertCalled(self, *args, **kwargs)
        
        def not_mapped():
            referenceable.remote_not_mapped()
        
        # verify non-mapped function raises error
        self.assertRaises(KeyError, not_mapped)