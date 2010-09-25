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

from datetime import datetime, timedelta
import time
import unittest

from pydra.tests import setup_test_environment
setup_test_environment()

from pydra.tests.cluster.module.test_module_manager import TestAPI, Bar


class InterfaceModuleTestCase(unittest.TestCase):
    
    def test_get_interface_name(self):
        """
        Tests generating interface name from different input
        
        Verifies:
            * name from function
            * name from attribute
            * name included in params
        """
        module = Bar()
        api = TestAPI()
        
        name = api._interface_name(module, module.foo)
        self.assertEqual('foo', name)
        
        name = api._interface_name(module, 'xoo')
        self.assertEqual('xoo', name)
        
        name = api._interface_name(module, module.foo, name='TEST_NAME')
        self.assertEqual('TEST_NAME', name)
        
        name = api._interface_name(module, 'xoo', name='TEST_NAME')
        self.assertEqual('TEST_NAME', name)
    
    def test_register_function(self):
        """
        Registering an interface
        
        Verifies:
            * registering a function
            * deregistering interface
        """
        module = Bar()
        api = TestAPI()
        
        api.register_interface(module, module.foo)
        self.assert_('foo' in api._registered_interfaces)
        api.deregister_interface(module, module.foo)
        self.assertFalse('foo' in api._registered_interfaces)
    
    def test_register_function_alternate_name(self):
        """
        Registering an interface
        
        Verifies:
            * registering a function using an alternate name
            * deregistering interface
        """
        module = Bar()
        api = TestAPI()
        
        api.register_interface(module, module.foo, name='TEST_NAME')
        self.assert_('TEST_NAME' in api._registered_interfaces)
        self.assertFalse('foo' in api._registered_interfaces)
        api.deregister_interface(module, module.foo, name='TEST_NAME')
        self.assertFalse('TEST_NAME' in api._registered_interfaces, api._registered_interfaces)
    
    def test_register_attribute(self):
        """
        Registering an interface
        
        Verifies:
            * registering an attribute
            * deregistering interface
        """
        module = Bar()
        api = TestAPI()
        
        api.register_interface(module, 'xoo')
        self.assert_('xoo' in api._registered_interfaces)
        api.deregister_interface(module, 'xoo')
        self.assertFalse('xoo' in api._registered_interfaces)
    
    def test_register_attribute_alternate_name(self):
        """
        Registering an interface
        
        Verifies:
            * registering an attribute with an alternate name
            * deregistering interface
        """
        module = Bar()
        api = TestAPI()
        
        api.register_interface(module, module.foo, name='TEST_NAME')
        self.assert_('TEST_NAME' in api._registered_interfaces)
        api.deregister_interface(module, module.foo, name='TEST_NAME')
        self.assertFalse('TEST_NAME' in api._registered_interfaces)
    
    def test_replace_interface(self):
        """
        Registering interface with existing name
        
        Verifies:
            * existing interface is replaced
        """
        module = Bar()
        api = TestAPI()
        api.register_interface(module, module.foo)
        
        # replace with named function
        api.register_interface(module, module.bar, name='foo')
        self.assert_('foo' in api._registered_interfaces)
        self.assertEqual(module.bar, api._registered_interfaces['foo'])
        
        # replace with function
        api.register_interface(module, module.foo)
        self.assert_('foo' in api._registered_interfaces)
        self.assertEqual(module.foo, api._registered_interfaces['foo'])
    
    
    def wrap_interface(self):
        """
        Wraps the interface in an implementation specific class
        
        Verifies:
            * interface is just returned, by default this does nothing
        """
        api = TestAPI()
        module = Bar()
        wrapped = api.wrap_interface(module.foo)
        self.assertEqual(wrapped, module.foo)

class InterfaceModuleAuthenticationTestCase(unittest.TestCase):
    
    def setUp(self):
        self.api = TestAPI(key_size=4096, key='./keys')
    
    def tearDown(self):
        import os
        
        if os.path.exists('./keys'):
            pass
    
    def test_clean_sessions(self):
        """
        removes expired sessions
        
        Verifies:
            * expired sessions are removed
            * non_expired sessions are not
        """
        api = TestAPI()
        
        session0 = {'expire':datetime.now()-timedelta(1)}
        session1 = {'expire':datetime.now()+timedelta(0,0,0,100)}
        session2 = {'expire':datetime.now()+timedelta(1)}
        api.sessions = {0:session0, 1:session1, 2:session2}
        
        # expire expired sessions
        api._clean_sessions()
        self.assertFalse(0 in api.sessions)
        self.assert_(1 in api.sessions)
        self.assert_(2 in api.sessions)
        
        # wait for second session to expire
        time.sleep(.15)
        api._clean_sessions()
        self.assertFalse(1 in api.sessions, api.sessions)
        self.assert_(2 in api.sessions)
    
    def test_authenticate(self):
        """
        Tests authentication request
        
        Verifies:
            * session has challenge stored
            * challenge is returned
        """
        api = self.api
        session = {'expire':datetime.now()+timedelta(1), 'challenge':None, 'auth':False}
        api.sessions = {0:session}
        
        challenge = api.authenticate(0)
        self.assert_(challenge, 'Challenge should not be none')
        self.assert_(session['challenge'])
        self.assertFalse(session['auth'])
    
    def test_authenticate_invalid_session(self):
        """
        Tests authenticating with an invalid session
        
        Verifies:
            * None is returned
        """
        api = self.api
        challenge = api.authenticate('INVALID_SESSION_ID')
        self.assertEqual(challenge, None)
    
    def test_challenge_response(self):
        """
        Test respond to challenge
        
        Verifies:
            * True is returned
            * auth set to True
            * challenge is destroyed
        """
        api = self.api
        session = {'expire':datetime.now()+timedelta(1), 'challenge':None, 'auth':False}
        api.sessions = {0:session}
        
        challenge = api.authenticate(0)
        self.assert_(challenge, 'Challenge should not be none')
        
        result = api.challenge_response(0, session['challenge'])
        
        self.assertEqual(True, result)
        self.assertEqual(True, session['auth'])
        self.assertEqual(None, session['challenge'])
    
    def test_challenge_response_invalid(self):
        """
        Test respond to challenge with invalid response
        
        Verifies:
            * auth is set to False
            * False is returned
            * challenge is destroyed
        """
        api = self.api
        session = {'expire':datetime.now()+timedelta(1), 'challenge':None, 'auth':False}
        api.sessions = {0:session}
        
        challenge = api.authenticate(0)
        self.assert_(challenge, 'Challenge should not be none')
        
        result = api.challenge_response(0, 'INVALID_RESPONSE')
        
        self.assertEqual(False, result)
        self.assertEqual(False, session['auth'])
        self.assertEqual(None, session['challenge'])
    
    def test_challenge_response_no_challenge(self):
        """
        Tests trying to respond before there is no challenge
        
        Verifies:
            * False returned
            * auth is set to False
            * challenge remains none
        """
        api = self.api
        session = {'expire':datetime.now()+timedelta(1), 'challenge':None, 'auth':False}
        api.sessions = {0:session}
        
        result = api.challenge_response(0, 'PREEMPTIVE_RESPONSE')
        self.assertEqual(False, result)
        self.assertEqual(False, session['auth'])
        self.assertEqual(None, session['challenge'])
