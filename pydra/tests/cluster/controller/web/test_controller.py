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
import simplejson
import unittest
import urllib2

from pydra.tests import setup_test_environment
setup_test_environment()

from pydra.cluster.auth.rsa_auth import load_crypto
from pydra.cluster.controller import ControllerException, \
        ControllerRemoteException, CONTROLLER_ERROR_NO_RSA_KEY, \
        CONTROLLER_ERROR_AUTH_FAIL
from pydra.cluster.controller.web.controller import WebControllerFunction, \
    WebController
from pydra.cluster.controller.web.interface import TwistedWebInterface
from pydra.tests.proxies import CallProxy


class Response():
        """ HTTPResponse Proxy class, needed since responses must have read() """
        def __init__(self, value):
            self.value = simplejson.dumps(value)
        
        def read(self):
            return self.value


class OpenerProxy():
    """
    Proxy class for urllib2 opener.  Using this class to avoid actually opening
    urls at any point.  self.open() is replaced with a CallProxy, and always
    returns a Response with value 1
    """
    
    def __init__(self, raise_=None):
        """
        :parameters:
            raise_: an exception that will always be raised
        """
        self.raise_ = raise_
        self.open = CallProxy(self.open)
    
    def open(self, url, data):
        if self.raise_:
            raise self.raise_
        return Response(1)


class WebControllerFunctionTestCase(unittest.TestCase):
    
    def setUp(self):
        load_crypto('./keys')
        self.api = WebController(key='./keys')
        self.api.opener = OpenerProxy()
        self.func = WebControllerFunction(self.api, 'method_name')
    
    def test_trivial(self):
        """
        Trivial test
        
        Verifies:
            * function is created
        """
        self.assert_(self.func)
    
    def test_make_url(self):
        """
        url for master api is constructed
        
        Verifies:
            * url matches format
        """
        url = self.func.make_url()
        self.assert_('http://localhost:18801/method_name')
    
    def test_call(self):
        """
        api call without args
        
        Verifies:
            * no exception thrown
            * response is as expected
        """
        response = self.func()
        self.assertEqual(1, response)
    
    def test_call_with_args(self):
        """
        API call with args and kwargs
        
        Verifies:
            * no exception thrown
            * response is as expected
        """
        args = [1,2,3]
        kwargs = dict(a=1, b=2, c=3)
        response = self.func(*args, **kwargs)
        self.assertEqual(1, response)
    
    def test_call_404(self):
        """
        API call for function that does not exist
        
        Verifies:
            * ControllerException is raised
        """
        error = urllib2.HTTPError(None, 404, None, None, None)
        self.api.opener.raise_=error
        self.assertRaises(ControllerException, self.func)
    
    def test_call_401(self):
        """
        API call for function that requires authorization
        
        Verifies:
            * authentication is triggered
        """
        error = urllib2.HTTPError(None, 401, None, None, None)
        self.api.opener.raise_=error
        self.func._authenticate = CallProxy(self.func._authenticate, False)
        self.func()
        self.func._authenticate.assertCalled(self)
    
    def test_call_500(self):
        """
        API call for function that has a serverside error
        
        Verifies:
            * ControllerRemoteException is raised
            * response incluse exception and stacktrace
        """
        exception = {'exception':'FakeException','traceback':'fake stack trace'}
        error = urllib2.HTTPError(None, 500, simplejson.dumps(exception), None, None)
        self.api.opener.raise_=error
        try:
            self.func()
            self.fail('function should have raised ControllerRemoteException')
        except ControllerRemoteException, e:
            self.assert_(e.error, 'exception name is not set')
            self.assert_(e.traceback, 'traceback is not set ')
    
    def test_call_http_error_other(self):
        """
        API call for function that has any other HTTPError besides the specific
        codes
        
        Verifies:
            *  ControllerException is raised
        """
        error = urllib2.HTTPError(None, None, None, None, None)
        self.api.opener.raise_=error
        self.assertRaises(ControllerException, self.func)
    
    def test_call_url_error(self):
        """
        API call for function that throws URLError
        
        Verifies:
            * ControllerException is raised
        """
        error = urllib2.URLError(None)
        self.api.opener.raise_=error
        self.assertRaises(ControllerException, self.func)
    
    def test_authenticate(self):
        """
        Tests authenticating with the remote server
        
        Verifies:
            * remote authentication functions are called
            * original function is called
        """
        
        # configure a TwistedWebInterface to proxy directly to by monkey
        # patching the methods directly onto the controller.  This lets us test
        # test interaction with the actual components.
        user_id = 'TEST_USER_ID'
        interface = TwistedWebInterface(key='./keys')
        interface.sessions[user_id] = {'auth':False, 'challenge':None}
        auth_proxy = CallProxy(interface.authenticate, user=user_id)
        resp_proxy = CallProxy(interface.challenge_response, user=user_id)
        self.api.authenticate = auth_proxy
        self.api.challenge_response = resp_proxy
        
        response = self.func._authenticate()
        auth_proxy.assertCalled(self)
        resp_proxy.assertCalled(self)
        self.assert_(response)
    
    def test_authenticate_no_key(self):
        self.api.priv_key = None
        try:
            self.func._authenticate()
            self.fail('function should have raised ControllerException')
        except ControllerException, e:
            self.assertEqual(e.code, CONTROLLER_ERROR_NO_RSA_KEY)
    
    def test_authenticate_bad_response(self):
        # configure a TwistedWebInterface to proxy directly to by monkey
        # patching the methods directly onto the controller.  This lets us test
        # test interaction with the actual components.
        user_id = 'TEST_USER_ID'
        interface = TwistedWebInterface(key='./keys')
        interface.sessions[user_id] = {'auth':False, 'challenge':None}
        auth_proxy = CallProxy(interface.authenticate, user=user_id)
        resp_proxy = CallProxy(interface.challenge_response, response=False, user=user_id)
        self.api.authenticate = auth_proxy
        self.api.challenge_response = resp_proxy
        
        try:
            self.func._authenticate()
            self.fail('function should have raised ControllerException')
        except ControllerException, e:
            self.assertEqual(e.code, CONTROLLER_ERROR_AUTH_FAIL)


class WebControllerTestCase(unittest.TestCase):
    
    def setUp(self):
        load_crypto('./keys')
    
    def test_trivial(self):
        WebController()
        WebController(host='locahost', port=18801, key='./keys')
    
    def test_get_attributes(self):
        """
        Tests getting attributes from a WebController Interface
        
        Verifies:
            * all attributes can be retrieved
        """
        api = WebController(key='./keys')
        self.assertEqual(api.host, 'localhost')
        self.assertEqual(api.port, 18801)
        self.assert_(api.pub_key)
        self.assert_(api.priv_key)
        self.assert_(api.opener)
    
    def test_get_web_function(self):
        """
        Tests getting a remote function.
        
        Verifies:
            * function is returned as WebControllerFunction
            * function is configured properlys
        """
        api = WebController()
        function = api.foo
        self.assert_(isinstance(function, (WebControllerFunction,)))
        self.assertEqual(function.key, 'foo')
        
    def test_get_cached_web_function(self):
        """
        Access the same function twice
        
        Verifies:
            * function instances are the same
        """
        api = WebController()
        function0 = api.foo
        function1 = api.foo
        self.assertEqual(function0, function1, 'second instance should have been cached')
