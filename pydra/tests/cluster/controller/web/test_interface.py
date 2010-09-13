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
import StringIO
import unittest

from twisted.internet.defer import Deferred
from twisted.web import server
from twisted.web.error import NoResource

from pydra.tests import setup_test_environment
setup_test_environment()

from pydra.cluster.module import ModuleManager
from pydra.cluster.controller.web.interface import deferred_response, \
    FunctionResource, InterfaceResource, TwistedWebInterface
from pydra.tests import MuteStdout
from pydra.tests.cluster.module.test_module_manager import Bar, TestAPI
from pydra.tests.mixin_testcases import ModuleTestCaseMixIn
from pydra.tests.proxies import CallProxy


class SessionProxy():
    uid = 'SESSION_ID'


class HTTPRequestProxy():
    """ Proxy for twisted Http requests objects """
    def __init__(self, args={}):
        self.args = args
        self.session = SessionProxy()
        self.writer = StringIO.StringIO()
        # register proxy for finish method
        self.finish = CallProxy(None, False)
        self.headers = {}

    def write(self, str):
        self.writer.write(str)

    def getSession(self):
        return self.session

    def setResponseCode(self, code):
        self.response_code = code
        
    def setHeader(self, key, value):
        self.headers[key] = value


class TwistedWebInterfaceTestCaseMixin(ModuleTestCaseMixIn):
    """
    Mixin for creating and registering a TwistedWebInterface module
    """
    
    def setUp(self):
        ModuleTestCaseMixIn.setUp(self)
        self.twisted_web_interface = TwistedWebInterface()
        self.manager.register(self.twisted_web_interface)


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
        api = TestAPI()
        manager.register(api)
        manager.register(module)
        self.assert_(module in manager._modules)

    def test_deferred_response(self):
        """
        Tests callback for deferred responses
        
        Verifies:
            * json serialized response is sent
            * finish is called to send response to http client
        """
        request = HTTPRequestProxy()
        response = [1,2,3]
        deferred_response(response, request)
        request.finish.assertCalled(self)
        self.assert_(simplejson.loads(request.writer.getvalue()), response)

class FunctionResourceTestCase(unittest.TestCase, TwistedWebInterfaceTestCaseMixin):
    
    def setUp(self):
        TwistedWebInterfaceTestCaseMixin.setUp(self)
        self.return_list = CallProxy(self.return_list)
    
    def return_list(self, *args, **kwargs):
        """ example function that returns a list """
        return [1,2,3]
    
    def return_deferred(self):
        """ helper function that always returns a deferred.  deferred is just a
        pass through so a reference to the deferred can be maintained """
        return self.deferred
    
    def raise_exception(self):
        """ function that always throws an exception, used for testing errors """
        raise Exception("I'm failing intentionally")
    
    def test_trivial(self):
        """
        Test instantiating FunctionResource class
        
        Verifies:
            * instance can be created with or without kwargs
        """
        FunctionResource(self.twisted_web_interface, self.return_list)
        FunctionResource(self.twisted_web_interface, self.return_list, auth=True, include_user=False)
    
    def test_new_session(self):
        """
        Test request from user without session
        
        Verifies:
            * session is created
            * session structure is correct
        """
        api = self.twisted_web_interface
        request = HTTPRequestProxy()
        resource = FunctionResource(api, self.return_list)
        resource.render(request)
        
        self.assert_('SESSION_ID' in api.sessions)
        session = api.sessions['SESSION_ID']
        self.assert_('expire' in session)
        self.assert_('auth' in session)
        self.assert_('challenge' in session)
        self.assertFalse(session['auth'])
        self.assertFalse(session['challenge'])
    
    def test_render_no_auth_required(self):
        """
        rendering a resource that does not require authentication
        
        Verifies:
            * response returned
        """
        api = self.twisted_web_interface
        request = HTTPRequestProxy()
        resource = FunctionResource(api, self.return_list, auth=False)
        response = resource.render(request)
        response = simplejson.loads(response)
        self.assertEqual([1,2,3], response)
    
    def test_render_with_user(self):
        """
        render a resource that requires the session_id as an arg
        
        Verifies:
            * response is returned
        """
        api = self.twisted_web_interface
        request = HTTPRequestProxy()
        resource = FunctionResource(api, self.return_list, auth=False, include_user=True)
        response = resource.render(request)
        response = simplejson.loads(response)
        self.assertEqual([1,2,3], response)
        self.return_list.assertCalled(self, 'SESSION_ID')
    
    def test_render_unauthorized(self):
        """
        render a resource that requires authorization, but user hasn't auth'ed
        
        Verifies:
            returns 401
        """
        api = self.twisted_web_interface
        request = HTTPRequestProxy()
        resource = FunctionResource(api, self.return_list, auth=True)
        response = resource.render(request)
        self.assertEqual(request.response_code, 401)
    
    def test_render_error(self):
        """
        renders a resource that throws an error
        
        Verifies:
            returns 500
        """
        api = self.twisted_web_interface
        request = HTTPRequestProxy()
        resource = FunctionResource(api, self.raise_exception, auth=False)
        
        with MuteStdout():
            response = resource.render(request)
        self.assertEqual(request.response_code, 500)
    
    def test_render_with_args(self):
        """
        Renders a resource that requires args
        """
        api = self.twisted_web_interface
        args = [1,2,3]
        kwargs = {'a':1, 'b':2, 'c':3}
        request = HTTPRequestProxy(args={'args':[simplejson.dumps(args)], \
                                        'kwargs':[simplejson.dumps(kwargs)]})
        resource = FunctionResource(api, self.return_list, auth=False)
        response = resource.render(request)
        response = simplejson.loads(response)
        self.assertEqual([1,2,3], response)
        self.return_list.assertCalled(self, *args, **kwargs)
    
    def test_render_deferred(self):
        """
        Renders a resource that returns a deferred
        
        Verifies:
            * server NOT_DONE_YET returned
            * deferred callback writes to response
        """
        self.deferred = Deferred()
        api = self.twisted_web_interface
        request = HTTPRequestProxy()
        resource = FunctionResource(api, self.return_deferred, auth=False)
        response = resource.render(request)
        
        self.assertEqual(server.NOT_DONE_YET, response)
        
        self.deferred.callback([1,2,3])
        request.finish.assertCalled(self)
        self.assertEqual([1,2,3], simplejson.loads(request.writer.getvalue()))


class InterfaceResourceTestCase(unittest.TestCase, TwistedWebInterfaceTestCaseMixin):
    
    def setUp(self):
        TwistedWebInterfaceTestCaseMixin.setUp(self)
        module = Bar()
        self.manager.register(module)
        self.resource = InterfaceResource(self.twisted_web_interface)
    
    def test_render(self):
        """
        Tests rendering the root resource
        
        Verifies:
            * list of registered interfaces is returned as json
        """
        response = set(simplejson.loads(self.resource.render(None)))
        check = set(['authenticate', 'challenge_response', 'foo','foo2'])
        self.assertEquals(response, check)
    
    def test_get_root_resource(self):
        """
        Tests root url
        
        Verifies:
            * returns list of interfaces that can be called
        """
        resource = self.resource.getChildWithDefault('', None)
        self.assertEqual(self.resource, self.resource)
    
    def test_get_interface_resource(self):
        """
        Tests retrieving a resource for an interface
        
        Verifies:
            * a function resource is returned
        """
        resource = self.resource.getChildWithDefault('foo', None)
        self.assert_(isinstance(resource, (FunctionResource,)))
    
    def test_get_invalid_interface(self):
        """
        Tests requesting a resource that does not exist:
        
        Verifies:
            * returns 404
        """
        resource = self.resource.getChildWithDefault('invalid path', None)
        self.assert_(isinstance(resource, (NoResource,)), 'invalid path should return 404 object')