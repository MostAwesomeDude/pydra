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
import types

from twisted.internet.defer import Deferred

from pydra.cluster.module.module_manager import ModuleManager
from pydra.cluster.auth.worker_avatar import WorkerAvatar

class ModuleManagerProxy(ModuleManager):
    """
    Proxy of module manager used for capturing signals sent by modules that
    are being tested.
    """
    modules = []
    
    def __init__(self, testcase=None, emit=True, *args, **kwargs):
        self.signals = []
        self.testcase = testcase
        super(ModuleManagerProxy, self).__init__(*args, **kwargs)
        self.emit = emit

    def emit_signal(self, signal, *args, **kwargs):
        self.signals.append((signal, args, kwargs))
        if self.emit:
            super(ModuleManagerProxy, self).emit_signal(signal, *args, **kwargs)

    def assertEmitted(self, signal, *args, **kwargs):
        """ asserts that the signal was emitted """        
        self.testcase.assert_(self.testcase!=None, "ModuleManagerProxy.testcase was not set, cannot assert emitted signals")
        
        if args or kwargs:
            #detailed match
            for t in self.signals:
                signal_, args_, kwargs_ = t
                if signal_==signal and args_==args and kwargs_==kwargs:
                    return t
            self.testcase.fail("exact signal (%s) was not emitted: %s" % (signal, self.signals))
            
        else:
            # simple match
            for t in self.signals:
                signal_, args_, kwargs_ = t
                if signal_==signal:
                    return t
            self.testcase.fail("signal (%s) was not emitted: %s" % (signal, self.signals))

    def assertNotEmitted(self, signal, *args, **kwargs):
        """ asserts that the signal was not emitted """
        self.testcase.assert_(self.testcase!=None, "ModuleManagerProxy.testcase was not set, cannot assert emitted signals")
        if args or kwargs:
            #detailed match
            for t in self.signals:
                signal_, args_, kwargs_ = t
                if signal_==signal and args_==args and kwargs_==kwargs:
                    self.testcase.fail("exact signal (%s) was emitted: %s" % (signal, self.signals))

        else:
            # simple match
            for t in self.signals:
                signal_, args_, kwargs_ = t
                if signal_==signal:
                    self.testcase.fail("signal (%s) was emitted: %s" % (signal, self.signals))


class ThreadsProxy():
    """ Proxy of threads module """
    def __init__(self, testcase):
        self.testcase = testcase
        self.calls = []
    
    def deferToThread(self, func, *args, **kwargs):
        deferred = Deferred()
        self.calls.append((func, args, kwargs, deferred))
        return deferred

    def was_deferred(self, func):
        for call in self.calls:
            if call[0] == func:
                return True
        return False


class CallProxy(object):
    """ Proxy for a method that will record calls to it.  To use this class
    monkey patch the original method using an instance of this class
    
    setting the enabled flag can enable/disable whether the method is actually
    executed when it is called, or just recorded.
    """
    def __init__(self, func, enabled=True, response=None, **kwargs):
        """
        :parameters:
            func: function to proxy
            enabled: whether to call the wrapped function
            kwargs: kwargs passed to all calls.  may be overwritten by kwargs
                    passed to function
        """
        self.func = func
        self.calls = []
        self.enabled = enabled
        self.kwargs = kwargs
        self.response = response
        
        if func:
            self.argumented_call = self.create_matching_function(func)

    def __new__(cls, *args, **kwargs):
        # create instance of class to hold args, technically
        # we could just return the inner function here but it might
        # still be a good idea to drop this whole __new__ business
        instance = super(CallProxy, cls).__new__(cls)
        instance.__init__(*args, **kwargs)
       
        # return wrapped function
        return instance.create_function()
        
    def create_function(self):
        """
        Creates an inner function that wraps the original call.  The inner
        function will:
            * have access to the callProxy instance by referencing self.
            * have additional functions monkey patched onto it so that it may
            perform actions such as assertCalled()
        """
        def assertCalled(testcase, *args, **kwargs):
            """
            Assertion function for checking if a callproxy was called
            """
            f = self.func
            calls = self.calls
            if args or kwargs:
                #detailed match
                for t in calls:
                    args_, kwargs_ = t
                    if args_==args and kwargs_==kwargs:
                        return t
                testcase.fail("exact call (%s) did not occur: %s" % (f, calls))
                
            else:
                # simple match
                testcase.assert_(calls, "%s was not called: %s" % (f, calls))
                return calls[0]
    
        def assertNotCalled(testcase, *args, **kwargs):
            """
            Assertion function for checking if callproxy was not called
            """
            f = self.func
            calls = self.calls
            if args or kwargs:
                #detailed match
                for t in calls:
                    args_, kwargs_ = t
                    if args_==args and kwargs_==kwargs:
                        testcase.fail("exact call (%s) was made: %s" % (f, calls))
            else:
                # simple match
                testcase.assertFalse(calls, '%s was not called' % f)
        
        def enable():
            self.enabled = True
        
        def disable():
            self.enabled = False
        
        def reset():
            self.calls = []
        
        def call_proxy (*args, **kwargs):
            #print "Entering", self, args, kwargs
            response = None
            kwargs_ = {}
            kwargs_.update(self.kwargs)
            kwargs_.update(kwargs)
            self.calls.append((args, kwargs_))
            
            if self.enabled:
                response = self.func(*args, **kwargs_)
            elif self.func:
                # call argumented call, this ensures the args are checked even when
                # the real function isn't actually called
                self.argumented_call(*args, **kwargs)
            #print "Exited", self.func
            return self.response if self.response != None else response
            
        call_proxy.assertCalled = assertCalled
        call_proxy.assertNotCalled = assertNotCalled
        call_proxy.calls = self.calls
        call_proxy.disable = disable
        call_proxy.enable = enable
        call_proxy.reset = reset
        call_proxy.func = self.func
        if self.func:
            call_proxy.__name__ = 'call_proxy(%s)' % self.func.__name__
        return call_proxy

    def create_matching_function(self, func):
        """
        constructs a function with a method signature that matches the
        function that is passed in.  The resulting function does not actually
        do anything.  It is only used for verifying arguments to the call match.
        
        The function is constructed from a combination of properties from an
        inner function and the function passed in.
        """
        def base(): pass
        
        base_code = base.__code__
        code = func.__code__
        
        new_code = types.CodeType( \
            code.co_argcount, \
            code.co_nlocals, \
            base_code.co_stacksize, \
            code.co_flags, \
            base_code.co_code, \
            base_code.co_consts, \
            base_code.co_names, \
            code.co_varnames, \
            base_code.co_filename, \
            func.__name__, \
            base_code.co_firstlineno, \
            base_code.co_lnotab)
         
        return types.FunctionType(new_code, func.func_globals, \
                                  func.__name__, func.func_defaults)


class RemoteProxy():
    """
    Proxy of worker (a twisted avatar) used for capturing remote method calls
    during testing.
    """
    def __init__(self, name='localhost:0'):
        self.remote = self
        self.calls = []
        self.name = name

    def callRemote(self, *args, **kwargs):
        deferred = Deferred()
        self.calls.append((args, kwargs, deferred))
        return deferred
    
    def assertCalled(self, testcase, function, *args, **kwargs):
        """
        Assertion function for checking if a remote_proxy had a specific
        callback called
        """
        for call in self.calls:
            args, kwargs, deferred = call
            _function = args[0]
            if _function == function:
                # for now only check function name.  eventually this should
                # also check some set of parameters
                return call
        testcase.fail('RemoteProxy (%s) did not have %s called' % (self.name, function))


class WorkerAvatarProxy(WorkerAvatar):
    """
    Worker avatar proxy that uses a remote proxy instead of a real remote
    """
    def attached(self, mind):
        self.remote = RemoteProxy(self.name)
