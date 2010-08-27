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
from twisted.internet.defer import Deferred

from pydra.cluster.module.module_manager import ModuleManager
from pydra.cluster.auth.worker_avatar import WorkerAvatar

class ModuleManagerProxy(ModuleManager):
    """
    Proxy of module manager used for capturing signals sent by modules that
    are being tested.
    """
    modules = []
    
    def __init__(self, *args, **kwargs):
        self.signals = []
        self.testcase = None
        super(ModuleManagerProxy, self).__init__(*args, **kwargs)

    def emit_signal(self, signal, *args, **kwargs):
        self.signals.append((signal, args, kwargs))

    def assertEmitted(self, signal, *args, **kwargs):
        """ asserts that the signal was emitted """        
        self.testcase.assert_(self.testcase!=None, "ModuleManagerProxy.testcase was not set, cannot assert emitted signals")
        
        if args or kwargs:
            #detailed match
            for t in self.signals:
                signal_, args_, kwargs_ = t
                if signal_==signal and args_==args and kwargs_==kwargs:
                    return
            self.testcase.fail("exact signal (%s) was not emitted: %s" % (signal, self.signals))
            
        else:
            # simple match
            for t in self.signals:
                signal_, args_, kwargs_ = t
                if t==signal:
                    return args, kwargs
            self.testcase.fail("signal (%s) was not emitted: %s" % (signal, self.signals))


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


class CallProxy():
    """ Proxy for a method that will record calls to it.  To use this class
    monkey patch the original method using an instance of this class
    
    setting the enabled flag can enable/disable whether the method is actually
    executed when it is called, or just recorded.
    """
    def __init__(self, func):
        self.func = func
        self.calls = []
        self.enabled = True

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.enabled:
            return self.func(*args, **kwargs)


class RemoteProxy():
    """
    Proxy of worker (a twisted avatar) used for capturing remote method calls
    during testing.
    """
    def __init__(self, name):
        self.remote = self
        self.calls = []
        self.name = name

    def callRemote(self, *args, **kwargs):
        deferred = Deferred()
        self.calls.append((args, kwargs, deferred))
        return deferred
    
    
class WorkerAvatarProxy(WorkerAvatar):
    """
    Worker avatar proxy that uses a remote proxy instead of a real remote
    """
    def attached(self, mind):
        self.remote = RemoteProxy(self.name)