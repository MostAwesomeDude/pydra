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

from pydra.cluster.module.interface_module import InterfaceModule
from pydra.cluster.module.attribute_wrapper import AttributeWrapper

import logging
logger = logging.getLogger('root')

class ModuleManager(object):
    """
    ModuleManager is used to coordinate module interactions.  It Loads them
    and handles transporting signals between emitters and listerners

    This allows modules to register for a signal that can be emitted by any 
    other component.  This allows multiple components to emit the same signal
    This system allows modules to be dependent on events rather than individual
    modules.  It also removes the need for complicated dependency checking when
    loading modules.

    IE.  Both the manual registration and autodiscovery can emit NODE_ADDED.
    """
    modules = None

    def __init__(self, modules=[]):
        """
        Initialize the ModuleManager with the list of nodes
        
        :Parameters:
            modules - list of modules to initially register
        """
        
        # List of module instances
        if self.modules == None:
            self.modules = []
        self._modules = []
        
        # List of module instances that provide an interface for controllers
        self._interface_modules = []
        
        # map of signals to modules that emit them. This is only used for debugging
        # purposes.  When initializing components this can be used to double check
        # registered listeners actually have someone to talk to.
        self._signals = {}
        
        # map of signals to listeners.  Used for calling functions 
        # when the signal is emitted
        self._listeners = {}
        
        # list of functions exposed to the controller Interface.  These are
        # functions that can be called via the user interface
        self._interfaces = []
        
        # dictionary of remotes which are a dictionary of methods that are exposed 
        # as remote methods.  Each remote is a separate interface that may be given
        # out depending no the type of remote user
        self._remotes = {}
        
        # list of proeprties that are shared with the manager.  This allows them
        # to be accessed easier by other modules.  The other modules won't need
        # to know which other module supplies the property, only that it exists
        # this is a map of the property name to the class.  This is because for
        # simple types it would be a copy rather than a reference.
        self._shared = {}
        
        # list of services that have been registered with the manager.  This is a
        # list of methods that each return a service object
        self._services = []
        
        # append any user modules onto this one
        self.modules += modules
        
        for module in self.modules:
            try:
                if module.__class__.__name__ == 'type':
                    module = module()
                self.register(module)
                
            except Exception, e:
                logger.error('Error Loading Module: %s' % module)
                import traceback, sys
                print e
                type, value, traceback_ = sys.exc_info()
                traceback.print_tb(traceback_, limit=10, file=sys.stdout)
        
        # adding friends
        for module in self._modules:
            if hasattr(module, '_friends'):
                for friend_name, friend_class  in module._friends.iteritems():
                    for module_again in self._modules:
                        # inefficient, but have to
                        if module_again.__class__ == friend_class:
                            module.__dict__[friend_name] = module_again

    def deregister_interface(self, module, interface):
        """
        Deregisters an exposed function.  If there are already interfaces
        registered with this ModuleManager then they will be informed
        
        :Parameters:
            interface: exposed function or attribute
        """
        if (module, interface) in self._interfaces:
            self._interfaces.remove((module, interface))
        
        for interface_module in self._interface_modules:
            if isinstance(interface, (tuple, list)):
                interface_module.register_interface(module, interface[0], \
                                                    **interface[1])
            else:
                interface_module.register_interface(module, interface)

    def deregister_interface_module(self, module):
        """
        deregister a module that provides an interface for controllers.  Register
        any exposed functions already registered with this ModuleManager
        
        :Parameters:
            module: module to deregister
        """
        if module in self._interface_modules:
            self._interface_modules.remove(module)
        module.deregister_all()

    def deregister_listener(self, signal, function):
        """
        Deregister a listener
        
        :Parameters:
            signal signal to listen for
            function function to call
        """
        if signal in self._listeners:
            signals = self._listeners[signal]
            if function in signals:
                signals.remove(function)
            if not signals:
                del self._listeners[signal]

    def deregister(self, module):
        """
        deregister a module
        
        :Parameters:
            module - class of the module to deregister.  Module will be
                        instantiated by the ModuleManager
        """
        logger.info('Unloading Module: %s' % module.__class__.__name__)
        module._deregister()
        self._modules.remove(module)
        
        for signal in module._signals:
            self.deregister_signal(signal, module)
        
        for signal, function in module._listeners.items():
            self.deregister_listener(signal, function)
        
        for remote in module._remotes:
            self.deregister_remote(module, *remote)
        
        for interface in module._interfaces:
            self.deregister_interface(module, interface)
        
        for service in module._services: 
            self.deregister_service(service)
        
        if isinstance(module,(InterfaceModule,)):
            self.deregister_interface_module(module)

    def deregister_remote(self, module, remote, function):
        """
        Deregister a remote - a remote is a function or attribute available to
                            internal RPC interfaces within the cluster
        """
        if remote in self._remotes:
            remote_ = self._remotes[remote]
            if isinstance(function,(list,tuple)): 
                if isinstance(function[0],(str,)):
                    # attribute exposed as method with options, repackage with
                    # wrapper around the attribute name
                    key = function[0]
                else:
                    # function with other options.  Add as is extracting the
                    # name from the mapped function
                    key = function[0].__name__
            
            elif isinstance(function,(str,)):
                # attribute exposed as a method with no other options
                key = function
            
            else:
                # function with no options
                key = function.__name__
            
            if key in remote_:
                del remote_[key]
            
            if not remote_:
                del self._remotes[remote]

    def deregister_service(self, service):
        """
        Deregister a service exposed by a module
        """
        if service in self._services:
            self._services.remove(service)

    def emit_signal(self, signal, *args, **kwargs):
        """
        Called when a module wants to emit a signal.  It notifies
        all listeners by calling the registered
        
        :Parameters:
            signal to emit
            args - args list to be passed through to functions
            kwargs - kwargs dict to be passed through to functions
        """
        if self._listeners.has_key(signal):
            signal_listeners = self._listeners[signal]
            for function in signal_listeners:
                function(*args, **kwargs)

    def get_services(self):
        """
        Get the service objects used by twistd.  The services are exposed by
        modules.  This method just calls all the mapped functions used for
        creating the services
        
        :returns: list of twisted service objects
        """
        return [service(self) for service in self._services]

    def get_shared(self, key):
        """
        Returns a shared property
        
        :Parameters:
            key to look up
            
        :returns: Value if exists, None otherwise
        """
        try:
            return self.__dict__[key]
        except KeyError:
            return None

    def register_interface(self, module, interface):
        """
        Registers an exposed function.  If there are already interfaces
        registered with this ModuleManager then they will be informed
        
        :Parameters:
            interface: exposed function or attribute
        """
        self._interfaces.append((module, interface))
        
        for interface_module in self._interface_modules:
            if isinstance(interface, (tuple, list)):
                interface_module.register_interface(module, interface[0], \
                                                    **interface[1])
            else:
                interface_module.register_interface(module, interface)

    def register_interface_module(self, interface_module):
        """
        Register a module that provides an interface for controllers.  Register
        any exposed functions already registered with this ModuleManager
        
        :Parameters:
            module: module to register
        """
        self._interface_modules.append(interface_module)
        
        for module, interface in self._interfaces:
            if isinstance(interface, (tuple, list)):
                interface_module.register_interface(module, interface[0], \
                                                    **interface[1])
            else:
                interface_module.register_interface(module, interface)

    def register_listener(self, signal, function):
        """
        Register a listener
        
        :Parameters:
            signal signal to listen for
            function function to call
        """
        try:
            self._listeners[signal].append(function)
        except KeyError:
            self._listeners[signal] = [function]

    def register(self, module):
        """
        Register a module
        
        :Parameters:
            module - class of the module to register.  Module will be
                        instantiated by the ModuleManager
        """
        logger.info('Loading Module: %s' % module.__class__.__name__)
        module._register(self)
        self._modules.append(module)
        
        for signal in module._signals:
            self.register_signal(signal, module)
        
        for signal, function in module._listeners.items():
            self.register_listener(signal, function)
        
        for remote in module._remotes:
            self.register_remote(module, *remote)
        
        for interface in module._interfaces:
            self.register_interface(module, interface)
        
        for service in module._services: 
            self.register_service(service)
        
        if isinstance(module,(InterfaceModule,)):
            self.register_interface_module(module)

    def register_remote(self, module, remote, function):
        """
        Register a remote - a remote is a function or attribute available to
                            internal RPC interfaces within the cluster
        
        :Parameters:
            module: module registering this remote
            remote: group of mapped functions this remote belongs to.
                       remotes are grouped to allow multiple interfaces with
                       distinct functions exposed to it.
            function: function to map.  This may be a function, or method
               name.  Optionally it may be contained in a tuple with other
               options.
                
               Available Options:
                  * secure (boolean): mark field as requiring authentication.
                                      default value is true. 
        """
        # get the _remote list, initialize if it
        # does not yet exist
        try:
            _remote = self._remotes[remote]
        except KeyError:
            _remote = {}
            self._remotes[remote] = _remote
        
        if isinstance(function,(list,tuple)): 
            if isinstance(function[0],(str,)):
                # attribute exposed as method with options, repackage with
                # wrapper around the attribute name
                attribute = function[0]
                wrapper = AttributeWrapper(module, attribute)
                function = list(function[1:])
                function.insert(0, wrapper)
                _remote[attribute] = function
            
            else:
                # function with other options.  Add as is extracting the
                # name from the mapped function
                _remote[function[0].__name__] = function
        
        elif isinstance(function,(str,)):
            # attribute exposed as a method with no other options
            _remote[function] = AttributeWrapper(module, function)
        
        else:
            # function with no options
            _remote[function.__name__] = function

    def register_service(self, service):
        """
        Register a service exposed by a module
        """
        self._services.append(service)

    def register_signal(self, signal, module):
        """
        Register a signal that a module can send
        """
        try:
            self._signals[signal].append(module)
        except KeyError:
            self._signals[signal] = [module]

    def set_shared(self, key, value):
        """
        Sets a shared property
        """
        self.__dict__[key] = value