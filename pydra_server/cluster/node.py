#! /usr/bin/python

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

from __future__ import with_statement

#
# Setup django environment 
#
if __name__ == '__main__':
    import sys
    import os

    #python magic to add the current directory to the pythonpath
    sys.path.append(os.getcwd())

    #
    if not os.environ.has_key('DJANGO_SETTINGS_MODULE'):
        os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'


from threading import Lock
from zope.interface import implements
from twisted.cred import portal, checkers
from twisted.spread import pb
from twisted.internet import reactor
from twisted.application import service, internet
import os
from subprocess import Popen


class NodeServer:
    """
    Node - A Node manages a server in your cluster.  There is one instance of Node running per server.
        Node will spawn worker processes for each core available on your machine.  This allows some
        central control over what happens on the node.
    """
    def __init__(self):
        self.workers = {}
        self.port_base = 11881
        self.host='localhost'
        self.node_key = None
        self.initialized = False
        self.__lock = Lock()
        #load tasks that are cached locally
        self.available_tasks = {}

        # get information about the server
        self.determine_info()

        print '[info] Node - starting server on port %s' % self.port_base


    def get_service(self):
        """
        Creates a service object that can be used by twistd init code to start the server
        """
        realm = ClusterRealm()
        realm.server = self

        #create security
        checker = checkers.InMemoryUsernamePasswordDatabaseDontUse()
        checker.addUser("tester", "1234")
        p = portal.Portal(realm, [checker])

        factory = pb.PBServerFactory(p)
        return internet.TCPServer(11890, factory)


    def determine_info(self):
        """
        Builds a dictionary of useful information about this Node
        """
        cores = self.detect_cores()

        self.info = {
            'cpu':2600,             # CPU MHZ per core
            'memory':3000,          # Memory allocated to the node
            'cores':cores           # Number of Cores
        }


    def init_node(self, master_host, master_port, node_key):
        """
        Initializes the node so it ready for use.  Workers will not be started
        until the master makes this call.  After a node is initialized workers
        should be able to reconnect if a connection is lost
        """

        # only initialize the node if it has not been initialized yet.
        # its possible for the server to be restarted without affecting
        # the state of the nodes
        if not self.initialized:
            with self.__lock:
                self.master_host = master_host
                self.master_port = master_port
                self.node_key = node_key

                #start the workers
                self.start_workers()

                self.initialized = True


    def start_workers(self):
        """
        Starts all of the workers.  By default there will be one worker for each core
        """
        self.pids = [
            Popen(["python", "pydra_server/cluster/worker.py", self.master_host, str(self.master_port), self.node_key, '%s:%s' % (self.node_key, i)]).pid 
            for i in range(self.info['cores'])
            ]


    def detect_cores(self):
        """
        Detect the number of core's on this Node
        """
        # Linux, Unix and MacOS:
        if hasattr(os, "sysconf"):
            if os.sysconf_names.has_key("SC_NPROCESSORS_ONLN"):
                # Linux & Unix:
                ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
                if isinstance(ncpus, int) and ncpus > 0:
                    return ncpus
            else: # OSX:
                return int(os.popen2("sysctl -n hw.ncpu")[1].read())
        # Windows:
        if os.environ.has_key("NUMBER_OF_PROCESSORS"):
                ncpus = int(os.environ["NUMBER_OF_PROCESSORS"]);
                if ncpus > 0:
                    return ncpus
        return 1 # Default


class ClusterRealm:
    implements(portal.IRealm)
    def requestAvatar(self, avatarID, mind, *interfaces):
        assert pb.IPerspective in interfaces
        avatar = MasterAvatar(avatarID)
        avatar.server = self.server
        avatar.attached(mind)
        return pb.IPerspective, avatar, lambda a=avatar:a.detached(mind)


class MasterAvatar(pb.Avatar):
    def __init__(self, name):
        self.name = name
        print '[info] master connected to node'

    def attached(self, mind):
        self.remote = mind

    def detached(self, mind):
        self.remote = None

    # returns the status of this node
    def perspective_status(self):
        pass

    # Returns a dictionary of useful information about this node
    def perspective_info(self):
        return self.server.info


    def perspective_init(self, master_host, master_port, node_key):
        """
        Initializes a node.  The server sends its connection information and
        credentials for the node
        """
        self.server.init_node(master_host, master_port, node_key)



#root application object
application = service.Application('Pydra Node')

#create node server
node_server = NodeServer()

# attach service
service = node_server.get_service()
service.setServiceParent(application)