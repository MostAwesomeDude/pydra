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

from pydra.cluster.node import worker_connection_manager
from pydra.tests.mixin_testcases import ModuleTestCaseMixIn
from pydra.tests.proxies import WorkerAvatarProxy


class WorkerConnectionManagerMixin():
    """
    TestCase mixin that contains functions for initializing
    WorkerConnectionManager and workers for a Node
    """
    def setUp(self):
        """ Initialize a WorkerConnectionManager """
        self.wcm = worker_connection_manager.WorkerConnectionManager()
        self.wcm._register(self.manager)

    def add_worker(self, main=False, finished=False):
        """ Helper Function for creating and connecting avatars """
        avatar = WorkerAvatarProxy(self.wcm, "worker:%s" % len(self.wcm.workers))
        avatar.attached(None)
        self.wcm.worker_authenticated(avatar)
        avatar.finished = finished
        return avatar


class WorkerConnectionManager(unittest.TestCase, ModuleTestCaseMixIn, WorkerConnectionManagerMixin):
    
    def setUp(self):
        self.tearDown()
        ModuleTestCaseMixIn.setUp(self)
        WorkerConnectionManagerMixin.setUp(self)
    
    def tearDown(self):
        pass
    
    def test_enable_workers(self):
        """
        Test enabling workers to log in
        """
        # XXX need to determine a meaningful way to check this works
        raise NotImplementedError
    
    def test_remove_worker(self):
        """
        Tests removing a worker from the active pool
        """
        # single worker
        wcm = self.wcm
        worker = self.add_worker()
        wcm.remove_worker(worker)
        self.assertFalse(worker.name in self.wcm.workers)
        
        # multiple workers
        worker0 = self.add_worker()
        worker1 = self.add_worker()
        wcm.remove_worker(worker0)
        self.assertFalse(worker0.name in self.wcm.workers)
        self.assert_(worker1.name in self.wcm.workers)
        wcm.remove_worker(worker1)
        self.assertFalse(worker1.name in self.wcm.workers)
    
    def test_authenticated(self):
        """
        Tests a worker successfully authenticating
        
        Verifies:
            * worker added to pool
            * WORKER_CONNECTED is emitted
            * second worker added to pool
            * WORKER_CONNECTED is emitted
        """
        wcm = self.wcm
        worker0 = self.add_worker()
        self.assert_(worker0.name in wcm.workers)
        self.manager.assertEmitted("WORKER_CONNECTED", worker0)
        
        worker1 = self.add_worker()
        self.assert_(worker1.name in wcm.workers)
        self.manager.assertEmitted("WORKER_CONNECTED", worker1)
    
    def test_disconnected(self):
        """
        Tests a worker disconnecting but its finished flag is set
        
        Verifies:
            * worker removed from pool
            * WORKER_DISCONNECTED is emitted
            * tests repeated for multiple workers
        """
        # single worker
        wcm = self.wcm
        worker = self.add_worker()
        wcm.worker_disconnected(worker)
        self.assertFalse(worker.name in self.wcm.workers)
        self.manager.assertEmitted("WORKER_DISCONNECTED", worker)
        
        # multiple workers
        worker0 = self.add_worker()
        worker1 = self.add_worker()
        wcm.worker_disconnected(worker0)
        self.assertFalse(worker0.name in wcm.workers)
        self.assert_(worker1.name in wcm.workers)
        self.manager.assertEmitted("WORKER_DISCONNECTED", worker0)
        wcm.worker_disconnected(worker1)
        self.assertFalse(worker1.name in wcm.workers)
        self.manager.assertEmitted("WORKER_DISCONNECTED", worker1)
    
    def test_disconnected_while_finished(self):
        """
        Tests a worker disconnecting
        
        Verifies:
            * WORKER_DISCONNECTED is emitted
            * tests repeated for multiple workers
        """
        # single worker
        wcm = self.wcm
        worker = self.add_worker(True)
        wcm.worker_disconnected(worker)
        self.assertTrue(worker.name in self.wcm.workers)
        self.manager.assertEmitted("WORKER_DISCONNECTED", worker)
        
        # multiple workers
        worker0 = self.add_worker(True)
        wcm.worker_disconnected(worker0)
        self.assertTrue(worker.name in wcm.workers)
        self.assertTrue(worker0.name in wcm.workers)
        self.manager.assertEmitted("WORKER_DISCONNECTED", worker0)
