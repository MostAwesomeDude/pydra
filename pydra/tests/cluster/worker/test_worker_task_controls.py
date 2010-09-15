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

from pydra.tests import setup_test_environment
setup_test_environment()

from pydra.cluster.module import ModuleManager
from pydra.cluster.worker import WorkerTaskControls

from pydra.tests.cluster.module.test_module_manager import TestAPI
from pydra.tests.mixin_testcases import ModuleTestCaseMixIn


class WorkerTaskControlsTestCase(unittest.TestCase, ModuleTestCaseMixIn):
    
    def setUp(self):
        ModuleTestCaseMixIn.setUp(self)
        self.worker_task_controls = WorkerTaskControls()
        self.manager.register(self.worker_task_controls)
        self.assert_(self.worker_task_controls in self.manager._modules)
    
    def test_trivial(self):
        """
        Trivial test that just instantiates class
        """
        module = WorkerTaskControls()
    
    def test_register(self):
        """
        Tests registering the module with a manager
        """
        manager = ModuleManager()
        module = WorkerTaskControls()
        api = TestAPI()
        manager.register(api)
        manager.register(module)
        self.assert_(module in manager._modules)
    
    def test_batch_complete(self):
        raise NotImplementedError
    
    def test_run_batch(self):
        raise NotImplementedError
    
    def test_run_task(self):
        raise NotImplementedError
    
    def test_stop_task(self):
        raise NotImplementedError
    
    def test_status(self):
        """
        retrieve status of worker
        
        Verifies:
            * WORKER_STATUS_WORKING if working
            * WORKER_STATUS_FINISHED if finished but results not sent
            * WORKER_STATUS_IDLE otherwise
        """
        raise NotImplementedError
    
    def test_work_complete(self):
        raise NotImplementedError
    
    def test_send_results_failed(self):
        raise NotImplementedError
    
    def test_send_stop_failed(self):
        raise NotImplementedError
    
    def test_send_successful(self):
        raise NotImplementedError
    
    def test_task_status(self):
        raise NotImplementedError
    
    def test_receive_results(self):
        raise NotImplementedError
    
    def test_release_worker(self):
        raise NotImplementedError
    
    def test_shutdown(self):
        raise NotImplementedError
    
    def test_request_worker(self):
        raise NotImplementedError
    
    def test_request_worker_release(self):
        raise NotImplementedError
    
    def test_release_request_successful(self):
        raise NotImplementedError
    
    def test_return_work(self):
        raise NotImplementedError
    
    def test_get_worker(self):
        """
        Get the worker
        
        Verifies:
            * always returns self
        """
        wtc = self.worker_task_controls
        worker = wtc.get_worker()
        self.assertEqual(worker, wtc)
    
    def test_get_key(self):
        """
        Get the task key
        
        Verifies:
            * always returns None
        """
        wtc = self.worker_task_controls
        key = wtc.get_key()
        self.assertEqual(None, key)
    
    def test_retrieve_task_failed(self):
        raise NotImplementedError
    
    def test_subtask_started(self):
        raise NotImplementedError