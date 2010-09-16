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

from twisted.trial import unittest as twisted_unittest
from twisted.internet import threads

from pydra.tests import setup_test_environment
setup_test_environment()

from pydra.cluster.constants import WORKER_STATUS_WORKING, WORKER_STATUS_IDLE, \
    WORKER_STATUS_FINISHED
from pydra.cluster.module import ModuleManager
from pydra.cluster.worker import WorkerTaskControls

from pydra.tests import clean_reactor
from pydra.tests.cluster.tasks.test_task_manager import TaskManagerTestCaseMixIn
from pydra.tests.cluster.module.test_module_manager import TestAPI
from pydra.tests.mixin_testcases import ModuleTestCaseMixIn
from pydra.tests.proxies import RemoteProxy

class WorkerTaskControlsTestCase(twisted_unittest.TestCase, TaskManagerTestCaseMixIn):
    
    def setUp(self):
        TaskManagerTestCaseMixIn.setUp(self)
        self.create_cache_entry()
        self.task_manager.autodiscover()
        self.worker_task_controls = WorkerTaskControls()
        self.manager.register(self.worker_task_controls)
        self.worker_task_controls.master = RemoteProxy('master')
        self.assert_(self.worker_task_controls in self.manager._modules)
    
    def tearDown(self):
        clean_reactor()
    
    def run_task(self, key='test.testmodule.TestTask'):
        wtc = self.worker_task_controls
        wtc.run_task(key, None)
        return key
    
    def assertResults(self, results, size=1, value=2, workunit=False, failures=[]):
        """
        asserts that results are structured correctly
        :parameters:
            results - results object (tuple/list)
            size - expected size of results
            value - expected starting value for results
            workunit - workunit_id if any
            failures - workunit_ids corresponding to any failures, or any true
                       value for a single result
                       
        Verifies:
            * result size
            * values of all results
        """
        self.assert_(isinstance(results, (tuple,list)))
        self.assertEqual(len(results), size)
        for i in range(size):
            result = results[i]
            value_, workunit_, failure = result
            if workunit:
                self.assert_(workunit_)
            elif failures:
                self.assertFalse(failure)
            self.assertEqual(value+i, value)
    
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
    
    
    def verify_running_task(self, key):
        """
        Callback for verifying running task
        
        Verifies:
            * task instance created
            * task key matches requested key
            * worker status is WORKER_STATUS_WORKING
        """
        wtc = self.worker_task_controls
        self.assertEqual(wtc._task, key)
        self.assert_(wtc._task_instance)
        self.assertEqual(wtc.status(), (WORKER_STATUS_WORKING,key,None))
    
    def test_run_task(self):
        """
        run a task
        
        Verifies:
            * task is started
        """
        key = 'test.testmodule.TestTask'
        self.run_task()
        return threads.deferToThread(self.verify_running_task, key)
    
    def test_run_task_invalid_key(self):
        self.run_task('FAKE_KEY')
    
    def test_run_subtask(self):
        raise NotImplementedError
    
    def test_stop_task(self):
        raise NotImplementedError
    
    def test_status_idle(self):
        """
        retrieve status of worker
        
        Verifies:
            * WORKER_STATUS_WORKING if working
            * WORKER_STATUS_FINISHED if finished but results not sent
            * WORKER_STATUS_IDLE otherwise
        """
        wtc = self.worker_task_controls
        self.assertEqual(wtc.status(), (WORKER_STATUS_IDLE,))
    
    def test_work_complete(self):
        """
        Task completes work and calls work_complete()
        
        Verifies:
            * status is marked finished
            * master is sent results
        """
        wtc = self.worker_task_controls
        key = self.run_task()
        wtc.work_complete(2)
        args, kwargs, deferred = wtc.master.assertCalled(self, 'send_results')
        self.assertResults(args[1])
        self.assertEqual(wtc.status(), (WORKER_STATUS_FINISHED,key,None))
    
    def test_work_complete_no_master(self):
        """
        Task completes work and calls work_complete() but node isn't available
        
        Verifies:
            * status is marked finished
            * _results are saved
        """
        wtc = self.worker_task_controls
        wtc.master = None
        key = self.run_task()
        wtc.work_complete(2)
        self.assert_(wtc._results)
        self.assertResults(wtc._results)
        self.assertEqual(wtc.status(), (WORKER_STATUS_FINISHED,key,None))
    
    def test_work_complete_stopped(self):
        """
        Task completes work and calls work_complete() but node isn't available
        
        Verifies:
            * stop_flag is set
            * master is informed the worker stopped
        """
        wtc = self.worker_task_controls
        self.run_task()
        wtc.work_complete(2)
        self.assert_(wtc._stop_flag)
        wtc.master.assertCalled(self, 'worker_stopped')
    
    def test_work_complete_stopped_no_master(self):
        """
        Task completes work and calls work_complete() but node isn't available
        
        Verifies:
            * stop flag is set
        """
        wtc = self.worker_task_controls
        wtc.master = None
        self.run_task()
        wtc.work_complete(2)
        self.assert_(wtc._stop_flag)
        wtc.master.assertCalled(self, 'worker_stopped')
    
    def test_send_results_failed(self):
        """
        Send results failed, but master returned before retry was available
        
        Verifies:
            XXX this failover is not yet implemented because certain failures
            will result in an endless loop if we immediately retry sending the
            results.
        """
        wtc = self.worker_task_controls
        key = self.run_task()
        wtc.work_complete(2)
        args, kwargs, deferred = wtc.master.assertCalled(self, 'send_results')
        self.fail('failover is not implemented')
    
    def test_send_results_failed_master_returned(self):
        """
        Send results failed, but master returned before retry was available
        
        Verifies:
            XXX this failover is not yet implemented because certain failures
            will result in an endless loop if we immediately retry sending the
            results.
        """
        self.fail('failover is not implemented')
        wtc = self.worker_task_controls
        key = self.run_task()
        wtc.work_complete(2)
        args, kwargs, deferred = wtc.master.assertCalled(self, 'send_results')
    
    def test_send_stop_failed(self):
        wtc = self.worker_task_controls
        key = self.run_task()
        wtc.work_complete(2)
        args, kwargs, deferred = wtc.master.assertCalled(self, 'send_results')
        self.fail('failover is not implemented')
    
    def test_send_stop_failed_master_returned(self):
        """
        Send stop failed, but master returned before retry was available
        
        Verifies:
            XXX this failover is not yet implemented because certain failures
            will result in an endless loop if we immediately retry sending the
            results.
        """
        self.fail('failover is not implemented')
        wtc = self.worker_task_controls
        key = self.run_task()
        wtc.work_complete(2)
        args, kwargs, deferred = wtc.master.assertCalled(self, 'send_results')
        self.fail('failover is not implemented')
    
    def test_send_successful(self):
        wtc = self.worker_task_controls
        key = self.run_task()
        wtc.work_complete(2)
        args, kwargs, deferred = wtc.master.assertCalled(self, 'send_results')
        self.fail('need to handle call later')
    
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