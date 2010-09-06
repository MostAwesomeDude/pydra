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

from twisted.internet.defer import Deferred

from pydra.tests import setup_test_environment
setup_test_environment()

from pydra.cluster.node import worker_manager
from pydra.cluster.constants import WORKER_STATUS_IDLE
from pydra.cluster.tasks import STATUS_UNKNOWN

from pydra.tests.cluster.node.test_worker_connection_manager import WorkerConnectionManagerMixin
from pydra.tests.mixin_testcases import ModuleTestCaseMixIn
from pydra.tests.proxies import ModuleManagerProxy, RemoteProxy, WorkerAvatarProxy


class PopenProxy():
    """ proxy class for subprocess.Popen.  This allows us to mock the actions
    of Popen without actually spawning processes, which could get very messy """
    
    killed = False
    terminated = False
    pid = None
    
    def __init__(self, args):
        self.args = args
        self.pid = 1234
    
    def kill(self):
        self.killed = True
    
    def terminate(self):
        self.terminated = True


class PopenProxyFactory():
    """ factory for creating PopenProxy objects.  It allows error to be mimiced
    by setting error==True"""
    error = False
    
    def __init__(self):
        self.instances = []
    
    def __call__(self, args):        
        if self.error:
            raise OSError
        self.instances.append(args)
        return PopenProxy(args)


class WorkerManager(unittest.TestCase, ModuleTestCaseMixIn, WorkerConnectionManagerMixin):
    
    def setUp(self):
        self.tearDown()
        ModuleTestCaseMixIn.setUp(self)
        WorkerConnectionManagerMixin.setUp(self)
        
        self.wm = worker_manager.WorkerManager()
        self.manager.register(self.wm)
        self.wm.master = RemoteProxy('master')
        
        # monkey patch Popen so that it does not actually start processes
        worker_manager.Popen = PopenProxyFactory()
        self.popen = worker_manager.Popen
        
        # monkey patch WorkerAvatar so WorkerAvatarProxy are used instead
        worker_manager.WorkerAvatar = WorkerAvatarProxy
    
    def tearDown(self):
        pass

    def test_init_node(self):
        """
        Verifies:
            * NODE_INITIALIZED is emitted
        """
        self.manager.emit = False
        self.wm.init_node("avatar_name",'localhost',1234,'fake_key')
        self.manager.assertEmitted('NODE_INITIALIZED', self.wm.node_key)

    def test_proxy_to_master(self):
        """
        Tests helper function for passing commands through to master
        
        Verify:
            * master is sent the remote call
        """
        wm = self.wm
        worker = self.add_worker()
        wm.proxy_to_master('foo_bar', worker, 1, 2, c=3)
        self.assertCalled(wm.master, 'foo_bar', worker, 1, 2, c=3)
    
    def test_proxy_to_worker(self):
        """
        Tests helper function for passing commands through to a worker
        
        Verify
            * worker is sent the remote call
            * message to non-existentworker raises error
        """
        wm = self.wm
        worker = self.add_worker()
        wm.proxy_to_worker('foo_bar', worker.name, 1, 2, c=3)
        
        def proxy():
            self.wm.proxy_to_worker('foo_bar', "FAKE_WORKER_ID")
        
        self.assertCalled(worker.remote, 'foo_bar', worker.name, 1, 2, c=3)
        self.assertRaises(Exception, proxy)

    def test_receive_results(self):
        """
        Subtask sending results to mainworker
        
        Verifies:
            * command is passed on to worker
        """
        wm = self.wm
        worker = self.add_worker()
        wm.receive_results(wm.master, worker.name)
        self.assertCalled(worker.remote, 'receive_results')

    def test_release_worker(self):
        """
        master instructs worker to be released and shutdown
        
        Verify:
            * worker marked as finished
            * WORKER_FINISHED emitted
        """
        wm = self.wm
        worker = self.add_worker()
        wm.release_worker(wm.master, worker.name)
        self.assertCalled(worker.remote, 'release_worker')
        self.assert_(worker.finished, 'worker should be marked as finished')
        self.manager.assertEmitted('WORKER_FINISHED', worker)

    def test_request_worker(self):
        """
        Worker requesting additional workers
        
        Verify:
            * master sent command
        """
        wm = self.wm
        worker = self.add_worker()
        wm.request_worker(worker.name)
        self.assertCalled(wm.master, 'request_worker')
    
    def test_request_worker_release(self):
        """
        Worker requesting additional workers
        
        Verify:
            * master sent command
        """
        wm = self.wm
        worker = self.add_worker()
        wm.request_worker_release(worker.name)
        self.assertCalled(wm.master, 'request_worker_release')

    def test_run_subtask_local(self):
        """
        Start a subtask on the mainworker

        Verifies:
            * command sent to worker
            * returns deferred
        """
        wm = self.wm
        worker = self.add_worker()
        args = 'fake.task', 'v1.0', 'FAKE_CLASS', 'SEARCH_PATH?', worker.name
        deferred = wm._run_task(*args, main_worker=worker.name)
        self.assert_(isinstance(deferred, (Deferred,)), 'run_task should return a deferred')
        self.assertCalled(worker.remote, 'run_task', *args)

    def test_run_task(self):
        """
        Tests running a task on a running worker
        
        TODO: make this use external function so that task_manager interaction
        can be tested as well
        
        Verifies:
            * command sent to worker
            * returns deferred
        """
        wm = self.wm
        worker = self.add_worker()
        args = 'fake.task', 'v1.0', 'FAKE_CLASS', 'SEARCH_PATH?', worker.name
        deferred = wm._run_task(*args)
        self.assert_(isinstance(deferred, (Deferred,)), 'run_task should return a deferred')
        self.assertCalled(worker.remote, 'run_task', *args)

    def test_run_task_worker_not_started(self, mainworker=True, subtask=False):
        """
        Tests running the task when the worker has not been started yet
        
        Verifies:
            * worker will be started
            * deferred chain will be set for
            * callback from worker starting causes run_task to be re-called
        """
        wm = self.wm
        worker_id = 'new_worker:0'
        main_worker_id = worker_id if mainworker else 'other_worker:0'
        workunits = 'FAKE_WORKUNITS' if subtask else None
        kwargs = dict(
             key='fake.task',
             version='v1.1',
             task_class='FAKE_CLASS',
             module_search_path='SEARCH_PATH?',
             worker_key=worker_id,
             workunits=workunits,
             main_worker=main_worker_id
        )
        
        # initial call that starts worker
        deferred = wm._run_task(**kwargs)
        self.assert_(isinstance(deferred, (Deferred,)), 'run_task should return a deferred')
        worker = wm.workers[worker_id]
        deferred.addCallback(self.callback, key='run_task')
        
        # simulate worker authenticating
        worker.attached(worker_id)
        self.wcm.worker_authenticated(worker)
        call_args, call_kwargs, call_deferred = self.assertCalled(worker.remote, 'run_task', **kwargs)
        
        # simulate worker successful run_task
        call_deferred.callback(1)
        self.assertCallback('run_task')
        
        return worker

    def test_run_task_worker_not_started_popen_error(self):
        """
        Failure for Popen to start properly, resulting in no popen object
        
        described by:
            http://pydra-project.osuosl.org/ticket/158
            http://bugs.python.org/issue1068268
            
        Verifies:
            * worker is still created
            * worker pid is fetched from the worker
            * deferred returned
            * deferred chain calls run_task
        """
        self.popen.error = True
        worker = self.test_run_task_worker_not_started()

        # verify pid will be fetched
        self.assertCalled(worker.remote, 'getpid')

    def test_send_results(self):
        """
        Worker is sending results from finished task

        Verifies:
            * worker marked as finished
            * WORKER_FINISHED is emitted
            * call passed to master
            * returns True (shutdown)
        """
        wm = self.wm
        worker = self.test_run_task_worker_not_started()
        shutdown = wm.send_results(worker.name, 'test finished')
        
        self.manager.assertEmitted('WORKER_FINISHED')
        self.assert_(worker.finished, 'Worker should be marked finished')
        self.assert_(shutdown, 'worker should be instructed to shutdown via return value')
        self.assertCalled(wm.master, 'send_results')

    def test_send_results_subtask(self):
        """
        Worker other than main_worker is sending results from a finished subtask

        Verifies:
            * worker not marked as finished
            * WORKER_FINISHED not emitted
            * call passed to master
            * returns False (no shutdown)
        """
        wm = self.wm
        worker = self.test_run_task_worker_not_started(mainworker=False, subtask=True)
        shutdown = wm.send_results(worker.name, 'test finished')

        self.manager.assertNotEmitted('WORKER_FINISHED')
        self.assertFalse(worker.finished, 'Worker should not be marked finished')
        self.assertFalse(shutdown, 'worker should not be instructed to shutdown via return value')
        self.assertCalled(wm.master, 'send_results')

    def test_send_results_subtask_local(self):
        """
        main_worker is sending results from a finished subtask

        Verifies:
            * worker not marked as finished
            * WORKER_FINISHED not emitted
            * call passed to master
            * returns False (no shutdown)
        """
        wm = self.wm
        worker = self.test_run_task_worker_not_started(mainworker=True, subtask=True)
        shutdown = wm.send_results(worker.name, 'test finished')

        self.manager.assertNotEmitted('WORKER_FINISHED')
        self.assertFalse(worker.finished, 'Worker should not be marked finished')
        self.assertFalse(shutdown, 'worker should not be instructed to shutdown via return value')
        self.assertCalled(wm.master, 'send_results')

    def test_send_results_failed_no_master(self):
        """
        sending results failed because the master disconnected      

        Verify:
            * call is not retried
        """
        self.fail('Failover is not implemented in the module, so test doesnt work yet')

    def test_send_resutls_failed_master_reconnected(self):
        """
        sending results fauled because the master disconnected, but reconnected
        before failure handler fired
        
        Verify:
            * call is retried
        """
        self.fail('Failover is not implemented in the module so test doesnt work yet')
        

    def test_stop_task(self):
        """
        Stop command triggered by user or by error in another worker
        
        Verify:
            * stop command passed on to worker
        """
        wm = self.wm
        worker = self.add_worker()
        wm.stop_task(wm.master, worker.name)
        self.assertCalled(worker.remote, 'stop_task')

    def test_subtask_started(self):
        """
        Subtask start notification sent to main worker
        
        Verify:
            * message passed on to worker
        """
        wm = self.wm
        worker = self.add_worker()
        wm.subtask_started(wm.master, worker.name)
        self.assertCalled(worker.remote, 'subtask_started')

    def test_task_status(self):
        """
        Fetch task status
        
        Verifies:
            * if worker is running command is passed to worker
            * otherwise returns STATUS_UNKNOWN
        """
        
        wm = self.wm
        worker = self.add_worker()
        wm.task_status(wm.master, worker.name)
        self.assertCalled(worker.remote, 'task_status')
        
        status = wm.task_status(wm.master, "FAKE_WORKER_ID")
        self.assert_(status==STATUS_UNKNOWN, 'invalid worker should return STATUS_UNKNOWN')

    def test_worker_status(self):
        """
        Fetch Worker Status
        
        Verifies:
            * Worker is idle if it is not running
            * otherwise, command passed on to worker
        """
        wm = self.wm
        worker = self.add_worker()
        status0 = wm.worker_status(wm.master, worker.name)
        status1 = wm.worker_status(wm.master, "FAKE_WORKER_ID")
        
        self.assertCalled(worker.remote, 'status')
        self.assert_(isinstance(status1, (tuple,)), "non existant worker should return a tuple")
        self.assert_(status1[0]==WORKER_STATUS_IDLE, "non existant worker should return status idle")

    def test_worker_stopped(self):
        """
        Worker stopped
        
        Verify:
            * worker is marked finished
            * command is sent to master
        """
        wm = self.wm
        worker = self.add_worker()
        wm.worker_stopped(worker.name)
        self.assert_(worker.finished, "worker should be marked finished")
        self.assertCalled(wm.master, 'worker_stopped', worker.name)
