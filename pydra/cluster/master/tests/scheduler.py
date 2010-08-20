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

from pydra.cluster.constants import *
from pydra.cluster.master.scheduler import TaskScheduler
from pydra.cluster.module.module_manager import ModuleManager
from pydra.cluster.tasks import *
from pydra.models import TaskInstance, WorkUnit, Batch

def suite():
    """
    Build a test suite from all the test suites in this module
    """
    return unittest.TestSuite([
            unittest.TestLoader().loadTestsFromTestCase(TaskScheduler_Test),
            unittest.TestLoader().loadTestsFromTestCase(TaskScheduler_Models_Test),
        ])


class ModuleManagerProxy(ModuleManager):
    """
    Proxy of module manager used for capturing signals sent by modules that
    are being tested.
    """
    modules = []
    
    def __init__(self, *args, **kwargs):
        self.signals = []
        super(ModuleManagerProxy, self).__init__(*args, **kwargs)

    def emit_signal(self, signal, *args, **kwargs):
        self.signals.append((signal, args, kwargs))


class TaskPackageProxy():
    version = 'version 1.0'


class TaskManagerProxy():
    """
    Proxy of TaskManager used for testing
    """
    def get_task_package(self, task):
        return TaskPackageProxy()


class WorkerProxy():
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


def c_task_instance(**kwargs):
    """ Creates a task instance for testing """
    task_instance = TaskInstance()
    task_instance.task_key = 'foo.bar'
    task_instance.status = STATUS_STOPPED
    task_instance.__dict__.update(kwargs)
    task_instance.save()
    return task_instance   


class TaskScheduler_Models_Test(unittest.TestCase):
    """
    Tests for models used by the TaskScheduler
    """
    
    def setUp(self):
        self.tearDown()

    def tearDown(self):
        Batch.objects.all().delete()
        WorkUnit.objects.all().delete()
        TaskInstance.objects.all().delete()

    def test_get_queued(self):
        """
        Verifies that the queued method of the manager returns queued tasks
        and nothing else
        """
        c_task_instance()
        c_task_instance(status=STATUS_RUNNING)
        c_task_instance(status=STATUS_FAILED)
        c_task_instance(status=STATUS_COMPLETE)
        c_task_instance(status=STATUS_CANCELLED)
        tasks = TaskInstance.objects.queued()  
        self.assert_(tasks.count()==1, tasks.count())
        
    def test_get_running(self):
        """
        Verifies that the running method of the manager returns running tasks
        and nothing else
        """
        c_task_instance()
        c_task_instance(status=STATUS_RUNNING)
        c_task_instance(status=STATUS_FAILED)
        c_task_instance(status=STATUS_COMPLETE)
        c_task_instance(status=STATUS_CANCELLED)
        tasks = TaskInstance.objects.running()        
        self.assert_(tasks.count()==1, tasks.count())


class TaskScheduler_Test(unittest.TestCase):
    """
    Tests for the TaskScheduler - the class responsible for tracking and
    decision making for the task queue.
    """
    
    def setUp(self):
        self.tearDown()
        self.scheduler = TaskScheduler()
        self.scheduler.task_manager = TaskManagerProxy()
        self.manager = ModuleManagerProxy()
        self.scheduler._register(self.manager)

    def add_worker(self, connect=False):
        """ Helper function for adding a worker to the scheduler """
        
        worker = WorkerProxy('localhost:%d'%len(self.scheduler.workers))
        self.scheduler.workers[worker.name] = worker
        if connect:
            # connect the proxy worker fully so that it can be scheduled
            self.scheduler.worker_status_returned([WORKER_STATUS_IDLE], worker, worker.name)
        return worker

    def queue_and_run_task(self, success=None):
        """ Helper for setting up a running task """
        s = self.scheduler
        worker = self.add_worker(True)
        task = c_task_instance()
        s._init_queue()
        response = s._schedule()
        
        # complete start sequence for task, or fail it.  if no flag is given
        # task will be left waiting for response from remote worker.
        if success == True:
            s.run_task_successful(None, worker.name)
        elif success == False:
            s.run_task_failed(None, worker.name)
            
        return response, worker, task

    def queue_and_run_subtask(self, worker, success=None):
        """ Helper for setting up a running subtask """
        s = self.scheduler
        
        # hook _schedule so it isn't called immediately from request_worker
        # reset it after request_worker has been called
        def noop():
            pass
        _schedule =s._schedule
        s._schedule = noop
        
        subtask = s.request_worker(worker.name, 'test.foo.bar', 'args', 'workunit_key')
        s._schedule = _schedule # reset
        self.assert_(subtask, "subtask was not created")
        response = _schedule()
        
        # complete start sequence for subtask, or fail it.  if no flag is given
        # subtask will be left waiting for response from remote worker.
        if success == True:
            s.run_task_successful(None, worker.name, subtask.subtask_key)
        elif success == False:
            s.run_task_failed(None, worker.name, subtask.subtask_key)
            
        return response, subtask

    def assertCalled(self, worker, function):
        """
        Assertion function for checking if a worker had a specific callback
        called
        """
        for call in worker.calls:
            args, kwargs, deferred = call
            _function = args[0]
            if _function == function:
                # for now only check function name.  eventually this should
                # also check some set of parameters
                return
        self.assert_(False, 'Worker (%s) did not have %s called' % (worker.name, function))

    def tearDown(self):
        self.scheduler = None
        Batch.objects.all().delete()
        WorkUnit.objects.all().delete()
        TaskInstance.objects.all().delete()

    def validate_queue_format(self):
        """ helper function for validating format of objects in the queue. This
        function should be used by anything that adds or modifies the queue.
        This will help ensure that any changes to the queue structure will be
        detected in all places that it needs to be changed.
        """
        s = self.scheduler
        
        # check formatting of _queue
        for task in s._queue:
            self.assert_(isinstance(task, (list,)), type(task))
            score, taskinstance = task
            self.assert_(isinstance(score, (list,tuple)), type(score))
            self.assert_(isinstance(taskinstance, (TaskInstance,)), type(taskinstance))
        
        # check formatting of _active_tasks
        for key, value in s._active_tasks.items():
            self.assert_(isinstance(key, (long,)), type(key))
            self.assert_(isinstance(value, (TaskInstance,)), type(value))

    def test_init(self):
        """
        Verifies the queue starts empty
        """
        s = self.scheduler
        self.assertFalse(s._queue, s._queue)
        self.assertFalse(s._active_tasks, s._queue)
    
    def test_init_queued_tasks(self):
        """
        Verifies:
            * queued tasks will be added to queue on init
            * queued tasks are correctly formatted
        """
        ti = c_task_instance()
        s = self.scheduler
        s._init_queue()
        self.assert_(s._queue, s._queue)
        self.assert_(s._active_tasks, s._active_tasks)
        self.validate_queue_format()
    
    def test_init_running_tasks(self):
        """
        Verifies:
            * running tasks will be added to queue on init
            * queued tasks are correctly formatted
        """
        ti = c_task_instance(status=STATUS_RUNNING)
        s = self.scheduler
        s._init_queue()
        self.assert_(s._queue, s._queue)
        self.assert_(s._active_tasks, s._active_tasks)
        self.validate_queue_format()
    
    def test_worker_connected(self):
        """
        Verify:
            * that worker is status is polled
        """
        worker = self.add_worker()
        s = self.scheduler
        
        s.worker_connected(worker)
        self.assertCalled(worker, 'worker_status')
    
    def test_worker_status_returned_idle(self):
        """
        Verify that idle worker is added to idle pool
        """
        worker = self.add_worker()
        s = self.scheduler
        s.worker_status_returned([WORKER_STATUS_IDLE], worker, worker.name)
        self.assert_(len(s._idle_workers)==1, s._idle_workers)
        self.assertFalse(s._active_workers, s._active_workers)
        self.assertFalse(s._waiting_workers, s._waiting_workers)
        self.assert_(worker.name in s._idle_workers, (s._idle_workers, worker.name))
    
    def test_queue_task(self):
        """
        Verifies:
           * task is added to queue
           * scheduler is advanced
        """
        s = self.scheduler
        s._queue_task('test.foo')
        self.assert_(s._queue, s._queue)
        self.assert_(s._active_tasks, s._active_tasks)
        self.validate_queue_format()
        self.assert_(False, "validate that the scheduler was advanced")
    
    def test_queue_subtask(self):
        """
        Verifies:
            * Workunit is created and added to taskinstance
            * scheduler is advanced
        """
        self._schedule = False
        def instrumented_schedule():
            self._schedule = True
        
        s = self.scheduler
        response, worker, task = self.queue_and_run_task(True)
        s._schedule = instrumented_schedule
        task = s.get_worker_job(worker.name)
        s.request_worker(worker.name, 'test.foo.bar', 'args', 'workunit_key')
        
        # verify workunit object exists
        self.assert_(task.workunits.all().count()==1, task.workunits.all().count())
        
        # verify workunit is queued
        self.assert_(len(task._worker_requests)==1, task._worker_requests)
        self.assert_(task._worker_requests[0].workunit=='workunit_key', 'queued work request doesn''t match')
        
        # verify schedule is advanced        
        self.assert_(self._schedule, "scheduler wasn't advanced")
    
    def test_queue_subtask_unknown_task(self):
        """
        A subtask request is submitted from a worker not running a task
        
        Verifies:
            * nothing happens
        """
        raise NotImplementedError
    
    def test_advance_queue(self):
        """
        Verifies:
            * next task is identified
            * correct worker is assigned to task
        """
        response, worker, task = self.queue_and_run_task()
        
        # non null respsonse confirms scheduler advanced
        self.assert_(response, 'either worker or task was not found')
        
        # validate the worker and task
        worker_key, task_id = response
        self.assert_(task.id==task_id, (task.id, task_id))
        self.assert_(worker_key==worker.name, (worker_key, worker.name))
        
        # validate the remote call was made to the worker
        self.assert_(worker.calls, worker.calls)
        args, kwargs, deferred = worker.calls[0]
        call = args[0]
        self.assert_(call=='run_task', call)
    
    def test_advance_queue_empty(self):
        """
        Verify:
            * workers remain idle
            * CLUSTER_IDLE emitted
        """
        self.add_worker(True)
        s = self.scheduler
        s._init_queue()
        
        response = s._schedule()
        self.assertFalse(response, 'scheduler was advanced')
        self.assertTrue(s._idle_workers, s._idle_workers)
        self.assertFalse(s._active_workers, s._active_workers)
        self.assertFalse(s._waiting_workers, s._waiting_workers)
        self.assertFalse(s._active_tasks, s._active_tasks)
    
    def test_advance_queue_no_workers(self):
        """
        Verify:
            * task remains in queue
            * no worker states were changed
        """
        s = self.scheduler
        s._init_queue()
        
        response = s._schedule()
        self.assertFalse(response, 'scheduler was advanced')
        self.assertFalse(s._idle_workers, s._idle_workers)
        self.assertFalse(s._active_workers, s._active_workers)
        self.assertFalse(s._waiting_workers, s._waiting_workers)
        self.assertFalse(s._active_tasks, s._active_tasks)
    
    def test_advance_queue_subtask_only_main_worker(self):
        """
        Advance the queue for a subtask when only the mainworker is available
        
        Verifies:
            * scheduler was advanced
            * mainworker should be chosen
        """
        response, worker, task = self.queue_and_run_task(True)
        subtask_response, subtask = self.queue_and_run_subtask(worker)
        
        self.assert_(subtask_response, "Scheduler was not advanced")
        subtask_worker, subtask_id = subtask_response
        self.assert_(worker.name==subtask_worker, "Subtask is not started on main worker")
    
    def test_advance_queue_subtask_only_other_worker(self):
        """
        Advance the queue for a subtask when the mainworker is not available
        
        Verifies:
            * other worker should be chosen
        """
        response, main_worker, task = self.queue_and_run_task(True)
        task = self.scheduler.get_worker_job(main_worker.name)
        task.local_workunit = True
        other_worker = self.add_worker(True)
        subtask_response, subtask = self.queue_and_run_subtask(main_worker)
        
        self.assert_(subtask_response, "Scheduler was not advanced")
        subtask_worker, subtask_id = subtask_response
        self.assert_(other_worker.name==subtask_worker, "Subtask is not started on other worker")
    
    def test_advance_queue_subtask(self):
        """
        Advance the queue for a subtask when both the mainworker and another
        worker are available.
        
        Verifies:
            * mainworker should be chosen
        """
        response, main_worker, task = self.queue_and_run_task(True)
        other_worker = self.add_worker(True)
        subtask_response, subtask = self.queue_and_run_subtask(main_worker)
        
        self.assert_(subtask_response, "Scheduler was not advanced")
        subtask_worker, subtask_id = subtask_response
        self.assert_(main_worker.name==subtask_worker, "Subtask is not started on main worker")
    
    def test_run_task_successful(self):
        """
        Verify that:
            * task instance is updated
            * mainworker is recorded
        """
        s = self.scheduler
        response, worker, task = self.queue_and_run_task(True)
        task = TaskInstance.objects.get(id=task.id)
        
        # verify task instance
        self.assert_(task.status==STATUS_RUNNING, task.status)
        self.assert_(task.worker==worker.name, task.worker)
        
        # verify mainworker is recorded
        self.assert_(len(s._main_workers)==1, s._main_workers)
        self.assert_(worker.name in s._main_workers, s._main_workers)
    
    def test_run_subtask_successful(self):
        """
        Verify that:
            * workunit is updated
            * main worker is notified
        """
        response, main_worker, task = self.queue_and_run_task(True)
        subtask_response, subtask = self.queue_and_run_subtask(main_worker, True)
        subtask = WorkUnit.objects.get(id=subtask.id)
        
        # verify task instance
        self.assert_(subtask.status==STATUS_RUNNING, subtask.status)
        self.assert_(subtask.worker==main_worker.name, subtask.worker)
        
        # verify mainworker is notified
        self.assertCalled(main_worker, 'subtask_started')
    
    def test_run_task_failed(self):
        """
        Verify that:
            * worker is returned to idle pool
            * worker is not in other lists
            * task is removed from queue
            * task is marked failed
            * scheduler is advanced
        """
        s = self.scheduler
        response, worker, task = self.queue_and_run_task(False)
        task = TaskInstance.objects.get(id=task.id)
        
        # validate worker status
        self.assertFalse(s._active_workers, s._active_workers)
        self.assertFalse(s._waiting_workers, s._waiting_workers)
        self.assertFalse(s._main_workers, s._main_workers)
        self.assert_(len(s._idle_workers)==1, s._idle_workers)
        self.assert_(worker.name in s._idle_workers, (s._idle_workers, worker.name))
        
        # validate task is not in queue
        self.assertFalse(s._queue, s._queue)
        self.assertFalse(s._active_tasks, s._active_tasks)
        
        # validate task is failed
        self.assert_(task.status==STATUS_FAILED, task.status)
        self.assert_(False, "validate that the scheduler was advanced")
    
    def test_run_subtask_failed_mainworker(self):
        """
        Verifies:
            * mainworker flags are reset
            * mainworker remains in correct pools
            * TODO: subtask is removed from queue <<<< ????
            * subtask is marked failed
            * scheduler is advanced
        """
        s = self.scheduler
        response, main_worker, task = self.queue_and_run_task(True)
        subtask_response, subtask = self.queue_and_run_subtask(main_worker, True)
        subtask = WorkUnit.objects.get(id=subtask.id)
        task = s.get_worker_job(main_worker.name)
        
        # validate worker flags
        self.assertFalse(task.local_workunit, "Local workunit flag not cleared")
        
        # validate worker status
        self.assertFalse(s._waiting_workers, s._waiting_workers)
        self.assertFalse(s._main_workers, s._main_workers)
        self.assertFalse(s._idle_workers, s._idle_workers)
        self.assert_(s._active_workers, s._active_workers)
        self.assert_(subtask.worker.name in s._active_workers, (s._active_workers, subtask.worker))
        
        # validate task is not in queue
        self.assertFalse(s._queue, s._queue)
        self.assertFalse(s._active_tasks, s._active_tasks)
        
        # validate task is failed
        self.assert_(task.status==STATUS_FAILED, task.status)
        self.assert_(False, "validate that the scheduler was advanced")
    
    def test_run_subtask_failed_otherworker(self):
        """
        Verifies:
            * subtask worker is moved to idle pool
            * mainworker remains in correct pool
            * TODO: subtask is removed from queue <<<< ????
            * subtask is marked failed
            * scheduler is advanced
        """
        s = self.scheduler
        response, main_worker, task = self.queue_and_run_task(True)
        task = s.get_worker_job(main_worker.name)
        task.local_workunit = True
        subtask_worker = self.add_worker(True)
        subtask_response, subtask = self.queue_and_run_subtask(main_worker)
        
        # validate worker status
        self.assertFalse(len(s._active_workers)==1, s._active_workers)
        self.assert_(subtask_worker.name in s._idle_workers, (s._idle_workers, subtask_worker.name))
        self.assertFalse(s._waiting_workers, s._waiting_workers)
        self.assertFalse(s._main_workers, s._main_workers)
        self.assert_(len(s._idle_workers)==1, s._idle_workers)
        self.assert_(subtask_worker.name in s._idle_workers, (s._idle_workers, subtask_worker.name))
        
        # validate task is not in queue
        self.assertFalse(s._queue, s._queue)
        self.assertFalse(s._active_tasks, s._active_tasks)
        
        # validate task is failed
        self.assert_(task.status==STATUS_FAILED, task.status)
        self.assert_(False, "validate that the scheduler was advanced")
    
    def test_task_completed(self):
        """
        Verifies:
            * task is removed from active list
            * task is recorded as completed
            * worker is removed from active pool
            * scheduler is advanced
        """
        s = self.scheduler
        response, worker, task = self.queue_and_run_task(True)
        s.send_results(worker.name, ((None, 'results: woot!', False),))
        task = TaskInstance.objects.get(id=task.id)
    
        # validate task queue
        self.assertFalse(s._queue, s._queue)
        self.assertFalse(s._active_tasks, s._active_tasks)
    
        # validate task status
        self.assert_(task.status==STATUS_COMPLETE)
    
        # validate worker status
        self.assertFalse(s._active_workers, s._active_workers)
        self.assertFalse(s._waiting_workers, s._waiting_workers)
        self.assertFalse(s._main_workers, s._main_workers)
        self.assert_(len(s._idle_workers)==1, s._idle_workers)
        self.assert_(worker.name in s._idle_workers, (s._idle_workers, worker.name))
        
        # validate scheduler advanced
        self.assert_(False, "validate that the scheduler was advanced")
    
    def test_subtask_completed(self):
        """
        Verifies:
            * Worker is moved to waiting (held) pool
            * Mainworker remains in active pool
            * Mainworker is notified of completion
            * Scheduler is advanced
        """
        raise NotImplementedError
    
    def test_cancel_task(self):
        """
        Cancel task that is waiting in the pool
        
        Verifies:
            * task is removed from queue
        """
        ti = c_task_instance()
        s = self.scheduler
        s._init_queue()
        s.cancel_task(ti.id)        
        self.assertFalse(s._queue, s._queue)
        self.assertFalse(s._active_tasks, s._active_tasks)

    def test_cancel_task_running(self):
        """
        Verifies:
            * Worker moved from active to idle pool
            * Scheduler is advanced
        """
        raise NotImplementedError
    
    def test_cancel_task_running_with_subtasks(self):
        """
        Verifies:
            * Task is stopped
            * SubTasks are stopped
            * Mainworker remains in active pool
            * Worker is moved to waiting (held) pool
            * Scheduler is advanced
        """
        raise NotImplementedError
    
    def test_release_waiting_worker(self):
        """
        Verifies:
            * Waiting worker is moved from waiting pool to idle pool
            * Scheduler is advanced
        """
        raise NotImplementedError
    