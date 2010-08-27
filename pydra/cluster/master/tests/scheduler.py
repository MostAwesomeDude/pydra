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
import time

from twisted.internet.defer import Deferred

from pydra.cluster.constants import *
from pydra.cluster.master import scheduler
from pydra.cluster.tasks import *
from pydra.models import TaskInstance, WorkUnit, Batch
from pydra.tests import django_testcase as django
from pydra.tests.proxies import ModuleManagerProxy, ThreadsProxy, CallProxy, RemoteProxy

def suite():
    """
    Build a test suite from all the test suites in this module
    """
    return unittest.TestSuite([
            unittest.TestLoader().loadTestsFromTestCase(TaskScheduler_Scheduling),
            unittest.TestLoader().loadTestsFromTestCase(TaskScheduler_Statuses),
            unittest.TestLoader().loadTestsFromTestCase(TaskScheduler_Models_Test),
        ])


# constants used by the tests
WORKER_IDLE = 0
WORKER_WAITING = 1
WORKER_ACTIVE= 2


class TaskPackageProxy():
    version = 'version 1.0'


class TaskManagerProxy():
    """
    Proxy of TaskManager used for testing
    """
    def get_task_package(self, task):
        return TaskPackageProxy()


def c_task_instance(**kwargs):
    """ Creates a task instance for testing """
    task_instance = TaskInstance()
    task_instance.task_key = 'foo.bar'
    task_instance.status = STATUS_STOPPED
    task_instance.__dict__.update(kwargs)
    task_instance.save()
    return task_instance 


class TaskScheduler_Models_Test(django.TestCase):
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

class TaskScheduler_Base(django.TestCase):
    """
    Base Test class for TaskScheduler - the class responsible for tracking and
    decision making for the task queue. This class contains some base setup
    and utility methods.
    """
    def setUp(self):
        self.tearDown()
        self.scheduler = scheduler.TaskScheduler()
        self.scheduler.task_manager = TaskManagerProxy()
        self.manager = ModuleManagerProxy()
        self.scheduler._register(self.manager)
        
        # intercept _schedule so that its calls can be recorded
        self.scheduler._schedule_real = self.scheduler._schedule
        self.scheduler._schedule = CallProxy(self.scheduler._schedule)
        
        # hook the threads modules in the scheduling module so we can intercept
        # any calls to deferToThread()
        self.threads_ = ThreadsProxy(self)
        scheduler.threads = self.threads_


    def add_worker(self, connect=False):
        """ Helper function for adding a worker to the scheduler """
        
        worker = RemoteProxy('localhost:%d'%len(self.scheduler.workers))
        self.scheduler.workers[worker.name] = worker
        if connect:
            # connect the proxy worker fully so that it can be scheduled
            self.scheduler.worker_status_returned([WORKER_STATUS_IDLE], worker, worker.name)
        return worker

    def queue_and_run_task(self, success=None):
        """ Helper for setting up a running task """
        s = self.scheduler
        worker = self.add_worker(True)
        task = s._queue_task('foo.bar')
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
                return call
        self.fail('Worker (%s) did not have %s called' % (worker.name, function))

    def assertWorkerStatus(self, worker, status, scheduler, main=True):
        """
        Assertion function for checking the status of a worker.  This
        encapsulates checking the presence of the worker in various pools which
        denote its state.  This ensures that if the code for tracking worker
        state changes, then only this assertion needs to change.
        """        
        if status==WORKER_IDLE:
            in_ = '_idle_workers'
            not_in = ['_active_workers','_waiting_workers']
            self.assertFalse(worker.name in scheduler._main_workers, "Worker (%s) shouldn't be in main workers" % worker.name)
            
        elif status==WORKER_WAITING:
            in_ = '_waiting_workers'
            not_in = ['_active_workers','_idle_workers']
        
        elif status==WORKER_ACTIVE:
            in_ = '_active_workers'
            not_in = ['_waiting_workers','_idle_workers']
            
            if main:
                self.assert_(worker.name in scheduler._main_workers, "Worker (%s) isn't in main workers" % worker.name)
        
        for pool in not_in:
            self.assertFalse(worker.name in getattr(scheduler, pool), "Worker (%s) shouldn't be in %s" % (worker.name,pool))
        self.assert_(worker.name in getattr(scheduler, in_), "Worker (%s) isn't in %s" % (worker.name,in_))

    def assertSchedulerAdvanced(self):
        """ asserts that the scheduler was attempted to be advanced using either
        deferToThread or directly called
        """
        self.assert_(self.scheduler._schedule.calls
            or self.threads_.was_deferred(self.scheduler._schedule),
            "No Attempt was made to advance the scheduler")
        
        # clear calls
        self.scheduler._schedule.calls = []
        self.threads_.calls = []

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
            self.assert_(isinstance(key, (long, int)), type(key))
            self.assert_(isinstance(value, (TaskInstance,)), type(value))

    
    
class TaskScheduler_Scheduling(TaskScheduler_Base):
    """
    Tests for the TaskScheduler involving scheduling tasks and running them
    """
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
        self.assertWorkerStatus(worker, WORKER_IDLE, s)

    
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
        self.assertSchedulerAdvanced()
    
    def test_queue_subtask(self):
        """
        Verifies:
            * Workunit is created and added to taskinstance
            * scheduler is advanced
        """
        s = self.scheduler
        response, worker, task = self.queue_and_run_task(True)
        s._schedule.enabled = False
        task = s.get_worker_job(worker.name)
        s.request_worker(worker.name, 'test.foo.bar', 'args', 'workunit_key')
        
        # verify workunit object exists
        self.assert_(task.workunits.all().count()==1, task.workunits.all().count())
        
        # verify workunit is queued
        self.assert_(len(task._worker_requests)==1, task._worker_requests)
        self.assert_(task._worker_requests[0].workunit=='workunit_key', 'queued work request doesn''t match')
        
        # verify schedule is advanced        
        self.assert_(s._schedule.calls, "scheduler wasn't advanced")
    
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
        worker = self.add_worker(True)
        s = self.scheduler
        s._init_queue()
        
        response = s._schedule()
        self.assertFalse(response, 'scheduler was advanced')
        self.assertWorkerStatus(worker, WORKER_IDLE, s)
    
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
        other_worker = self.add_worker(True)
        # queue work on mainworker
        subtask_response, subtask = self.queue_and_run_subtask(main_worker, True)
        # queue work on other worker
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
        self.assertWorkerStatus(worker, WORKER_ACTIVE, s, True)
    
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
        self.assertWorkerStatus(worker, WORKER_IDLE, s)
        
        # validate task is not in queue
        self.assertFalse(s._queue, s._queue)
        self.assertFalse(s._active_tasks, s._active_tasks)
        
        # validate task is failed
        self.assert_(task.status==STATUS_FAILED, task.status)
        self.assertSchedulerAdvanced()
    
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
        self.assertSchedulerAdvanced()
    
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
        self.assertSchedulerAdvanced()
    
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

        self.assert_(task.status==STATUS_COMPLETE)
        self.assertWorkerStatus(worker, WORKER_IDLE, s)
        self.assertSchedulerAdvanced()
    
    def test_subtask_completed(self):
        """
        subtask on non-main_worker completes
        
        Verifies:
            * Worker is moved to waiting (held) pool
            * Mainworker remains in active pool
            * Mainworker is notified of completion
            * Scheduler is advanced
        """
        s = self.scheduler
        response, main_worker, task = self.queue_and_run_task(True)
        task = self.scheduler.get_worker_job(main_worker.name)
        other_worker = self.add_worker(True)
        # queue work on mainworker
        subtask_response, subtask = self.queue_and_run_subtask(main_worker, True)
        # queue work on other worker
        subtask_response, subtask = self.queue_and_run_subtask(main_worker, True)
        s.send_results(other_worker.name, ((subtask.subtask_key, 'results: woot!', False),))
        
        # validate worker status
        self.assertWorkerStatus(main_worker, WORKER_ACTIVE, s, True)
        self.assertWorkerStatus(other_worker, WORKER_WAITING, s)
        self.assert_(other_worker.name in task.waiting_workers)
        
        # validate main_worker is notified
        self.assertCalled(main_worker, 'receive_results')
        self.assertSchedulerAdvanced()
    
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
            * task removed from _queue
            * request sent to worker to stop
            * worker is not yet stopped
            
            after mainworker stop:
            * task marked canceled
            * worker returned to idle pool
        """
        s = self.scheduler
        response, worker, task = self.queue_and_run_task(True)
        task = self.scheduler.get_worker_job(worker.name)
        s.cancel_task(task.id) 
        self.assertCalled(worker, 'stop_task')
        self.assertWorkerStatus(worker, WORKER_ACTIVE, s, True)
        self.assertFalse(s._queue, "Task should be removed from _queue")
        
        # verify main worker is stopped
        s.worker_stopped(worker.name)
        self.assertWorkerStatus(worker, WORKER_IDLE, s)
        self.assert_(task.status == STATUS_CANCELLED, task.status)
        self.assertSchedulerAdvanced()
    
    def test_cancel_task_running_with_subtasks(self):
        """
        Verifies:
            * task removed from _queue
            * request sent to main worker to stop it
            * request sent to workers to stop them
            * Mainworker remains in active pool
            * Worker remains in active pool
            
            after mainworker stop:
            * task marked canceled
            * worker returned to idle pool
            
            after subtask worker stop:
            * worker returned to idle pool
        """
        s = self.scheduler
        response, main_worker, task = self.queue_and_run_task(True)
        task = self.scheduler.get_worker_job(main_worker.name)
        other_worker = self.add_worker(True)
        subtask_response, subtask = self.queue_and_run_subtask(main_worker, True)
        subtask_response, subtask = self.queue_and_run_subtask(main_worker, True)
        s.cancel_task(task.id)
        
        # check that stop requests sent
        self.assertFalse(s._queue, "Task should be removed from _queue")
        self.assertWorkerStatus(main_worker, WORKER_ACTIVE, s, True)
        self.assertWorkerStatus(other_worker, WORKER_ACTIVE, s, False)
        
        # verify main worker is stopped
        s.worker_stopped(main_worker.name)
        self.assertWorkerStatus(main_worker, WORKER_IDLE, s)
        self.assert_(task.status == STATUS_CANCELLED, task.status)
        self.assertSchedulerAdvanced()
        
        # verify subtask worker is stopped
        s.worker_stopped(other_worker.name)
        self.assertWorkerStatus(other_worker, WORKER_IDLE, s)
        self.assertSchedulerAdvanced()    
    
    def test_cancel_task_running_with_held_workers(self):
        """
        Verifies:
            * task removed from _queue
            * request sent to main worker to stop it
            * request sent to workers to stop them
            * Mainworker remains in active pool
            * Worker remains in active pool
            
            after mainworker stop:
            * task marked canceled
            * worker returned to idle pool
            
            after subtask worker stop:
            * worker returned to idle pool
        """
        
        s = self.scheduler
        response, main_worker, task = self.queue_and_run_task(True)
        task = self.scheduler.get_worker_job(main_worker.name)
        task.local_workunit = task
        other_worker = self.add_worker(True)
        subtask_response, subtask = self.queue_and_run_subtask(main_worker, True)
        s.send_results(other_worker.name, ((subtask.subtask_key, 'results: woot!', False),))
        s.cancel_task(task.id)
        
        # check that stop requests sent, but workers remain active
        self.assertFalse(s._queue, "Task should be removed from _queue")
        self.assertWorkerStatus(main_worker, WORKER_ACTIVE, s, True)
        self.assertWorkerStatus(other_worker, WORKER_WAITING, s)
        
        # verify main worker is stopped
        s.worker_stopped(main_worker.name)
        self.assertWorkerStatus(main_worker, WORKER_IDLE, s)
        self.assert_(task.status == STATUS_CANCELLED, task.status)
        self.assertSchedulerAdvanced()
        
        # verify subtask worker is stopped
        s.worker_stopped(other_worker.name)
        self.assertWorkerStatus(other_worker, WORKER_IDLE, s)
        self.assertSchedulerAdvanced()    
    
    def test_release_waiting_worker(self):
        """
        Verifies:
            * Waiting worker is moved from waiting pool to idle pool
            * Scheduler is advanced
        """
        s = self.scheduler
        response, main_worker, task = self.queue_and_run_task(True)
        task = self.scheduler.get_worker_job(main_worker.name)
        task.local_workunit = task
        other_worker = self.add_worker(True)
        subtask_response, subtask = self.queue_and_run_subtask(main_worker, True)
        s.send_results(other_worker.name, ((subtask.subtask_key, 'results: woot!', False),))
        s.request_worker_release(main_worker.name)
    
        self.assertWorkerStatus(main_worker, WORKER_ACTIVE, s, True)
        self.assertWorkerStatus(other_worker, WORKER_IDLE, s)
        self.assertFalse(other_worker.name in task.waiting_workers)
        self.assertSchedulerAdvanced()


class TaskScheduler_Statuses(TaskScheduler_Base):
    """
    Tests for the TaskScheduler involving retrieving task statuses

    Verifies:
        * returns a deferred instead of status
        * worker has remote call issues
        * deferred returns status after status returned from call
        * task status is recorded properly
    """
    def setUp(self):
        super(TaskScheduler_Statuses, self).setUp()
        self.status = None

    def set_status(self, status):
        self.status = status

    def assertVerifyStatus(self, task, status=STATUS_STOPPED, progress=-1):
        """
        helper for verifying a status is reported as expected.
        
        Verifies:
            * task is in the dict
            * dict is properly formatted:
                * queued tasks only have status indicating as such
                * running tasks have progress, status, and starttime.
        """
        self.assert_(task.id in self.status, 'task status is not in dict')
        task_status = self.status[task.id]
        
        self.assert_(status==task_status['s'], 'Status does not match')
        
        if status==STATUS_STOPPED:
            self.assert_(task_status.keys()==['s'],'status for queued tasks only requires status flag')
        else:
            self.assert_(progress==task_status['p'], 'Progress does not match')
            self.assert_(task_status['t'], 'Progress does not have a start time')

    def test_get_status(self):
        response, worker, task = self.queue_and_run_task(True)
        s = self.scheduler
        deferred = s.fetch_task_status()        
        self.assert_(isinstance(deferred, (Deferred,)), "Status should be a deferred")
        deferred.addCallback(self.set_status)
        
        args, kwargs, call_deferred = self.assertCalled(worker, 'task_status')
        call_deferred.callback(50)
        
        # verify that status is set by deferred. after last status callback
        self.assert_(self.status!=None, "status should have been set")
        self.assertVerifyStatus(task, STATUS_RUNNING, 50)
        return self.status

    def test_get_status_empty_queue(self):
        """
        Tests call to get status when no tasks are queued
        
        Verifies:
            * status is returned immediately as a dict
            * status is empty
        """
        s = self.scheduler
        self.status = s.fetch_task_status()
        self.assert_(isinstance(self.status, (dict,)), 'status should be a dictionary')
        self.assert_(len(self.status)==0, 'status should be empty')

    def test_get_status_all_queued(self):
        """
        no running tasks, only queued tasks
        
        Verifies:
            * status is returned immediately as a dict
            * statuses are marked as not running
        """
        s = self.scheduler
        task0 = s._queue_task('test.foo')
        task1 = s._queue_task('test.foo')
        self.status = s.fetch_task_status()
        
        self.assert_(isinstance(self.status, (dict,)), 'status should be a dictionary')
        self.assertVerifyStatus(task0)
        self.assertVerifyStatus(task1)
    
    def test_get_status_multiple_running(self):
        """
        Tests status when queue contains multiple running tasks:
        
        Verify:
            * status is returned as deferred
            * status is recorded and callback called when all statuses returned
            * status contains both properly formatted status objects
        """
        s = self.scheduler    
        response0, worker0, task0 = self.queue_and_run_task(True)
        response1, worker1, task1 = self.queue_and_run_task(True)
        deferred = s.fetch_task_status()
        
        self.assert_(isinstance(deferred, (Deferred,)), "Status should be a deferred")
        deferred.addCallback(self.set_status)
        
        args0, kwargs0, call_deferred0 = self.assertCalled(worker0, 'task_status')
        call_deferred0.callback(10)
        args1, kwargs1, call_deferred1 = self.assertCalled(worker1, 'task_status')
        call_deferred1.callback(20)        
        
        # verify that status is set by deferred. after last status callback
        self.assert_(self.status!=None, "status should have been set")
        self.assertVerifyStatus(task0, STATUS_RUNNING, 10)
        self.assertVerifyStatus(task1, STATUS_RUNNING, 20)

    def test_get_status_mixed_queue(self):
        """
        Tests status when queue contains both queued and running tasks:
        
        Verify:
            * status is returned as deferred
            * status is recorded and callback called
            * status contains both properly formatted status objects
        """
        s = self.scheduler
        response0, worker0, task0 = self.queue_and_run_task(True)
        task1 = s._queue_task('test.foo')
        deferred = s.fetch_task_status()
        self.assert_(isinstance(deferred, (Deferred,)), "Status should be a deferred")
        deferred.addCallback(self.set_status)
        
        args, kwargs, call_deferred = self.assertCalled(worker0, 'task_status')
        call_deferred.callback(10)
        
        # verify that status is set by deferred. after last status callback
        self.assert_(self.status!=None, "status should have been set")
        self.assertVerifyStatus(task0, STATUS_RUNNING, 10)
        self.assertVerifyStatus(task1)
    
    def test_get_status_status_pending(self):
        """
        Subsequent calls to get_status while remote calls are pending
        
        Verify:
            * deferred is returned from get_status
            * deferred is the same as the original deferred
        """
        s = self.scheduler
        response, worker, task = self.queue_and_run_task(True)
        deferred0 = s.fetch_task_status()
        deferred1 = s.fetch_task_status()
        self.assert_(isinstance(deferred1, (Deferred,)), "Second status should be a deferred")
        self.assert_(deferred0==deferred1, "deferreds should be the same object: %s vs %s" % (deferred0, deferred1))
    
    def test_get_status_cached(self):
        """
        Tests that result is cached
        
        Verify:
            * status is the same object
        """
        s = self.scheduler
        self.test_get_status()
        status = s.fetch_task_status()
        self.assert_(isinstance(status, (dict,)), "Second Status should be a dict: %s" % status )
        self.assert_(id(status)==id(self.status), "Status is not the same object")
    
    def test_get_status_cached_all_queued(self):
        """
        Tests that result is cached
        
        Verify:
            * status is the same object
        """
        s = self.scheduler
        task = s._queue_task('test.foo')
        
        self.status = s.fetch_task_status()
        self.assert_(isinstance(self.status, (dict,)), "Status should be a dict")
        self.assertVerifyStatus(task)
        
        status = s.fetch_task_status()
        self.assert_(isinstance(status, (dict,)), "Second status should be a dict")
        self.assert_(id(status)==id(self.status), "Status is not the same object")

    def test_get_status_cache_expired(self):
        """
        Verify that time delay resets cache
        """        
        response, worker, task = self.queue_and_run_task(True)
        s = self.scheduler
        deferred = s.fetch_task_status()        
        self.assert_(isinstance(deferred, (Deferred,)), "Status should be a deferred")
        deferred.addCallback(self.set_status)
        
        args, kwargs, call_deferred = self.assertCalled(worker, 'task_status')
        call_deferred.callback(50)
        # verify that status is set by deferred. after last status callback
        self.assert_(self.status!=None, "status should have been set")
        self.assertVerifyStatus(task, STATUS_RUNNING, 50)
        
        # check initial status call
        # give time to expire, then retry.
        time.sleep(4)
        worker.calls = []
        deferred = s.fetch_task_status()        
        self.assert_(isinstance(deferred, (Deferred,)), "Status should be a deferred")
        deferred.addCallback(self.set_status)
        
        args, kwargs, call_deferred = self.assertCalled(worker, 'task_status')
        call_deferred.callback(50)
        # verify that status is set by deferred. after last status callback
        self.assert_(self.status!=None, "status should have been set")
        self.assertVerifyStatus(task, STATUS_RUNNING, 50)

    def test_get_status_cache_expired_all_queued(self):
        """
        Tests that result is cached
        
        Verify:
            * status is the same object
        """
        s = self.scheduler
        task = s._queue_task('test.foo')
        
        self.status = s.fetch_task_status()
        self.assert_(isinstance(self.status, (dict,)), "Status should be a dict")
        self.assertVerifyStatus(task)
        time.sleep(4)
        status = s.fetch_task_status()
        self.assert_(isinstance(status, (dict,)), "Second status should be a dict")
        self.assert_(id(status)!=id(self.status), "Status should not be the same object")