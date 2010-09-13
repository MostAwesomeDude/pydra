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
import time
import unittest
from threading import Event

from twisted.trial import unittest as twisted_unittest
from twisted.internet import threads

from pydra.cluster.tasks import TaskNotFoundException, STATUS_STOPPED, \
    STATUS_COMPLETE, STATUS_RUNNING
from pydra.cluster.tasks.parallel_task import ParallelTask
from pydra.cluster.tasks.datasource.slicer import IterSlicer

from pydra.tests.proxies import CallProxy
from pydra.tests.cluster.tasks.proxies import WorkerProxy
from pydra.tests.cluster.tasks.test_tasks import StandaloneTask


class TestParallelTask(ParallelTask):
    """
    Example class for running tests in parallel
    """
    datasource = IterSlicer, range(10)
    description = 'A demo task illustrating a Parallel task.  This task runs 5 TestTasks at the same time'

    def __init__(self):
        ParallelTask.__init__(self)
        self.set_subtask(StandaloneTask, 'subtask')
        self._finished = []
        self.complete=False

    def work_unit_complete(self, data, results):
        self._finished.append(results)

    def work_complete(self):
        self.complete = True


class ParallelTaskTwistedTest(twisted_unittest.TestCase):
    """
    Test ParllelTask functionality that requires twisted to run
    """
    
    def setUp(self):
        self.pt = TestParallelTask()
        self.worker = WorkerProxy()
        self.pt.parent = self.worker
        self.callback = CallProxy(None, False)

    def test_request_workers(self):
        """
        Tests requesting all work units
        
        Verifies:
            * work_request called for every work unit
        """
        pt = self.pt
        pt.request_workers()
        self.assertEqual(pt._workunit_count, 10, "Workunit count is not correct")
        self.assertEqual(len(pt._data_in_progress), 10, "in progress count is not correct")
        self.assertEqual(len(self.worker.request_worker.calls), 10, "request_worker was not called the correct number of times")

    def test_progress_stopped(self):
        self.assertEquals(self.pt.status(), STATUS_STOPPED)

    def verify_parallel_work(self):
        """
        helper function for verifying parllel task.  must be called from
        deferToThread()
        
        Verifies Start:
            * status is marked running
            * work_units in progress is correct
            * work_unit count is correct
            * work_request called for every work unit
        
        Verifies Workunit completion:
            * _data_in_progress is removed
            * custom subtask completion function is run
            * worker is released if no more work is pending
            
        Verifies All workunits complete:
            * all workers are released
            * custom completion function is run
            * task is marked complete
            * task returns response
        """
        # sleep for 0.1 second to allow start thread to finish requesting workes
        time.sleep(.1)
        pt = self.pt
        self.assertEqual(pt.status(), STATUS_RUNNING)
        self.assertEqual(pt._workunit_count, 10, "Workunit count is not correct")
        self.assertEqual(len(pt._data_in_progress), 10, "in progress count is not correct")
        self.assertEqual(len(self.worker.request_worker.calls), 10, "request_worker was not called the correct number of times")
        self.callback.assertNotCalled(self)
        
        for i in range(10):
            self.assertEqual(pt.status(), STATUS_RUNNING)
            pt._work_unit_complete(i, i)
            self.assertEqual(pt._workunit_count, 10, "Workunit count is not correct")
            self.assertEqual(len(pt._data_in_progress), 9-i, "in progress count is not correct")
        
        self.assert_(pt.complete, 'task is not marked complete via completion code')
        self.assertEquals(pt.status(), STATUS_COMPLETE, "Status is not reporting completed")
        self.assertEqual(len(self.worker.request_worker_release.calls), 1, "request_worker_release was not called the correct number of times")

    def test_parallel_work(self):
        """
        Tests requesting all work units
        """
        pt = self.pt
        pt.start()
        return threads.deferToThread(self.verify_parallel_work)

    def test_worker_failed(self):
        """
        Work being returned due to worker failure failure:
        
        Verifies work is removed from in progress
        """
        pt = self.pt
        pt.request_workers()
        
        for i in range(10):
            pt._worker_failed(i)
            self.assert_(i not in pt._data_in_progress, 'workunit still in progress')
            self.assertEquals(len(pt._data_in_progress), 9-i, 'Data in progress is wrong size')
            
        self.assertEquals(len(pt._data_in_progress), 0, 'Data in progress is wrong size')
    
    def verify_batch_complete(self):
        """
        helper function for verifying batch completiion of parallel task
        subtask.  Must be called from deferToThread()
        
        Verifies Start:
            * status is marked running
            * work_units in progress is correct
            * work_unit count is correct
            * work_request called for every work unit
        
        Verifies Workunit completion:
            * _data_in_progress is removed
            * custom subtask completion function is run
            * worker is released if no more work is pending
            
        Verifies All workunits complete:
            * all workers are released
            * custom completion function is run
            * task is marked complete
            * task returns response
        """
        # sleep for 0.1 second to allow start thread to finish requesting workes
        time.sleep(.1)
        pt = self.pt
        self.assertEqual(pt.status(), STATUS_RUNNING)
        self.assertEqual(pt._workunit_count, 10, "Workunit count is not correct")
        self.assertEqual(len(pt._data_in_progress), 10, "in progress count is not correct")
        self.assertEqual(len(self.worker.request_worker.calls), 10, "request_worker was not called the correct number of times")
        
        for i in range(0,10,2):
            self.assertEqual(pt.status(), STATUS_RUNNING)
            results = ((i, i, 0), (i+1, i+1, 0))
            pt._batch_complete(results)
            self.assertEqual(pt._workunit_count, 10, "Workunit count is not correct")
            self.assertEqual(len(pt._data_in_progress), 8-i, "in progress count is not correct")
        
        self.assert_(pt.complete, 'task is not marked complete via completion code')
        self.assertEquals(pt.status(), STATUS_COMPLETE, "Status is not reporting completed")
        self.assertEqual(len(self.worker.request_worker_release.calls), 1, "request_worker_release was not called the correct number of times")

    def test_batch_complete(self):
        """
        Tests completing batched workunits
        """
        pt = self.pt
        pt.start(callback=self.callback)
        return threads.deferToThread(self.verify_batch_complete)


class ParallelTaskStandaloneTest(twisted_unittest.TestCase):
    """
    Test `ParallelTask` functionality without actually running the Twisted
    reactor.
    """

    def setUp(self):
        self.pt = TestParallelTask()

    def test_trivial(self):
        pass

    def test_get_work_units(self):
        s = set()
        for i, work_unit in enumerate(self.pt.get_work_units()):
            data, index = work_unit
            s.add(index)
            self.assertEqual(data, i)
        self.assertEqual(s, set(self.pt._data_in_progress.keys()))
        self.assertEqual(self.pt._workunit_count, 10, "Workunit count is not correct")
        self.assertEqual(len(self.pt._data_in_progress), 10, "in progress count is not correct")

    def test_set_subtask(self):
        """
        Tests setting the subtask of parallelTask
        """
        args = 1, 2, 3
        kwargs = {'a':1}
        pt = ParallelTask()
        pt.set_subtask(StandaloneTask, *args, **kwargs)
        self.assertEqual(pt._subtask_class, StandaloneTask, 'Class is not the same')
        self.assertEqual(pt._subtask_args, args, 'args are not the same')
        self.assertEqual(pt._subtask_kwargs, kwargs, 'kwargs are not the same')

    def test_from_subtask(self):
        """
        Tests creating a ParallelTask class using ParallelTask.from_subtask()
        """
        args = 1, 2, 3
        kwargs = {'a':1}
        pt = ParallelTask.from_subtask(StandaloneTask, *args, **kwargs)
        self.assertEqual(pt._subtask_class, StandaloneTask, 'Class is not the same')
        self.assertEqual(pt._subtask_args, args, 'args are not the same')
        self.assertEqual(pt._subtask_kwargs, kwargs, 'kwargs are not the same')

    def test_get_subtask(self):
        """
        Tests ParallelTask.get_subtask()
        
        Verify:
            * Subtask is retrieved
            * calling get_subtask a 2nd time returns the same instance
        """
        key = ['TestParallelTask','StandaloneTask']
        subtask = self.pt.get_subtask(key)
        self.assertNotEqual(subtask, None, 'Subtask should never be none')
        self.assert_(isinstance(subtask, (StandaloneTask,)))
        
        # same copy should be returned
        self.assertEqual(subtask, self.pt.get_subtask(key) , 'Subtask is not the same instance')

    def test_get_subtask_clean(self):
        """
        Tests ParallelTask.get_subtask(clean=True)
        
        Verifies:
            * same instance is returned if status==STATUS_STOPPED
            * a new instance is returned otherwise
        """
        key = ['TestParallelTask','StandaloneTask']
        subtask0 = self.pt.get_subtask(key)
        subtask1 = self.pt.get_subtask(key, clean=True)
        self.assertEqual(subtask0, self.pt.get_subtask(key) , 'Subtask is not the same instance')
        
        for status in [STATUS_COMPLETE, STATUS_RUNNING]:
            subtask0 = subtask1
            subtask0._status = status
            subtask1 = self.pt.get_subtask(key, clean=True)
            self.assertNotEqual(subtask0, subtask1, 'Subtask is not a different instance, status=%s' % status)


class ParallelTask_Test(unittest.TestCase):
    """
    Tests to verify functionality of ParallelTask class
    """

    def setUp(self):
        self.parallel_task = TestParallelTask()
        self.worker = WorkerProxy()
        self.parallel_task.parent = self.worker

    def test_key_generation_paralleltask(self):
        """
        Verifies that the task key used to look up the task is generated correctly
        """
        expected = 'TestParallelTask'
        key = self.parallel_task.get_key()
        self.assertEqual(key, expected, 'Generated key [%s] does not match the expected key [%s]' % (key, expected) )

    def test_key_generation_paralleltask_child(self):
        """
        Verifies that the task key used to look up the task is generated correctly
        """
        expected = 'TestParallelTask.StandaloneTask'
        key = self.parallel_task.subtask.get_key()
        self.assertEqual(key, expected, 'Generated key [%s] does not match the expected key [%s]' % (key, expected) )

    def test_get_subtask_paralleltask(self):
        """
        Verifies:
             * that the task key returns the correct task if given the correct key
             * that the task key returns an error if given an incorrect key
        """
        # correct key
        key = 'TestParallelTask'
        expected = self.parallel_task
        returned = self.parallel_task.get_subtask(key.split('.'))
        self.assertEqual(returned, expected, 'Subtask retrieved was not the expected Task')
        
        # incorrect Key
        key = 'FakeTaskThatDoesNotExist'
        self.assertRaises(TaskNotFoundException, self.parallel_task.get_subtask, key.split('.'))

    def test_get_subtask_paralleltask_child(self):
        """
        Verifies:
             * that the task key returns the correct task if given the correct key
             * that the task key returns an error if given an incorrect key
        """
        # correct key
        key = 'TestParallelTask.StandaloneTask'
        expected = self.parallel_task.subtask
        returned = self.parallel_task.get_subtask(key.split('.'))
        self.assertEqual(returned, expected, 'Subtask retrieved was not the expected Task')
        
        # incorrect Key
        key = 'TestParallelTask.FakeTaskThatDoesNotExist'
        self.assertRaises(TaskNotFoundException, self.parallel_task.get_subtask, key.split('.'))

    def test_get_worker_paralleltask(self):
        """
        Verifies that the worker can be retrieved
        """
        returned = self.parallel_task.get_worker()
        self.assert_(returned, 'no worker was returned')
        self.assertEqual(returned, self.worker, 'worker retrieved was not the expected worker')

    def test_get_worker_paralleltask_child(self):
        """
        Verifies that the worker can be retrieved
        """
        returned = self.parallel_task.subtask.get_worker()
        self.assert_(returned, 'no worker was returned')
        self.assertEqual(returned, self.worker, 'worker retrieved was not the expected worker')
