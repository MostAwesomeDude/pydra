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

from pydra.cluster.tasks import TaskNotFoundException, STATUS_STOPPED, \
        STATUS_RUNNING, STATUS_COMPLETE

from pydra.cluster.tasks.tasks import Task
from pydra.cluster.tasks.parallel_task import ParallelTask

from pydra.tests import setup_test_environment
setup_test_environment()

from pydra.tests.cluster.tasks.proxies import WorkerProxy
from pydra.tests.cluster.tasks.impl.task import StartupAndWaitTask


class Task_TwistedTest(twisted_unittest.TestCase):
    """
    Task Tests that require the twisted framework to test
    """

    def verify_status(self, task, parent,  subtask_key=None):
        try:
            parent.start(subtask_key=subtask_key)
            
            # wait for event indicating task has started
            task.starting_event.wait(1)
            self.assertEqual(task.status(), STATUS_RUNNING, 'Task started but status is not STATUS_RUNNING')
            
            task._stop()
            
            # don't release running lock till this point.  otherwise
            # the task will just loop indefinitely and may starve
            # other threads that need to execute
            task.running_event.set()
            
            #wait for the task to finish
            task.finished_event.wait(1)
            self.assertEqual(task._status, STATUS_COMPLETE, 'Task stopped by status is not STATUS_COMPLETE')
        
        except Exception, e:
            raise
        
        finally:
            #release events just in case
            
            task._stop()
            task.clear_events()

    def test_start_task(self):
        """
        Tests Task.start()
            verify:
                * that the work method is deferred to a thread successfully
                * that the status changes to STATUS_RUNNING when its running
                * that the status changes to STATUS_COMPLETED when its finished
        """
        task = StartupAndWaitTask()
        task.parent = WorkerProxy()
        
        # defer rest of test because this will cause the reactor to start
        return threads.deferToThread(self.verify_status, task=task, parent=task)

    def test_start_subtask(self):
        """
        Tests Task.start()
            verify:
                * that the work method is deferred to a thread successfully
                * that the status changes to STATUS_RUNNING when its running
                * that the status changes to STATUS_COMPLETED when its finished
        """
        task = ParallelTask()
        task.subtask = StartupAndWaitTask()
        task.parent = WorkerProxy()
        
        # defer rest of test because this will cause the reactor to start
        return threads.deferToThread(self.verify_status, task=task.subtask, parent=task, subtask_key='ParallelTask.StartupAndWaitTask')


class Task_Internal_Test(unittest.TestCase):
    """
    Tests for verify functionality of Task class
    """
    def setUp(self):
        self.task = StandaloneTask()
        self.worker = WorkerProxy()
        self.task.parent = self.worker

    def tearDown(self):
        pass

    def test_key_generation_task(self):
        """
        Verifies that the task key used to look up the task is generated correctly
        """
        expected = 'StandaloneTask'
        key = self.task.get_key()
        self.assertEqual(key, expected, 'Generated key [%s] does not match the expected key [%s]' % (key, expected) )

    def test_get_subtask_task(self):
        """
        Verifies:
             * that the task key returns the correct task if given the correct key
             * that the task key returns an error if given an incorrect key
        """
        # correct key
        key = 'StandaloneTask'
        expected = self.task
        returned = self.task.get_subtask(key.split('.'))
        self.assertEqual(returned, expected, 'Subtask retrieved was not the expected Task')
        
        # incorrect Key
        key = 'FakeTaskThatDoesNotExist'
        self.assertRaises(TaskNotFoundException, self.task.get_subtask, key.split('.'))

    def test_get_worker_task(self):
        """
        Verifies that the worker can be retrieved
        """
        returned = self.task.get_worker()
        self.assert_(returned, 'no worker was returned')
        self.assertEqual(returned, self.worker, 'worker retrieved was not the expected worker')


class StandaloneTask(Task):
    def work(self, **kwargs):
        """
        Simple work method that always returns a testable sentinel.
        """
        return range(5)


class TaskStandaloneTest(unittest.TestCase):
    """
    Test `Task` functionality without running the Twisted reactor.
    """

    def setUp(self):
        self.task = StandaloneTask()

    def test_trivial(self):
        pass

    def test_initial_status(self):
        self.assertEqual(self.task.status(), STATUS_STOPPED)

    def test_synchronous_start(self):
        self.assertEqual(self.task.start(), range(5))
