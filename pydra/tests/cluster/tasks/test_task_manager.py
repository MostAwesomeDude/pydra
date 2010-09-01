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

import os.path
import shutil
import tempfile
import time
import unittest
from datetime import datetime

#environment must be configured before loading tests
from pydra.config import configure_django_settings, load_settings
configure_django_settings()
load_settings()
import pydra_settings

from pydra.cluster.tasks import TaskNotFoundException, packaging
from pydra.cluster.tasks.task_manager import TaskManager
from pydra.models import TaskInstance
from pydra.util import makedirs

from pydra.tests.mixin_testcases import ModuleTestCaseMixIn

test_string = """
from pydra.cluster.tasks import Task
class TestTask(Task):
    pass
"""

class TaskManagerTest(unittest.TestCase, ModuleTestCaseMixIn):

    def setUp(self):
        ModuleTestCaseMixIn.setUp(self)

        # Munge both task directories to be under our control and also
        # different for each unit test.
        pydra_settings.TASKS_DIR = tempfile.mkdtemp()
        pydra_settings.TASKS_DIR_INTERNAL = tempfile.mkdtemp()

        # Lazy-inited, with no autodiscovery.
        self.task_manager = TaskManager(None, lazy_init=True)
        self.task_manager._register(self.manager)

        # Make a test package.
        self.package = 'test'
        module = "testmodule"
        self.package_dir = os.path.join(
            self.task_manager.tasks_dir, self.package)
        self.package_dir_internal = os.path.join(
            self.task_manager.tasks_dir_internal, self.package)
        makedirs(self.package_dir)
        with open(os.path.join(self.package_dir, "%s.py" % module), "w") as f:
            f.write(test_string)

        # Save test package information for later.
        self.tasks = [
            '%s.%s.TestTask' % (self.package, module),
        ]
        self.completion = {}
        for task in self.tasks:
            self.completion[task] = None


        self.task_instances = []
        for task in self.tasks [:2]:
            #queued tasks
            task_instance = TaskInstance()
            task_instance.task_key=task
            task_instance.save()
            self.task_instances.append(task_instance)

            #running tasks
            task_instance = TaskInstance()
            task_instance.task_key=task
            task_instance.started = datetime.now()
            task_instance.save()
            self.task_instances.append(task_instance)

            #finished tasks
            task_instance = TaskInstance()
            task_instance.task_key=task
            task_instance.started = datetime.now()
            completed_time = datetime.now()
            task_instance.completed = completed_time
            task_instance.save()
            self.completion[task] = completed_time
            self.task_instances.append(task_instance)

            #failed tasks
            task_instance = TaskInstance()
            task_instance.task_key=task
            task_instance.started = datetime.now()
            task_instance.status = -1
            task_instance.save()
            self.task_instances.append(task_instance)


    def tearDown(self):
        for task in self.task_instances:
            task.delete()
        self.clear_cache()
        for directory in (pydra_settings.TASKS_DIR,
            pydra_settings.TASKS_DIR_INTERNAL):
            try:
                os.rmdir(directory)
            except OSError:
                print "Warning: Directory %s not empty" % directory
                try:
                    os.removedirs(directory)
                except OSError:
                    print "Warning: Directory %s still dirty" % directory
                    shutil.rmtree(directory)


    def create_cache_entry(self, hash='FAKE_HASH'):
        """
        Creates an entry in the task_cache_internal
        """
        internal_folder = os.path.join(self.task_manager.tasks_dir_internal,
                    self.package, hash)

        makedirs(internal_folder)


    def clear_cache(self):
        """
        Clears the entire cache of all packages
        """
        self.clear_package_cache()
        if os.path.exists(self.package_dir_internal):
            shutil.rmtree(self.package_dir_internal)


    def clear_package_cache(self):
        """
        Cleans just the cached versions of the selected task
        """
        if os.path.exists(self.package_dir_internal):
            for version in os.listdir(self.package_dir_internal):
                shutil.rmtree(os.path.join(self.package_dir_internal, version))


    def test_trivial(self):
        """
        Test the basic init and teardown of the test harness and TaskManager.
        """
        pass


    def test_listtasks(self):
        """
        Tests `TaskManager.list_tasks()` for completeness and times.
        """
        self.task_manager.autodiscover()
        tasks = self.task_manager.list_tasks()
        self.assertEqual(len(tasks), 1)

        for task in self.tasks:
            recorded_time = self.completion[task]
            recorded_time = time.mktime(recorded_time.timetuple()) if recorded_time else None
            list_time = tasks[task]['last_run']
            self.assertEqual(recorded_time, list_time, "Completion times for task don't match: %s != %s" % (recorded_time, list_time))


    def test_init_cache_empty_cache(self):
        self.task_manager.init_task_cache()
        self.assertEqual(len(self.task_manager.registry), 0, 'Cache is empty, but registry is not')


    def test_init_cache(self):
        self.create_cache_entry()
        self.task_manager.init_task_cache()
        package = self.task_manager.registry[(self.package, 'FAKE_HASH')]
        self.assertNotEqual(package, None, 'Registry does not contain package')


    def test_init_package(self):
        self.create_cache_entry()
        self.task_manager.init_task_cache()
        length = len(self.task_manager.registry)
        package = self.task_manager.registry[(self.package, 'FAKE_HASH')]
        self.assertNotEqual(package, None, 'Registry does not contain package')

    def test_init_package_empty_package(self):
        os.mkdir(self.package_dir_internal)
        self.assertRaises(TaskNotFoundException, self.task_manager.init_package, self.package)
        self.assertEqual(len(self.task_manager.registry), 0, 'Cache is empty, but registry is not')

    def test_init_package_multiple_versions(self):
        self.create_cache_entry('FAKE_HASH_1')
        self.create_cache_entry('FAKE_HASH_2')
        self.create_cache_entry('FAKE_HASH_3')
        self.task_manager.init_package(self.package)
        length = len(self.task_manager.registry)
        package = self.task_manager.registry[(self.package, 'FAKE_HASH_3')]
        self.assertNotEqual(package, None, 'Registry does not contain latest package')
        try:
            package = None
            package = self.task_manager.registry[(self.package, 'FAKE_HASH_2')]
        except KeyError:
            pass
        self.assertEqual(package, None, 'Registry contains old package')

    def test_autodiscover(self):
        self.assertEqual(len(self.task_manager.list_task_packages()), 0)
        self.assertEqual(len(self.task_manager.list_tasks()), 0)
        self.task_manager.autodiscover()
        self.assertEqual(len(self.task_manager.list_task_packages()), 1)
        self.assertEqual(len(self.task_manager.list_tasks()), 1)

    def test_add_package(self):
        self.create_cache_entry()
        package = packaging.TaskPackage(self.package,
            self.package_dir_internal, 'FAKE_HASH')
        self.task_manager._add_package(package)
        package = self.task_manager.registry[(self.package, 'FAKE_HASH')]
        self.assertNotEqual(package, None, 'Registry does not contain package')

    def test_add_package_with_dependency(self):
        self.create_cache_entry()
        package = packaging.TaskPackage(self.package,
            self.package_dir_internal, 'FAKE_HASH')
        self.task_manager._add_package(package)
        package = self.task_manager.registry[(self.package, 'FAKE_HASH')]
        self.assertNotEqual(package, None, 'Registry does not contain package')
        self.fail('Not Implemented')


    def test_add_package_with_missing_dependency(self):
        self.fail('Not Implemented')

    def test_retrieve_task(self):
        self.create_cache_entry()
        self.task_manager.init_task_cache()
        helper = RetrieveHelper()
        task_key = 'demo.demo_task.TestTask'
        self.task_manager.retrieve_task(task_key,'FAKE_HASH', helper.callback, \
                                   helper.errback)
        self.assertEquals(task_key, helper.task_key , 'Task_key does not match')
        self.assertEquals('FAKE_HASH', helper.version , 'Task_key does not match')

    def test_lazy_init(self):
        self.create_cache_entry()
        helper = RetrieveHelper()
        task_key = 'demo.demo_task.TestTask'
        self.task_manager.retrieve_task(task_key,'FAKE_HASH', helper.callback, \
                                   helper.errback)
        self.assertEquals(task_key, helper.task_key , 'Task_key does not match')
        self.assertEquals('FAKE_HASH', helper.version , 'Task_key does not match')

    def test_lazy_init_with_dependency(self):
        self.create_cache_entry()
        helper = RetrieveHelper()
        task_key = 'demo.demo_task.TestTask'
        self.task_manager.retrieve_task(task_key,'FAKE_HASH', helper.callback, \
                                   helper.errback)
        self.assertEquals(task_key, helper.task_key , 'Task_key does not match')
        self.assertEquals('FAKE_HASH', helper.version , 'Task_key does not match')
        self.fail('Not Implemented')

    def test_lazy_init_with_missing_dependency(self):
        self.create_cache_entry()
        helper = RetrieveHelper()
        task_key = 'demo.demo_task.TestTask'
        self.task_manager.retrieve_task(task_key,'FAKE_HASH', helper.callback, \
                                   helper.errback)
        self.assertEquals(task_key, helper.task_key , 'Task_key does not match')
        self.assertEquals('FAKE_HASH', helper.version , 'Task_key does not match')
        self.fail('Not Implemented')


class RetrieveHelper():
    task_key = None
    version = None
    task_class= None
    module_path = None
    args = None
    kwargs = None

    def callback(self, task_key, version, task_class, module_path, *args, **kw):
        self.task_key = task_key
        self.version = version
        self.task_class = task_class
        self.module_path = module_path
        self.args = args
        self.kwargs = kw

    def errback(self):
        pass

if __name__ == "__main__":
    unittest.main()
