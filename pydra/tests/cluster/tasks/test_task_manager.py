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

import inspect
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
from pydra.tests import django_testcase

test_string = """
from pydra.cluster.tasks import Task
class TestTask(Task):
    def work(self, **kwargs): pass
"""

class TaskManagerTestCaseMixIn(ModuleTestCaseMixIn):
    def setUp(self):
        ModuleTestCaseMixIn.setUp(self)
        
        # Munge both task directories to be under our control and also
        # different for each unit test.
        pydra_settings.TASKS_DIR = tempfile.mkdtemp()
        pydra_settings.TASKS_DIR_INTERNAL = tempfile.mkdtemp()
        
        # Lazy-inited, with no autodiscovery.
        self.task_manager = TaskManager(None, lazy_init=True)
        self.manager.register(self.task_manager)
        
        # Make a test package with a single file
        self.packages = []
        self.tasks = []
        self.create_package()
        self.create_file()
        
        # Save test package information for later.
        self.package_name = 'test'
        self.package_dir_internal = os.path.join( \
            self.task_manager.tasks_dir_internal, self.package_name)
        self.task = 'test.testmodule.TestTask'

    def create_package(self, package='test'):
        """
        Creates a package we can place test files into.  by default this will
        stick a single file in the package with a basic task.
        """
        dir = os.path.join(self.task_manager.tasks_dir, package)
        makedirs(dir)
        self.packages.append(package)

    def create_file(self, package='test', module='testmodule', str=test_string):
        """
        Adds a python file to the package dir.
        """
        dir = os.path.join(self.task_manager.tasks_dir, package)
        with open(os.path.join(dir, "%s.py" % module), "w") as f:
                f.write(str)

    def create_file_from_object(self, object, package='test'):
        """
        creates a file from existing module or class. This will locate the
        source file and copy it into the package.  This does not maintain dir
        heirarchy, the file will be copied to the root of the package.
        """
        src = inspect.getsourcefile(object)
        dest = os.path.join(self.task_manager.tasks_dir, package)
        shutil.copy(src, dest)

    def create_cache_entry(self, package='test', hash='FAKE_HASH'):
        """
        Creates fake entries in the internal tasks directory. This or
        autodiscover must be called after adding files to the package.
        """
        dir = os.path.join(self.task_manager.tasks_dir, package)
        internal_folder = os.path.join(self.task_manager.tasks_dir_internal,
                    package, hash)
        shutil.copytree(dir, internal_folder)

    def clear_cache(self):
        """
        Clears the entire cache of all packages.
        """
        for package in self.packages:
            dir = os.path.join(self.task_manager.tasks_dir_internal, package)
            shutil.rmtree(dir, True)

    def clear_package_cache(self, package='test'):
        """
        Clears the test package's cache.
        """
        os.path.join(self.task_manager.tasks_dir_internal, package)
        if os.path.exists(dir):
            shutil.rmtree(dir)

    def tearDown(self):
        self.clear_cache()
        
        for package in self.packages:
            shutil.rmtree(os.path.join(self.task_manager.tasks_dir, package))
        
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


class TaskManagerTest(django_testcase.TestCase, TaskManagerTestCaseMixIn):

    def setUp(self):
        TaskManagerTestCaseMixIn.setUp(self)
        
        self.completion = {}
        for task in self.tasks:
            self.completion[task] = None
        
        self.task_instances = []
        for task in self.tasks:
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
        TaskInstance.objects.all().delete()
        TaskManagerTestCaseMixIn.tearDown(self)

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
        package = self.task_manager.registry[(self.package_name, 'FAKE_HASH')]
        self.assertNotEqual(package, None, 'Registry does not contain package')

    def test_init_package_empty_package(self):
        os.mkdir(self.package_dir_internal)
        self.assertRaises(TaskNotFoundException,
            self.task_manager.init_package, self.package_name)
        self.assertEqual(len(self.task_manager.registry), 0, 'Cache is empty, but registry is not')

    def test_init_package_multiple_versions(self):
        module = "testmodule"
        self.test_retrieve_task()
        self.create_cache_entry(hash='SECOND_ENTRY')
        self.task_manager.autodiscover()
        self.test_retrieve_task()

    def test_autodiscover(self):
        self.assertEqual(len(self.task_manager.list_task_packages()), 0)
        self.assertEqual(len(self.task_manager.list_tasks()), 0)
        self.task_manager.autodiscover()
        self.assertEqual(len(self.task_manager.list_task_packages()), 1)
        self.assertEqual(len(self.task_manager.list_tasks()), 1)

    def test_add_package(self):
        self.create_cache_entry()
        package = packaging.TaskPackage(self.package_name,
            self.package_dir_internal, 'FAKE_HASH')
        self.task_manager._add_package(package)
        package = self.task_manager.registry[(self.package_name, 'FAKE_HASH')]
        self.assertNotEqual(package, None, 'Registry does not contain package')

    def test_retrieve_task(self):
        self.task_manager.autodiscover()
        helper = RetrieveHelper()
        task_key = self.task
        deferred = self.task_manager.retrieve_task(task_key, None)
        deferred.addCallbacks(helper.callback, helper.errback)
        
        self.assertEquals(task_key, helper.task_key,
            'Task_key does not match')
        self.assert_(helper.task_class, 'task class was not retrieved')

    def test_retrieve_task_lazy_init(self):
        self.create_cache_entry()
        helper = RetrieveHelper()
        task_key = self.task
        deferred = self.task_manager.retrieve_task(task_key, None)
        deferred.addCallbacks(helper.callback, helper.errback)

        self.assertEqual(task_key, helper.task_key)
        self.assert_(helper.task_class, 'task class was not retrieved')

    def test_lazy_init(self):
        self.assertTrue(self.task_manager.lazy_init)


class RetrieveHelper():
    task_key = None
    version = None
    task_class= None
    module_path = None
    args = None
    kwargs = None

    def callback(self, task_tuple, *args, **kw):
        self.task_key, self.version, self.task_class, self.module_path = task_tuple
        self.args = args
        self.kwargs = kw

    def errback(self, *args, **kwargs):
        print "Retrieval failed!"


if __name__ == "__main__":
    unittest.main()
