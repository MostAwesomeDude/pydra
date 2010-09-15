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
from __future__ import with_statement

from collections import defaultdict
from threading import RLock
import os
import shutil
import time


from django.core.paginator import Paginator, InvalidPage, EmptyPage
from django.template import Context, loader

from twisted.internet.defer import Deferred
from twisted.internet.task import LoopingCall

from pydra.config import load_settings
load_settings()
import pydra_settings
from pydra.cluster.module import Module
from pydra.cluster.tasks import packaging, TaskContainer, TaskNotFoundException
from pydra.logs.logger import task_log_path
from pydra.models import TaskInstance
from pydra.util import graph, makedirs

import logging
logger = logging.getLogger('root')


class TaskManager(Module):
    """
    The task manager tracks and controls tasks and is a central point for the
    cluster to enumerate, discover, and acquire tasks to run.
    """

    _signals = [
        'TASK_ADDED',
        'TASK_UPDATED',
        'TASK_REMOVED',
        'TASK_INVALID',
        'TASK_OUTDATED',
    ]

    _shared = [
        'get_task',
    ]

    autodiscover_call = None
    """
    `twisted.internet.task.LoopingCall` used to scan for new tasks.
    """

    def __init__(self, scan_interval=20, lazy_init=False):
        """
        Constructor.

        If `lazy_init` is True, `scan_interval` is forced to 0, disabling
        autodiscovery of tasks.

        :Parameters:
            scan_interval : int
                If non-zero, the manager will automatically scan TASKS_DIR at
                this interval for new tasks.
            lazy_init : bool
                Whether tasks should be lazily loaded. When False, tasks will
                be loaded on discovery; when True, tasks will be loaded on
                demand.
        """

        self._interfaces = [
            self.list_tasks,
            self.task_history,
            self.task_history_detail,
            self.task_log,
        ]

        self._listeners = {
            'TASK_RELOAD':self.init_package,
            'TASK_STARTED':self._task_started,
            'TASK_STARTED':self._task_stopped,
        }

        self.lazy_init = lazy_init

        # Interval, in seconds, between scans of the task folders for new
        # tasks. None disables scanning.
        self.scan_interval = scan_interval

        if self.lazy_init:
            self.scan_interval = 0
        else:
            self._listeners['MANAGER_INIT'] = self.init_task_cache

        self.tasks_dir = pydra_settings.TASKS_DIR
        self.tasks_dir_internal = pydra_settings.TASKS_DIR_INTERNAL

        makedirs(self.tasks_dir_internal)

        # full_task_key or pkg_name: pkg_object
        # preserved for both compatibility and efficiency
        self.registry = {}
        self.package_dependency = graph.DirectedGraph()

        self._task_callbacks = defaultdict(list)
        """
        Dictionary mapping task keys to a list of `Deferred`s waiting to be
        fired.
        """

        self._lock = RLock()

        self.__initialized = False

        if self.scan_interval:
            self.autodiscover_call = LoopingCall(self.autodiscover)


    def processTask(self, task, parent=False):
        """
        Given a task, return a list of lists of information about the task.

        :Parameters:
            task : `Task`
                Task to process.

        :returns: A list of lists.
        """

        tasklist = []

        #turn the task into a tuple
        processedTask = [task.__class__.__name__, parent, task.msg]

        #add that task to the list
        tasklist.append(processedTask)

        #add all children if the task is a container
        if isinstance(task,TaskContainer):
            for subtask in task.subtasks:
                tasklist += self.processTask(subtask.task, task.id)

        return tasklist



    def processTaskProgress(self, task):
        """
        Given a task, return a list of dicts of information about the task's
        progress and status.

        :Parameters:
            task : `Task`
                Task to process.

        :returns: A list of dicts.
        """

        tasklist = []

        #turn the task into a tuple
        processedTask = {
            'id':task.id,
            'status':task.status(),
            'progress':task.progress(),
            'msg':task.progressMessage()
        }

        #add that task to the list
        tasklist.append(processedTask)

        #add all children if the task is a container
        if isinstance(task,TaskContainer):
            for subtask in task.subtasks:
                tasklist += self.processTaskProgress(subtask.task)

        return tasklist


    def list_tasks(self, toplevel=True, keys=None):
        """
        Return a list of tasks.

        XXX actually returns a dict.
        XXX keys is hilariously inefficient.
        XXX toplevel is unused.

        :Parameters:
            keys : list
                Whitelist of tasks to list.
        """

        message = {}
        # show all tasks by default
        if keys == None:
            keys = self.list_task_keys()

        for key in keys:
            try:
                last_run_instance = TaskInstance.objects.filter(task_key=key).exclude(completed=None).order_by('-completed').values_list('completed','task_key')[0]
                last_run = time.mktime(last_run_instance[0].timetuple())
            #no instances
            except (KeyError, IndexError):
                last_run = None

            # render the form if the task has one
            task = self.registry[key, None].tasks[key]
            if task.form:
                t = loader.get_template('task_parameter_form.html')
                c = Context ({'form':task.form()})
                rendered_form = t.render(c)
            else:
                rendered_form = None

            message[key] = {'description':task.description ,
                            'last_run':last_run,
                            'form':rendered_form}

        return message


    def progress(self, keys=None):
        """
        Return a dict of task progresses.

        XXX "progresses?" Really?
        XXX keys is, yet again, inefficient

        :Parameters:
            keys : list
                Whitelist of tasks to list.
        """

        message = {}

        # show all tasks by default
        if keys == None:
            keys = self.list_task_keys()

        # store progress of each task in a dictionary
        for key in keys:
            progress = self.processTaskProgress(self.registry[key,
                        None].tasks[key])
            message[key] = {
                'status':progress
            }

        return message


    def init_task_cache(self):
        """
        Initializes the task cache.

        This method scans tasks_dir_internal for already-versioned tasks, and
        also starts the autodiscover mechanism, if enabled.
        """

        # read task_cache_internal (this is one-time job)
        files = os.listdir(self.tasks_dir_internal)
        for pkg_name in files:
            self.init_package(pkg_name)

        # trigger the autodiscover procedure immediately
        if self.autodiscover_call:
            self.autodiscover_call.start(self.scan_interval, True)


    def init_package(self, pkg_name, version=None):
        """
        Load a single package into the registry.

        :Parameters:
            pkg_name : str
                The name of the package to be loaded.
            version
                The version of the package to be loaded, or None for the first
                version found.

        :returns: A `TaskPackage`, or None if the package could not be loaded.
        """

        with self._lock:
            pkg_dir = os.path.join(self.tasks_dir_internal, pkg_name)
            if os.path.isdir(pkg_dir):
                versions = os.listdir(pkg_dir)
                if not versions:
                    raise TaskNotFoundException(pkg_name)

                elif version:
                    # load specified version if available
                    if not version in versions:
                        raise TaskNotFoundException(pkg_name)
                    v = version
                elif len(versions) != 1:
                    # load the newest version
                    logger.warn('Internal task cache contains more than one version of the task')
                    v = versions[0]
                    for dir in versions:
                        if os.path.getmtime('%s/%s' % (pkg_dir,dir)) > \
                            os.path.getmtime('%s/%s' % (pkg_dir,v)):
                                v = dir
                else:
                    v = versions[0]

                # load this version
                full_pkg_dir = os.path.join(pkg_dir, v)
                pkg = packaging.TaskPackage(pkg_name, full_pkg_dir, v)
                if pkg.version != v:
                    # verification
                    logger.warn('Invalid package %s:%s' % (pkg_name, v))
                self._add_package(pkg)

                # invoke attached task callbacks
                callbacks = self._task_callbacks[pkg_name]
                module_path, cycle = self._compute_module_search_path(pkg_name)
                while len(callbacks):
                    task_key, d = callbacks.pop(0)
                    if cycle:
                        d.errback((task_key, pkg.version,
                            'Cycle detected in dependency'))
                    else:
                        d.callback((task_key, pkg.version, pkg.tasks[task_key],
                                module_path))
                return pkg
        return None

    def autodiscover(self):
        """
        Scan for new and updated tasks.

        This method should not be called from external code; it is called
        periodically every `scan_interval` seconds.

        This method may chew up CPU time since it calls `read_task_package()`
        which computes directory hashes.
        """

        old_packages = self.list_task_packages()

        files = os.listdir(self.tasks_dir)
        for filename in files:
            pkg_dir = os.path.join(self.tasks_dir, filename)
            if os.path.isdir(pkg_dir):
                self.read_task_package(filename)
                old_packages.discard(filename)

        for pkg_name in old_packages:
            self.emit('TASK_REMOVED', pkg_name)


    def task_history(self, key, page):
        """
        Return a paginated list of times a task has run.
        """

        instances = TaskInstance.objects.filter(task_key=key) \
            .order_by('-completed').order_by('-started')
        paginator = Paginator(instances, 10)

         # If page request (9999) is out of range, deliver last page.
        try:
            paginated = paginator.page(page)

        except (EmptyPage, InvalidPage):
            page = paginator.num_pages
            paginated = paginator.page(page)

        instances = [i.json_safe() for i in paginated.object_list]

        return {
                'prev':paginated.has_previous(),
                'next':paginated.has_next(),
                'page':page,
                'instances':instances
               }


    def task_history_detail(self, task_id):
        """
        Return detailed history about a specific `TaskInstance`.
        """

        try:
            task_instance = TaskInstance.objects.get(id=task_id)
        except TaskInstance.DoesNotExist:
            return None

        workunits = [workunit.json_safe() for workunit in
                     task_instance.workunits.all().order_by('id')]
        task_key = task_instance.task_key
        task = self.registry[task_key, None].tasks[task_key]
        return {
                    'details':task_instance.json_safe(),
                    'name':task.__name__,
                    'description':task.description,
                    'workunits':workunits
               }

    def task_log(self, task_id, subtask=None, workunit_id=None):
        """
        Return the log file for the given task.

        :Parameters:
            task_id
                ID of the task.
            subtask
                Path to subtask, or None for no subtask.
            workunit_id
                Workunit key, or None for no workunit.
        """

        if subtask:
            dir, logfile = task_log_path(task_id, subtask, workunit_id)
        else:
            dir, logfile = task_log_path(task_id)

        fp = open(logfile, 'r')
        log = fp.read()
        fp.close()
        return log

    def retrieve_task(self, task_key, version):
        """
        Obtains a task through a variety of methods.

        XXX So close and yet so far to properly using Deferreds. :c

        :Parameters:
            task_key
                The task key.
            version
                The task version, or None for the latest version added to the
                manager's cache.

        :returns: A `Deferred` that will be fired with a tuple containing the
        task key, the package version, the task class, and the module path, on
        success, or the task key, package version, and error string, on failure.
        """

        d = Deferred()

        pkg_name = task_key[:task_key.find('.')]
        needs_update = False
        with self._lock:

            # get the task. if configured for lazy init, this class will only
            # attempt to load a task into the registry once it is requested.
            # subsequent requests will pull from the registry.
            pkg = self.registry.get( (pkg_name, version), None)
            if not pkg and self.lazy_init:
                logger.debug('Lazy Init: %s' % pkg_name)
                pkg = self.init_package(pkg_name, version)

            if pkg:
                pkg_status = pkg.status
                if pkg_status == packaging.STATUS_OUTDATED:
                    # package has already entered a sync process;
                    # append the callback
                    self._task_callbacks[pkg_name].append((task_key, d))
                task_class = pkg.tasks.get(task_key, None)
                if task_class and (version is None or pkg.version == version):
                    module_path, cycle = self._compute_module_search_path(
                            pkg_name)
                    if cycle:
                        d.errback(
                            (task_key, pkg.version, 'Cycle detected in dependency'))
                    else:
                        d.callback((task_key, version, task_class,
                            module_path))
                else:
                    # needs update
                    pkg.status = packaging.STATUS_OUTDATED
                    needs_update = True
            else:
                # no local package contains the task with the specified
                # version, but this does NOT mean it is an error -
                # try synchronizing tasks first
                needs_update = True

        if needs_update:
            self.emit('TASK_OUTDATED', pkg_name, version)
            self._task_callbacks[pkg_name].append((task_key, d))

        return d

    def list_task_keys(self):
        return [k[0] for k in self.registry.keys() if k[0].find('.') != -1]


    def list_task_packages(self):
        return set(k[0] for k in self.registry.keys() if k[0].find('.') == -1)


    def read_task_package(self, pkg_name):
        """
        Read a `TaskPackage` directory from the task directory and import it
        into the internal task directory.

        This method may emit TASK_ADDED or TASK_UPDATED depending on the task
        it processes.

        Any pending callbacks on the task will be executed after this method
        runs.

        This method chews CPU because it runs hashes.

        :Parameters:
            pkg_name
                Name of the package.

        :returns: A `TaskPackage`, or None if the task is not loaded.
        """

        # this method is slow in finding updates of tasks
        with self._lock:
            pkg_dir = self.get_package_location(pkg_name)
            signal = None
            sha1_hash = packaging.compute_sha1_hash(pkg_dir)
            internal_folder = os.path.join(self.tasks_dir_internal,
                    pkg_name, sha1_hash)

            pkg = self.registry.get((pkg_name, None), None)
            if not pkg or pkg.version != sha1_hash:
                # copy this folder to tasks_dir_internal
                try:
                    shutil.copytree(pkg_dir, internal_folder)
                except OSError:
                    # already in tree, just update the timestamp so it shows
                    # as the newest version
                    os.utime('%s' % (internal_folder), None)
                    logger.warn('Package %s v%s already exists' % (pkg_name,
                                sha1_hash))
            # find updates
            if (pkg_name, None) not in self.registry:
                signal = 'TASK_ADDED'
            elif sha1_hash != self.registry[pkg.name, None].version:
                signal = 'TASK_UPDATED'

        if signal:
            pkg = self.init_package(pkg_name, sha1_hash)
            self.emit(signal, pkg_name)
            return pkg
        return None


    def get_task_package(self, task_key):
        return self.registry.get( (task_key, None), None)


    def get_package_location(self, pkg_name):
        return os.path.join(self.tasks_dir, pkg_name)


    def _add_package(self, pkg):
        """
        Adds a package to the registry.

        This method does **not** load dependencies of the reqested package.

        `RuntimeError` will be raised if the package could not be added due to
        dependency issues.

        XXX RuntimeError is overkill. :T

        :Parameters:
            pkg
                Package to add.
        """
        for dep in pkg.dependency:
            for key in self.registry.keys():
                if key[0] == dep:
                    break
            else:
                raise RuntimeError(
                        'Package %s has unresolved dependency issues: %s' %
                        (pkg.name, dep))
            self.package_dependency.add_edge(pkg.name, dep)
        self.package_dependency.add_vertex(pkg.name)
        for key, task in pkg.tasks.iteritems():
            self.registry[key, pkg.version] = pkg
            self.registry[key, None] = pkg
        self.registry[pkg.name, pkg.version] = pkg

        # mark this package as the latest one
        self.registry[pkg.name, None] = pkg


    def _compute_module_search_path(self, pkg_name):
        """
        Create a list of import paths that a package depends on.

        If the package's dependencies are not located in the interpreter's
        path, then this method will not find them.

        :Parameters:
            pkg_name
                Name of the package, and also the directory in which it is
                located.
        """
        pkg_location = self.get_package_location(pkg_name)
        module_search_path = [pkg_location, os.path.join(pkg_location,'lib')]
        st, cycle = graph.dfs(self.package_dependency, pkg_name)
        # computed packages on which this task depends
        required_pkgs = [self.get_package_location(x) for x in \
                st.keys() if  st[x] is not None]
        module_search_path += required_pkgs
        module_search_path += [os.path.join(x, 'lib') for x in required_pkgs]
        return module_search_path, cycle


    def _task_started(self, task_key, version):
        """
        Listener for task start.

        Used for tracking tasks that are currently running.
        """

        pass


    def _task_stopped(self, task_key, version):
        """
        Listener for task completion.

        Used for deleting obsolete versions of tasks that could not be deleted
        earlier due to them being in use.
        """

        pass

