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

import logging
logger = logging.getLogger('root')

from twisted.internet import reactor, threads

from pydra.cluster.tasks import TaskNotFoundException, STATUS_CANCELLED, \
    STATUS_FAILED, STATUS_STOPPED, STATUS_RUNNING, STATUS_PAUSED, \
    STATUS_COMPLETE
from pydra.logs.logger import get_task_logger

class Task(object):
    """
    Tasks encapsulate a single function into a unit of work. `Task` allows
    functions to be managed and tracked in a uniform manner.

    Basic usage of `Task` is fairly simple. Subclass `Task` and override
    `work()` in the subclass.

    `Task` is abstract and requires the following methods to be implemented:

     * `work()`
    """

    parent = None
    _status = STATUS_STOPPED
    __callback = None
    _callbackargs = None
    workunit = None
    STOP_FLAG = False
    form = None

    msg = None
    description = 'Default description about Task baseclass.'


    def __eq__(self, val):
        return self.__repr__() == val.__repr__()


    def __init__(self, msg=None):
        # XXX unused
        self.msg = msg
        self.id = 1
        self.work_deferred = False
        self.logger = logger


    def _complete(self, results):
        """
        Callback called after `work()` is finished.

        :Parameters:
            results
                The result of `work()`
        """

        self._status = STATUS_COMPLETE

        if self.__callback:
            self.logger.debug('%s - Task._work() -Making callback' % self)
            self.__callback(results, **self._callback_args)
        else:
            self.logger.warning('%s - Task._work() - NO CALLBACK TO MAKE: %s'
                % (self, self.__callback))


    def _get_subtask(self, task_path, clean=False):
        """
        Trivial lookup of subtask. This basic implementation only checks
        whether the current instance matches the requested task path.

        Raises `TaskNotFoundException` if the requested subtask cannot be
        found.

        :Parameters:
            task_path : list
                List of strings corresponding to the unique hierarchy of a
                task
            clean : bool
                Whether the subtask should be reinstantiated; currently
                ignored

        :returns: A tuple containing: the consumed portion of the task path,
            and the subtask.
        """
        #A Task can't have children,  if this is the last entry in the path
        # then this is the right task
        if len(task_path) == 1 and task_path[0] == self.__class__.__name__:
            return task_path, self
        else:
            raise TaskNotFoundException("Task not found: %s" % task_path)


    def _stop(self):
        """
        Stop the task.

        Calling this method causes the task to stop at the nearest
        opportunity. It may or may not complete its work.

        At the moment, this method is not safe or reliable.

        Subclasses of `Task` with subtasks may wish to override this method in
        order to stop subtasks.
        """

        self.STOP_FLAG=True


    def _start(self, args={}, callback=None, callback_args={}):
        """
        Executes `_work()` and sets up the callback.

        :Parameters:
            args : dict
                Keyword arguments to be passed to `work()`
            callback : callable
                Callback for completed work
            callback_args : dict
                Keyword arguments to be passed to `callback`
        """
        self.__callback = callback
        self._callback_args=callback_args

        self._status = STATUS_RUNNING
        results = self._work(**args)

        return results


    def _work(self, **kwargs):
        """
        Call `work()`, then `_complete()`.

        :returns: The results of `work()`.
        """

        results = self.work(**kwargs)
        self._complete(results)
        return results


    def get_key(self):
        """
        A unique key that represents this task instance.

        This key can be used to find this task from its root. The primary
        purpose of these keys are to find subtasks.
        """

        key = self.__class__.__name__
        base = self.parent.get_key()
        if base:
            key = '%s.%s' % (base, key)
        return key


    def get_subtask(self, task_path, clean=False):
        """
        Given a task path, obtain the corresponding subtask, or raise
        `TaskNotFoundException`.

        Do not override this method; override `_get_subtask()` instead.

        :Parameters:
            task_path : list
                List of strings corresponding to the unique hierarchy of a
                task
            clean : bool
                Whether the subtask should be reinstantiated

        :returns: The requested subtask
        """
        subtask = self
        while task_path:
            task = subtask
            consumed_path, subtask = task._get_subtask(task_path)
            task_path = task_path[len(consumed_path):]
        if clean and not subtask._status == STATUS_STOPPED:
            task_path, subtask = task._get_subtask(consumed_path, clean=True)
        return subtask


    def get_worker(self):
        """
        Get the worker running this task, or None if the task is standalone.
        """

        if self.parent:
            return self.parent.get_worker()
        else:
            return None


    def request_worker(self, *args, **kwargs):
        """
        Requests a worker for a subtask from the task's parent.

        :returns: The requested worker.
        """

        return self.parent.request_worker(*args, **kwargs)


    def start(self, args={}, subtask_key=None, workunit=None, task_id=-1, \
              callback=None, callback_args={}, errback=None, errback_args={}):
        """
        Start the task.

        The task's work will be spawned in a separate thread and run
        asynchronously.

        XXX Corbin: Make this return a Deferred instead of those last four
        args.

        :Parameters:
            args : dict
                Keyword arguments to pass to the work function
            subtask_key
                The subtask to run, or None for no subtask
            workunit
                The workunit key or data
            task_id : int
                The ID of the task being run
            callback : callable
                A callback to be called after the task completes its work
            callback_args : dict
                Keyword arguments to pass to the callback
            errback : callable
                A callback to be called if an exception happens during task
                execution
            errback_args : dict
                Arguments to pass to errback
        """

        #if this was subtask find it and execute just that subtask
        if subtask_key:
            self.logger.debug('Task - starting subtask %s' % subtask_key)
            split = subtask_key.split('.')
            subtask = self.get_subtask(split, True)
            subtask.logger = get_task_logger(self.get_worker().worker_key, \
                                             task_id, \
                                             subtask_key, workunit)
            self.logger.debug('Task - got subtask')
            self.work_deferred = threads.deferToThread(subtask._start, args, \
                                            callback, callback_args)

        elif self._status == STATUS_RUNNING:
            # only start root task if not already running
            return

        else:
            #else this is a normal task just execute it
            
            self.logger.debug('Task - starting task: %s' % self)
            if self.get_worker():
                self.work_deferred = threads.deferToThread(self._start, args,
                    callback, callback_args)
            else:
                # Standalone; just return the result of work()
                return self.work()

        if errback and self.work_deferred:
            self.work_deferred.addErrback(errback, **errback_args)

        return 1


    def start_subtask(self, subtask, args, workunit, task_id, callback, \
                      callback_args):
        """
        Start a subtask.

        This method sets additional parameters needed by
        a subtask, such as workunit selection, and then starts the requested
        subtask.

        This method should be overridden by subclasses of `Task` that wish to
        include workunits or other subtask specific functionality. This method
        sets up logging for the subtask, so subclasses should probably call
        `super()` as well.

        XXX Corbin: Should this return a Deferred?
        XXX Corbin: This function is completely stubbed. Should it raise NIE,
        or what?

        :Parameters:
            args
                Arguments to pass to the subtask
            subtask
                Key of subtask to be run
            workunit
                Workunit key or data
            task_id
                ID of the task to run
            callback
                Callback to execute after task completes
            callback_args
                Arguments to pass to callback
        """

        pass


    def subtask_started(self, subtask, id):
        """
        Callback used to inform the task that a previously queued subtask has
        been started on a worker.

        :Parameters:
            subtask
                Subtask path
            id
                ID for the workunit
        """

        self.logger.info('*** Workunit Started - %s:%s ***' % (subtask, id))


    def status(self):
        """
        The current status of the task.

        Overrideable.

        XXX Corbin: This should be a property. I patched it to be a property
        at one point; not sure where the patch went.
        """

        return self._status


    def work(self, *args, **kwargs):
        """
        Do the actual computation of the task.

        This method is abstract and must be implemented by any subclasses.
        """
        raise NotImplementedError


    def progress(self):
        """
        Return the current progress of the task, as an integer between 0 and
        100.

        The default implementation returns 100 if the task is finished and 0
        otherwise.
        """

        return 100 if self.status == STATUS_COMPLETE else 0
