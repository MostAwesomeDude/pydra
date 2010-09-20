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

from pydra.cluster.tasks import Task, TaskNotFoundException, \
    STATUS_CANCELLED, STATUS_FAILED, STATUS_STOPPED, STATUS_RUNNING, \
    STATUS_PAUSED, STATUS_COMPLETE

import logging
logger = logging.getLogger('root')


class SubTaskWrapper():
    """
    SubTaskWrapper - class used to store additional information
    about the relationship between a container and a subtask.
    
    This class acts as a proxy for all Task methods
    
        percentage - the percentage of work the task accounts for.
    """
    def __init__(self, task, parent, percentage):
        self.task = task
        self.percentage = percentage
        self.parent = parent

    def get_subtask(self, task_path):
        return self.task.get_subtask(task_path)

    def get_key(self):
        """
        Returns the index of this wrapper
        """
        index = self.parent.subtasks.index(self)
        base = self.parent.get_key()
        
        return '%s.%s' % (base, index)

    def get_worker(self):
        """
        Retrieves the worker running this task.  This function is recursive and bubbles
        up through the task tree till the worker is reached
        """
        return self.parent.get_worker()

    def request_worker(self, *args, **kwargs):
        return self.parent.request_worker(*args, **kwargs)

    def _stop(self, *args, **kwargs):
        return self.task._stop(*args, **kwargs)

    def __repr__(self):
        return self.task.__repr__()


class TaskContainer(Task):
    """
    TaskContainer - an extension of Task that contains other tasks

    TaskContainer does no work itself.  Its purpose is to allow a bigger job
    to be broken into discrete functions.  IE.  downloading and processing.
    """
    def __init__(self, msg, sequential=True):
        Task.__init__(self, msg)
        self.subtasks = []
        self.sequential = sequential
        for task in self.subtasks:
            task.parent = self

    def add_task(self, task, percentage=None):
        """
        Adds a task to the container
        """
        subtask = SubTaskWrapper(task, self, percentage)
        self.subtasks.append(subtask)
        task.parent=subtask
        task.id = '%s-%d' % (self.id,len(self.subtasks))

    def reset(self):
        for subtask in self.subtasks:
            subtask.task.reset()

    def _get_subtask(self, task_path, clean=False):
        """
        Overridden to deal with the oddity of how ContainerTask children are
        indicated in keys.  Children are indicated as integer followed by
        another entry for the class itself. indexes because there may be more
        than one of the same class.  Task.get_subtask(...) will break if the
        first element in the task_path is an integer.
        
        If the task_path indicates the child of a ContainerTask the index will
        also be popped off in this function and the subsequent call with go
        directly to the subtask rather than through the wrapper
        """
        if len(task_path) == 1:
            if task_path[0] == self.__class__.__name__:
                return task_path, self
            else:
                raise TaskNotFoundException("Task not found")
        
        try:
            # get index and recurse into subtask
            index = int(task_path[1])
            task = self.subtasks[index].task
            return task_path[:2], task
        except (ValueError, IndexError):
            raise TaskNotFoundException("Task not found")

    def _work(self, **kwargs):
        # start first subtask
        self._current_subtask = 0
        self._start_subtask(args=kwargs)

    def _start_subtask(self, args={}):
        """
        Starts a subtask
        """
        subtask = self.subtasks[self._current_subtask]
        subtask.task.logger = self.logger
        logger.debug('TaskContainer - starting subtask: %s' % subtask)
        deferred = subtask.task.start(args=args)
        deferred.addCallback(self._subtask_complete)
        logger.debug('TaskContainer - STARTED! subtask: %s' % subtask)

    def _subtask_complete(self, results):
        """
        Callback when a subtask is complete.  Will either move on
        to the next subtask or call the callback
        """

        self._current_subtask += 1
        if self._current_subtask < len (self.subtasks):
            self._start_subtask(args=results)
        else:
            self._complete(results)

    def _stop(self):
        """
        Overridden to call stop on all children
        """
        Task._stop(self)
        for subtask in self.subtasks:
            subtask._stop()

    def progress(self):
        """
        progress - returns the progress as a number 0-100.
        
        A container task's progress is a derivitive of its children.
        the progress of the child counts for a certain percentage of the
        progress of the parent.  This weighting can be set manually or
        divided evenly by calculatePercentage()
        """
        progress = 0
        auto_total = 100
        auto_subtask_count = len(self.subtasks)
        for subtask in self.subtasks:
            if subtask.percentage:
                auto_total -= subtask.percentage
                auto_subtask_count -= 1
        auto_percentage = auto_total / auto_subtask_count / float(100)
        
        for subtask in self.subtasks:
            if subtask.percentage:
                percentage = subtask.percentage/float(100)
            else:
                percentage = auto_percentage
            
            # if task is done it complete 100% of its work
            if subtask.task._status == STATUS_COMPLETE:
                progress += 100*percentage
            
            # task is only partially complete
            else:
                progress += subtask.task.progress()*percentage
        
        return progress

    def progressMessage(self):
        """ 
        returns a plain text status message
        """
        for subtask in self.subtasks:
            if subtask.task._status == STATUS_RUNNING:
                return subtask.task.progressMessage()
        
        return None

    def status(self):
        """
        getStatus - returns status of this task.  A container task's status is 
        a derivitive of its children.
        
        failed - if any children failed, then the task failed
        running - if any children are running then the task is running
        paused - paused if no other children are running
        complete - complete if all children are complete
        stopped - default response if no other conditions are met
        """
        has_paused = False
        has_unfinished = False
        has_failed = False
        
        for subtask in self.subtasks:
            status = subtask.task.status()
            if status == STATUS_RUNNING:
                # we can return right here because if any child is running the 
                # container is considered to be running.  This overrides STATUS_FAILED
                # because we want to indicate that the task is still doing something
                # even though it should be stopped
                return STATUS_RUNNING
            
            elif status == STATUS_FAILED:
                # mark has_failed flag.  this can still be overridden by a running task
                has_failed = True
                has_unfinished = True
            
            elif status == STATUS_PAUSED:
                # mark has_paused flag, can be overriden by failed or running tasks
                has_paused = True;
                has_unfinished = True
            
            elif status == STATUS_STOPPED:
                #still need to mark this status to indicate we arent complete
                has_unfinished = True
        
        if has_failed:
            return STATUS_FAILED
        
        if has_paused:
            return STATUS_PAUSED
        
        if not has_unfinished:
            return STATUS_COMPLETE
        
        # its not any other status, it must be stopped
        return STATUS_STOPPED
