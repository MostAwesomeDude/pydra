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

from django.db import models
from threading import Lock

import dbsettings


""" ================================
Settings
================================ """
from _mysql_exceptions import ProgrammingError
try:
    class PydraSettings(dbsettings.Group):
        host        = dbsettings.StringValue('host', 'IP Address or hostname for this server.  This value will be used by all nodes in the cluster to connect', default='localhost')
        port        = dbsettings.IntegerValue('port','Port this server listens on for Workers to connect to', default=18800)
        controller_port = dbsettings.IntegerValue('controller_port','Port this server listens on for Controllers', default=18801)
        tasks_dir = dbsettings.StringValue('tasks_dir', 'Directory where tasks are stored.  Absolute paths are prefered.', default='./pydra_server/task_cache')
        tasks_dir_internal = dbsettings.StringValue('tasks_dir_internal', 'Internal directory where tasks are stored.  Absolute paths are prefered.', default='./pydra_server/task_cache_internal')
        multicast_all    = dbsettings.BooleanValue('multicast_all', 'Automatically use all the nodes found', default=False)
    pydraSettings = PydraSettings('Pydra')

except ProgrammingError:
    pass #table hasnt been created yet 




""" ================================
Models
================================ """


class Node(models.Model):
    """
    Represents a node in the cluster
    """
    host            = models.CharField(max_length=255)
    port            = models.IntegerField(default=11890)
    cores_available = models.IntegerField(null=True)
    cores           = models.IntegerField(null=True)

    # key given to node for use by its workers
    key             = models.CharField(max_length=50, null=True)

    # keys used by master to connect to the node
    # this keypair is generated by the Master, the private key
    # is passed to the Node the first time it sees it.
    pub_key         = models.TextField(null=True)

    cpu_speed       = models.IntegerField(null=True)
    memory          = models.IntegerField(null=True)
    seen            = models.IntegerField(default=False)

    # non-model fields
    ref             = None
    _info           = None
    pub_key_obj     = None

    def __str__(self):
        return '%s:%s' % (self.host, self.port)

    def status(self):
        ret = 1 if self.ref else 0
        return ret

    class Meta:
        permissions = (
            ("can_edit_nodes", "Can create and edit nodes"),
        )

    def load_pub_key(self):
        """
        Load public key object from raw data stored in the model
        """
        if self.pub_key_obj:
            return self.pub_key_obj

        elif not self.pub_key:
            return None

        else:
            from django.utils import simplejson
            from Crypto.PublicKey import RSA

            pub_raw = simplejson.loads(self.pub_key)
            pub = [long(x) for x in pub_raw]
            pub_key_obj = RSA.construct(pub)
            self.pub_key_obj = pub_key_obj

            return  pub_key_obj


class TaskInstanceManager(models.Manager):
    """
    Custom manager overridden to supply pre-made queryset for queued and running
    tasks
    """
    def queued(self):
        return self.filter(status=None, started=None)

    def running(self):
        return self.filter(status=None).exclude(started=None)



class TaskInstance(models.Model):
    """
    Represents and instance of a Task.  This is used to track when a Task was 
    run and whether it completed.

    task_key:      Key that identifies the code to be run
    subtask_key:   Path within the task that identifies the child task to run
    args:          Dictionary of arguments passed to the task
    queued:        Datetime when this task instance was queued
    started:       Datetime when this task instance was started
    completed:     Datetime when this task instance completed successfully or 
                   failed
    worker:        Identifier for the worker that ran this task
    status:        Current Status of the task instance
    log_retrieved: Was the logfile retrieved from the remote worker
    """
    task_key        = models.CharField(max_length=255)
    subtask_key     = models.CharField(max_length=255, null=True)
    args            = models.TextField(null=True)
    queued          = models.DateTimeField(auto_now_add=True)
    started         = models.DateTimeField(null=True)
    completed       = models.DateTimeField(null=True)
    worker          = models.CharField(max_length=255, null=True)
    status          = models.IntegerField(null=True)
    log_retrieved   = models.BooleanField(default=False)

    objects = TaskInstanceManager()

    ######################
    # non-model attributes
    ######################

    # scheduling-related
    priority         = 5
    running_workers  = [] # running workers keys (excluding the main worker)
    waiting_workers  = [] # workers waiting for more workunits
    last_succ_time   = None # when this task last time gets a worker
    _worker_requests = [] # (args, subtask_key, workunit_key)

    # others
    main_worker  = None
    _request_lock = Lock()

    def compute_score(self):
        """
        Computes a priority score for this task, which will be used by the
        scheduler.

        Empirical analysis may reveal a good calculation formula. But in
        general, the following guideline is useful:
        1) Stopped tasks should have higher scores. At least for the current
           design, a task can well proceed even with only one worker. So 
           letting a stopped task run ASAP makes sense.
        2) A task with higher priority should obviously have a higher score.
        3) A task that has been out of worker supply for a long time should
           have a relatively higher score.
        """
        return self.priority 

    def queue_worker_request(self, request):
        """
        A worker request is a tuple of:
        (requesting_worker_key, args, subtask_key, workunit_key).
        """
        with self._request_lock:
            self._worker_requests.append(request)

    def pop_worker_request(self):
        """
        A worker request is a tuple of:
        (requesting_worker_key, args, subtask_key, workunit_key).
        """
        with self._request_lock:
            try:
                return self._worker_requests.pop(0)
            except IndexError:
                return None

    def poll_worker_request(self):
        """
        Returns the first worker request in the queue without removing
        it.
        """
        with self._request_lock:
            try:
                return self._worker_requests[0]
            except IndexError:
                return None

    class Meta:
        permissions = (
            ("can_run", "Can run tasks on the cluster"),
            ("can_stop_all", "Can stop anyone's tasks")
        )


class WorkUnit(models.Model):
    """
    Workunits are subtask requests that can be distributed by pydra.  A
    workunit is generally the smallest unit of work for a task.  This
    model represents key data points about them.

    subtask_key:   Path within the task that identifies the child task to run
    workunit_key:  key that uniquely identifies this workunit within the 
                   datasource for the task.
    args:          Dictionary of arguments passed to the task
    started:       Datetime when this workunit was started
    completed:     Datetime when this workunit completed successfully or 
                   failed
    worker:        Identifier for the worker that ran this workunit
    status:        Current Status of the task instance
    log_retrieved: Was the logfile retrieved from the remote worker
    """
    task_instance   = models.ForeignKey(TaskInstance, related_name='workunits')
    subtask_key     = models.CharField(max_length=255)
    workunit_key    = models.CharField(max_length=255)
    args            = models.TextField(null=True)
    started         = models.DateTimeField(null=True)
    completed       = models.DateTimeField(null=True)
    worker          = models.CharField(max_length=255, null=True)
    status          = models.IntegerField(null=True)
    log_retrieved   = models.BooleanField(default=False)

