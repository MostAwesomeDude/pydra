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

from threading import Event
from twisted.internet import reactor
from pydra.cluster.tasks.tasks import Task
from pydra.cluster.tasks import STATUS_STOPPED

from pydra.tests.proxies import CallProxy


class WorkerProxy():
    """
    Class for proxying worker functions
    """
    worker_key = "WorkerProxy"

    def __init__(self):
        self.request_worker = CallProxy(None, False)
        self.request_worker_release = CallProxy(None, False)

    def get_worker(self):
        return self

    def get_key(self):
        return None


class StartupAndWaitTask(Task):
    """
    Task that runs indefinitely.  Used for tests that
    require a task with state STATUS_RUNNING.  This task
    uses a lock so that the testcase can request the lock
    and effectively pause the task at specific places to
    verify its internal state
    """

    def __init__(self):
        self.starting_event = Event()   # used to lock task until _work() is called
        self.running_event = Event()    # used to lock running loop
        self.finished_event = Event()   # used to lock until Task.work() is complete
        self.failsafe = None
        self.data = None
        Task.__init__(self)

    def clear_events(self):
        """
        This clears threads waiting on events.  This function will
        call set() on all the events to ensure nothing is left waiting.
        """
        self.starting_event.set()
        self.running_event.set()
        self.finished_event.set()

    def _work(self, **kwargs):
        """
        extended to add locks at the end of the work
        """
        try:
            # set a failsafe to ensure events get cleared
            self.failsafe = reactor.callLater(2, self.clear_events)
            
            ret = super(StartupAndWaitTask, self)._work(**kwargs)
            self.finished_event.set()
        
        finally:
            if self.failsafe:
                self.failsafe.cancel()
        
        return ret

    def work(self, data=None):
        """
        simple "user defined" work method.  just simulates work being done,
        until an external object modifies the STOP_FLAG flag
        """
        self.data = data
        self.starting_event.set()
        while not self.STOP_FLAG:
            # wait for the running_event.  This  prevents needless looping
            # and still simulates a task that is working
            self.running_event.wait(2)
            
        return {'data':self.data}


class StatusSimulatingTaskProxy():
    """
    Task Proxy for simulating status
    """
    value = 0
    _status = None

    def __init__(self):
        self._status = STATUS_STOPPED

    def status(self):
        return self._status

    def progress(self):
        return self.value
