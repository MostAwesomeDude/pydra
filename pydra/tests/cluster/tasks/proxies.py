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

from pydra.tests.proxies import CallProxy

from pydra.cluster.tasks import STATUS_STOPPED


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