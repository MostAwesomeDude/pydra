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


class WorkerProxy():
    """
    Proxy for a Worker used when running a task from the commandline.  When
    run from the command line or in another script there will be no worker
    associated with the Task.  Several functions of the task require a worker
    this proxy class fills in that role by implementing the same methods, 
    though they rarely actually do anything
    """

    worker_key = 'Worker_Proxy'

    def get_worker(self):
        return self