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

from pydra.tests.proxies import ModuleManagerProxy, RemoteProxy
from pydra.cluster.node import worker_manager


def suite():
    """
    Build a test suite from all the test suites in this module
    """
    return unittest.TestSuite([
            unittest.TestLoader().loadTestsFromTestCase(WorkerManager),
        ])


class WorkerManager(unittest.TestCase):
    
    def setUp(self):
        self.tearDown()
        self.wm = worker_manager.WorkerManager()
        self.manager = ModuleManagerProxy()
        self.manager.testcase = self
        self.wm._register(self.manager)
        self.wm.master = RemoteProxy('master')
    
    def tearDown(self):
        pass

    def test_clean_up_finished_worker(self):
        raise NotImplementedError

    def test_receive_results(self):
        raise NotImplementedError

    def test_request_release_worker(self):
        raise NotImplementedError

    def test_request_worker(self):
        raise NotImplementedError

    def test_run_task(self):
        raise NotImplementedError

    def test_send_results(self):
        raise NotImplementedError

    def test_send_results_subtask(self):
        raise NotImplementedError

    def test_send_results_failed(self):
        raise NotImplementedError

    def test_stop_task(self):
        raise NotImplementedError

    def test_subtask_started(self):
        raise NotImplementedError

    def test_task_status(self):
        raise NotImplementedError

    def test_task_status_no_worker(self):
        """
        Tests task status when the worker requested is not running
        """
        raise NotImplementedError

    def test_worker_stopped(self):
        raise NotImplementedError
