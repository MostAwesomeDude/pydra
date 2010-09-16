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
import os
import shutil
import sys
import tempfile

from django.db import connection

from pydra.tests import MuteStdout


class TestCase(unittest.TestCase):
    """
    Specialized TestCase that creates and destroys the django environment
    and database required for some tests.
    """
    test_db = None
    test_db_dir = None
    
    def __init__(self, *args, **kwargs):
        
        super(TestCase, self).__init__(*args, **kwargs)
        
        # XXX setupClass and tearDownClass are new in 2.7.  Monkey patch setUp()
        # and tearDown() for all other versions of python
        def setUpBoth():
            self.__class__.setUpClass()
            self._setUp()
        
        def tearDownBoth():
            self._tearDown()
            self.__class__.tearDownClass()
        
        self._setUp = self.setUp
        self.setUp = setUpBoth
        self._tearDown = self.tearDown
        self.tearDown = tearDownBoth
    
    @classmethod
    def setUpClass(cls):
        # add the config dir to the path so the default dir can be found
        sys.path.insert(0, './config')
        import pydra_settings
        
        # if using sqlite, update the database location to a temp dir
        if pydra_settings.DATABASE_ENGINE == 'sqlite3':
            cls.test_db_dir = tempfile.mkdtemp()
            pydra_settings.DATABASE_NAME = '%s/pydra.db3' % cls.test_db_dir
        
        # point django at the test config
        if not os.environ.has_key('DJANGO_SETTINGS_MODULE'):
            os.environ['DJANGO_SETTINGS_MODULE'] = 'pydra_settings'
        
        # create test db.
        with MuteStdout():
            cls.test_db = connection.creation.create_test_db(autoclobber=True)
    
    @classmethod
    def tearDownClass(cls):
        try:
            sys.path.remove('./config')
        except ValueError:
            pass
        try:
            del os.environ['DJANGO_SETTINGS_MODULE']
        except KeyError:
            pass
        with MuteStdout():
            connection.creation.destroy_test_db(cls.test_db)
            
        if cls.test_db_dir:
            try:
                os.rmdir(cls.test_db_dir)
            except OSError:
                #print "Warning: Directory %s not empty" % cls.test_db_dir
                try:
                    os.removedirs(cls.test_db_dir)
                except OSError:
                    #print "Warning: Directory %s still dirty" % cls.test_db_dir
                    try:
                        shutil.rmtree(cls.test_db_dir)
                    except OSError:
                        #print 'Warning couldn''t remove tree'
                        pass