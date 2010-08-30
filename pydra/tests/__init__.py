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
import sys
import logging

from pydra.config import configure_django_settings, load_settings


def setup_test_environment():
    """
    configures test environment:
        * setup django environment
        * load pydra settings
        * silence logging
    """    
    # configure pydra and django environment
    configure_django_settings()
    settings = load_settings()
    
    # silence logging
    settings.LOG_LEVEL = 100
    logger = logging.getLogger('root')
    logger.level = 100


class MuteStdout(object):
    """ context manager that mutes stdout """
    def __enter__( self ):
        self.stdout = sys.stdout
        sys.stdout = self
    
    def __exit__( self, type, value, tb ):
        sys.stdout = self.stdout
    
    def write(self, str):
        """ ignore all calls to write """
        pass