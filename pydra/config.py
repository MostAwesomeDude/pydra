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

"""
The Pydra configuration module.

This module is used to specify paths used by Pydra. These are used to deal
with platform-specific oddities that occur when installing a package. This
file should be importable as pydra.config allowing these paths to be located.
"""

import imp
import os

CONFIG_DIR = [
    "/etc/pydra",
    "config",
]

pydra_settings = None

def configure_django_settings(settings='pydra_settings'):
    """
    Configures sys.path and DJANGO_SETTINGS_MODULE for Pydra.  Because Pydra
    components are run as applications rather than web-apps these things must
    be setup for django to find them.
    """

    if not os.environ.has_key('DJANGO_SETTINGS_MODULE'):
        os.environ['DJANGO_SETTINGS_MODULE'] = settings


def load_settings():
    """
    Get the settings module loaded into the interpreter.

    After this function is invoked at least once, there are three ways of
    getting at the settings module:

    >>> import pydra_settings
    >>> pydra_settings = pydra.config.pydra_settings
    >>> pydra_settings = pydra.config.load_settings()

    These three statements should be equivalent.

    :returns: The pydra_settings module.
    """

    global pydra_settings

    if not pydra_settings:
        pydra_settings = imp.load_module("pydra_settings",
            *imp.find_module("pydra_settings", CONFIG_DIR))

    return pydra_settings
