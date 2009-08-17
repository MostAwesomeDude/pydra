#!/usr/bin/env python
"""
This script is used a custom version of manage.py that django creates for
sites when they are created by the django tool.  This file is designed to
work from any location.  It uses pydra.config which should be on sys.path
to locate the pydra_settings file.
"""

from django.core.management import execute_manager, ManagementUtility

from pydra.config import configure_django_settings
configure_django_settings()
import pydra_settings as settings

if __name__ == "__main__":
    # call the ManagementUtility without setting up the environment because
    # this script assumes that pydra is already on the sys.path.
    utility = ManagementUtility(None)
    utility.execute()
