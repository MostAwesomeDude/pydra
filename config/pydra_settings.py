# Django settings for pydra project.
VERSION = '0.5.1'

DEBUG = True
TEMPLATE_DEBUG = DEBUG

# Base directory for storing all files created at runtime
# this includes encryption keys, logs, tasks, etc
RUNTIME_FILES_DIR = '/home/simpson/pydra/lib'

# Directory where process ids are stored.
RUNTIME = '/home/simpson/pydra/run'



DATABASE_ENGINE = 'sqlite3'                         # 'mysql', 'oracle', 'postgresql_psycopg2', 'postgresql', or 'sqlite3'.
DATABASE_NAME = '%s/pydra.db3' % RUNTIME_FILES_DIR  # Database file path for sqlite3.

                                                    # These are not used for sqlite3:
DATABASE_USER = ''
DATABASE_PASSWORD = ''
DATABASE_HOST = ''                                  # Set to empty string for localhost.
DATABASE_PORT = ''                                  # Set to empty string for default.


# absolute path to the docroot of this site
DOC_ROOT = '/home/simpson/pydra/pydra_site'

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# If running in a Windows environment this must be set to the same as your
# system time zone.
TIME_ZONE = 'America/Chicago'

# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'en-us'

SITE_ID = 1

# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = True

# Make this unique, and don't share it with anybody.
SECRET_KEY = 'dk#^frv&4y_&7a90#bn62@t-1jyc@q9*!69y7zq&@&8)g#szu4'

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.load_template_source',
    'django.template.loaders.app_directories.load_template_source',
)

INSTALLED_APPS = (
    'pydra'
)

#Logging
import logging
LOG_LEVEL = logging.DEBUG
LOG_DIR = '/home/simpson/pydra/log'
LOG_FILENAME_MASTER = '%s/master.log' % LOG_DIR
LOG_FILENAME_NODE   = '%s/node.log' % LOG_DIR
LOG_SIZE = 10000000
LOG_BACKUP = 10
LOG_ARCHIVE = '%s/archive' % LOG_DIR


# Connection settings for cluster.  These settings affect the local node only
# some settings only apply to a Master or to a Node. 
#
#   HOST is the name that pydra will be exposed to and used as part of an
#        identifier.
#
#   PORT is the port that Node will listen on for the Master to connect.
#
#   CONTROLLER_PORT is the port Master listens for Controllers on
# 
#   WORKER_PORT is the port Node listens for Workers on
HOST = 'localhost'
PORT = 11890
CONTROLLER_PORT = 18801
WORKER_PORT = 18800

# Directory in which to place tasks to deploy to Pydra.  Tasks will be parsed
# and versioned.  Processed Tasks are stored in TASKS_DIR_INTERNAL.  Modifying
# files within TASKS_DIR_INTERNAL _WILL_ break things.
#
# tarziped files will be stored in TASK_SYNC_CACHE to speed up task
# synchronization.  Setting this param to None turns off the cache
TASKS_DIR = '%s/tasks' % RUNTIME_FILES_DIR
TASKS_DIR_INTERNAL = '%s/tasks_internal' % RUNTIME_FILES_DIR
TASKS_SYNC_CACHE  = '%s/task_sync_cache' % RUNTIME_FILES_DIR

# Automatically add nodes found with autodiscovery 
MULTICAST_ALL = False 
