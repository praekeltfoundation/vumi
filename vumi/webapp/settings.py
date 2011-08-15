# Django settings for webapp project.

DEBUG = True
TEMPLATE_DEBUG = DEBUG

ADMINS = (
    # ('Your Name', 'your_email@domain.com'),
)

from os.path import abspath, join, dirname, basename
APP_ROOT = abspath(join(dirname(__file__), '..'))
PROJECT_NAME = basename(APP_ROOT)

MANAGERS = ADMINS

DATABASES = {
    'default': {
        'ENGINE':   'django.db.backends.postgresql_psycopg2',
        'NAME':     'vumi',       # Or path to database file if using sqlite3.
        'USER':     'vumi',       # Not used with sqlite3.
        'PASSWORD': 'vumi',       # Not used with sqlite3.
        'HOST':     'localhost',  # Set to empty string for localhost.
                                  # Not used with sqlite3.
        'PORT':     '',           # Set to empty string for default.
                                  # Not used with sqlite3.
    }
}


# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# If running in a Windows environment this must be set to the same as your
# system time zone.
TIME_ZONE = 'UTC'

# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'en-us'

SITE_ID = 1

# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = True

# Absolute path to the directory that holds media.
# Example: "/home/media/media.lawrence.com/"
MEDIA_URL = '/media/'
MEDIA_ROOT = join(APP_ROOT, 'webroot', 'media')
STATIC_URL = '/static/'
STATIC_ROOT = join(APP_ROOT, 'webroot', 'static')


# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash if there is a path component (optional in other cases).
# Examples: "http://media.lawrence.com", "http://example.com/media/"
MEDIA_URL = ''

# URL prefix for admin media -- CSS, JavaScript and images. Make sure to use a
# trailing slash.
# Examples: "http://foo.com/media/", "/media/".
# ADMIN_MEDIA_PREFIX = '/static/media/'

# Make this unique, and don't share it with anybody.
SECRET_KEY = 'x-4ce&vmrq4i%rkzhd0j^%$-%t%l1%@g_9m4eyqdc%#s=74(dh'

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.Loader',
    'django.template.loaders.app_directories.Loader',
    'django.template.loaders.eggs.Loader',
)

MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
)

ROOT_URLCONF = 'vumi.webapp.urls'

TEMPLATE_DIRS = (
    # Put strings here, like "/home/html/django_templates" or
    # "C:/www/django/templates".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
    join(APP_ROOT, 'webapp', 'templates'),
    join(APP_ROOT, 'webapp', 'prelaunch', 'templates', 'www-prelaunch'),
)

INSTALLED_APPS = (
    'django.contrib.admin',
    'django.contrib.admindocs',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'django.contrib.staticfiles',
    'vumi.webapp.api',
    'vumi.webapp.prelaunch',
    'celery',
    'south',
    'django_nose',
    'gunicorn',
)

# link our profile to the django.contrib.auth.models.User
AUTH_PROFILE_MODULE = 'api.Profile'

# for Piston
PISTON_DISPLAY_ERRORS = True

# for Celery
BROKER_HOST = "localhost"
BROKER_PORT = 5672
BROKER_USER = "vumi"
BROKER_PASSWORD = "vumi"
BROKER_VHOST = "/develop"

CELERY_QUEUES = {
    "default": {
        "exchange": "vumi",
        "binding_key": "vumi.webapp",
    }
}
CELERY_DEFAULT_QUEUE = "default"
CELERY_DEFAULT_EXCHANGE_TYPE = "direct"
CELERY_DEFAULT_ROUTING_KEY = "vumi.webapp"

# set the environment VUMI_SKIP_QUEUE to have the Celery tasks evaluated
# immediately, skipping the queue
import os
CELERY_ALWAYS_EAGER = os.environ.get('VUMI_SKIP_QUEUE', False)
CELERY_TASK_SERIALIZER = "json"

SOUTH_TEST_MIGRATE = False

TEST_RUNNER = 'django_nose.NoseTestSuiteRunner'
