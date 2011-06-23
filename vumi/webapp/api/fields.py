from urlparse import urlparse
from django.db import models
from django.core.validators import URLValidator
from django.utils.translation import ugettext_lazy as _

# Allow south to introspect our custom fields
from south.modelsinspector import add_introspection_rules
add_introspection_rules([],
                        ['vumi\.webapp\.api\.fields\.AuthenticatedURLField'])


class AuthenticatedURLValidator(URLValidator):

    def __call__(self, value):
        parts = urlparse(value)
        netloc = parts.netloc
        if '@' in netloc:
            domain_only = netloc.split('@')[1]
            new_parts = parts._replace(netloc=domain_only)
            super(AuthenticatedURLValidator, self).__call__(new_parts.geturl())
        else:
            super(AuthenticatedURLValidator, self).__call__(value)


class AuthenticatedURLField(models.CharField):
    """
    Like URLField with the only difference being that this Field allows and
    validates URLs with embedded username & password credentials.

    eg. http://username:password@host:port/path

    While storing these is generally a bad idea, we're storing them for remote
    callbacks. If we want to allow passwordless auth for that we're either
    going to need to pull some OAuth tricks or require IP lockdown.

    """
    description = _("Authenticated URL")

    def __init__(self, verbose_name=None, name=None, verify_exists=True,
                 **kwargs):
        kwargs['max_length'] = kwargs.get('max_length', 200)
        models.CharField.__init__(self, verbose_name, name, **kwargs)
        self.validators.append(AuthenticatedURLValidator(
                verify_exists=verify_exists))
