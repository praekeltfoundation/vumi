from django.db import models
from django.contrib import admin


class Registrant(models.Model):
    """Someone who signed up prelaunch"""
    full_name = models.CharField(blank=True, max_length=255)
    email_address = models.CharField(blank=True, max_length=255)
    created_at = models.DateTimeField(auto_now=True)

    def __unicode__(self):
        return u"%s <%s>" % (self.full_name, self.email_address)

admin.site.register(Registrant)
