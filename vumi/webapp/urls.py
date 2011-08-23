from django.conf.urls.defaults import patterns, url, include
from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    (r'^', include('vumi.webapp.prelaunch.urls', namespace="prelaunch")),
    (r'^api/v1/', include('vumi.webapp.api.urls', namespace="api")),
    (r'^health', include('vumi.webapp.health.urls', namespace="health")),
    # Uncomment the admin/doc line below and add 'django.contrib.admindocs'
    # to INSTALLED_APPS to enable admin documentation:
    (r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    (r'^admin/', include(admin.site.urls)),
)

urlpatterns += patterns('django.contrib.staticfiles.views',
   url(r'^static/(?P<path>.*)$', 'serve'),
)
