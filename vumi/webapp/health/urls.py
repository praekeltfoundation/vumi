from django.conf.urls.defaults import patterns
from django.http import HttpResponse


def general_health(request):
    # we're still able to respond
    return HttpResponse("OK")

urlpatterns = patterns('',
    (r'^/$', general_health),
)
