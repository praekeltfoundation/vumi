from django.conf.urls.defaults import *
from django.http import HttpResponse
from django.views.generic.base import TemplateView

urlpatterns = patterns('',
    (r'^$', TemplateView.as_view(template_name='index.html')),
)
