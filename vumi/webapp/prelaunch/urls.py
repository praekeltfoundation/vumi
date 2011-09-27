from django.conf.urls.defaults import patterns
from django.views.generic.base import TemplateView
from vumi.webapp.prelaunch import views

urlpatterns = patterns('',
    (r'^$', TemplateView.as_view(template_name='index.html')),
    (r'^index\.html$', TemplateView.as_view(template_name='index.html')),
    (r'^thanks\.html$', views.thanks),
    (r'^contact\.html$', TemplateView.as_view(template_name='contact.html')),
    (r'^feature\.html$', TemplateView.as_view(template_name='feature.html')),
    (r'^open-source\.html$', TemplateView.as_view(
                                            template_name='open-source.html')),
    (r'^terms\.html$', TemplateView.as_view(template_name='terms.html')),
    (r'^process\.html$', views.process),
    (r'^qr', TemplateView.as_view(template_name='qr/index.html')),
)
