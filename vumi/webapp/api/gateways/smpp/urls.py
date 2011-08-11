from django.conf.urls.defaults import patterns
from piston.resource import Resource
from piston.authentication import HttpBasicAuthentication
from vumi.webapp.api.gateways.smpp import handlers

ad = {'authentication': HttpBasicAuthentication(realm="Vumi")}

smpp_send_resource = Resource(handler=handlers.SendSMPPHandler, **ad)

urlpatterns = patterns(
    '',
    (r'^send\.json$', smpp_send_resource, {}, 'smpp-send'),
)
