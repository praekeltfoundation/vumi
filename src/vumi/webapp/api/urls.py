from django.conf.urls.defaults import *
from piston.resource import Resource
from piston.authentication import HttpBasicAuthentication
from vumi.webapp.api import handlers
from vumi.webapp.api import views

ad = {'authentication': HttpBasicAuthentication(realm="Vumi")}
url_callback_resource = Resource(handler=handlers.URLCallbackHandler, **ad)
conversation_resource = Resource(handler=handlers.ConversationHandler, **ad)

urlpatterns = patterns('',
    (r'^conversation\.yaml$', conversation_resource, {
        'emitter_format': 'yaml'
    }, 'conversation'),
    (r'^account/callbacks\.json$', url_callback_resource, {}, 'url-callbacks-list'),
    (r'^account/callbacks/(?P<callback_id>\d+)\.json$', url_callback_resource, {}, 'url-callback'),
    (r'^callback\.html$', views.example_sms_callback, {}, 'sms-example-callback'),
)

# gateways
urlpatterns += patterns('',
    (r'^sms/clickatell/', 
        include('vumi.webapp.api.gateways.clickatell.urls', 
                    namespace='clickatell')),
    (r'^sms/opera/', 
        include('vumi.webapp.api.gateways.opera.urls', 
                    namespace='opera')),
    (r'^sms/e-scape/', 
        include('vumi.webapp.api.gateways.e_scape.urls', 
                    namespace='e-scape')),
    (r'^sms/techsys/',
        include('vumi.webapp.api.gateways.techsys.urls',
                    namespace='techsys')),
    (r'^sms/smpp/', 
        include('vumi.webapp.api.gateways.smpp.urls',
                    namespace='smpp')),
)
