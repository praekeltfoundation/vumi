from django.conf.urls.defaults import patterns, include
from piston.resource import Resource
from piston.authentication import HttpBasicAuthentication
from vumi.webapp.api import handlers
from vumi.webapp.api import views

ad = {'authentication': HttpBasicAuthentication(realm="Vumi")}
url_callback_resource = Resource(handler=handlers.URLCallbackHandler, **ad)
conversation_resource = Resource(handler=handlers.ConversationHandler, **ad)

urlpatterns = patterns('',
    (r'^conversation\.yaml$', conversation_resource,
     {'emitter_format': 'yaml'}, 'conversation'),
    (r'^account/callbacks\.json$', url_callback_resource,
     {},
     'url-callbacks-list'),
    (r'^account/callbacks/(?P<callback_id>\d+)\.json$', url_callback_resource,
     {}, 'url-callback'),
    (r'^callback\.html$', views.example_sms_callback,
     {}, 'sms-example-callback'),
)

# gateways
urlpatterns += patterns('',
    (r'^sms/', include('vumi.webapp.api.base.urls')),
    # receive & receipt callbacks for clickatell HTTP api
    (r'^sms/clickatell/',
        include('vumi.webapp.api.gateways.clickatell.urls',
                    namespace='clickatell')),
    # receive & receipt callbacks for Opera HTTP api
    (r'^sms/opera/',
        include('vumi.webapp.api.gateways.opera.urls',
                    namespace='opera')),
    (r'^sms/smpp/',
        include('vumi.webapp.api.gateways.smpp.urls',
                    namespace='smpp')),
)
