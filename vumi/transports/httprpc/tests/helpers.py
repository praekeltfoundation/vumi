from urllib import urlencode

from twisted.internet.defer import inlineCallbacks, returnValue

from zope.interface import implements

from vumi.errors import VumiError
from vumi.tests.helpers import IHelper
from vumi.transports.tests.helpers import TransportHelper
from vumi.utils import http_request_full


class HttpRpcTransportHelperError(VumiError):
    """Error raised when the HttpRpcTransportHelper encouters an error."""


class HttpRpcTransportHelper(TransportHelper):
    """
    Test helper for subclasses of HttpRpcTransport.

    Adds support for making HTTP requests to the HttpRpcTransport to the
    base TransportHelper.

    :param dict request_defaults: Default URL parameters for HTTP requests.

    Other parameters are the same as for TransportHelper.
    """

    implements(IHelper)

    def __init__(self, transport_class, use_riak=False, request_defaults=None,
                 **msg_helper_args):
        super(HttpRpcTransportHelper, self).__init__(
            transport_class, use_riak=use_riak, **msg_helper_args)
        if request_defaults is None:
            request_defaults = {}
        self.request_defaults = request_defaults
        self.transport_url = None

    @inlineCallbacks
    def get_transport(self, config, cls=None, start=True):
        transport = yield super(HttpRpcTransportHelper, self).get_transport(
            config, cls=cls, start=True)
        self.transport_url = transport.get_transport_url(config['web_path'])
        returnValue(transport)

    def mk_request_raw(self, suffix='', params=None, data=None, method='GET'):
        """
        Make an HTTP request, ignoring this helper's ``request_defaults``.
        """
        if self.transport_url is None:
            raise HttpRpcTransportHelperError(
                "call .get_transport() before making HTTP requests.")
        url = self.transport_url + suffix
        if params is not None:
            url += '?%s' % urlencode(params)
        return http_request_full(url, data=data, method=method)

    def mk_request(self, _suffix='', _data=None, _method='GET', **kw):
        """
        Make an HTTP request.
        """
        params = self.request_defaults.copy()
        params.update(kw)
        return self.mk_request_raw(
            suffix=_suffix, params=params, data=_data, method=_method)
