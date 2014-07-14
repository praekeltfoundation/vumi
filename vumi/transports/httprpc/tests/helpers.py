from urllib import urlencode

from twisted.internet.defer import inlineCallbacks, returnValue

from zope.interface import implements

from vumi.errors import VumiError
from vumi.tests.helpers import IHelper, generate_proxies
from vumi.transports.tests.helpers import TransportHelper
from vumi.utils import http_request_full


class HttpRpcTransportHelperError(VumiError):
    """Error raised when the HttpRpcTransportHelper encouters an error."""


class HttpRpcTransportHelper(object):
    """
    Test helper for subclasses of
    :class:`~vumi.transports.httprpc.HttpRpcTransport`.

    Adds support for making HTTP requests to the HTTP RPC transport to the
    base :class:`~vumi.transports.tests.helpers.TransportHelper`.

    :param dict request_defaults: Default URL parameters for HTTP requests.

    Other parameters are the same as for
    :class:`~vumi.transports.tests.helpers.TransportHelper`.
    """

    implements(IHelper)

    def __init__(self, transport_class, use_riak=False, request_defaults=None,
                 **msg_helper_args):
        self._transport_helper = TransportHelper(
            transport_class, use_riak=use_riak, **msg_helper_args)

        if request_defaults is None:
            request_defaults = {}
        self.request_defaults = request_defaults
        self.transport_url = None

        generate_proxies(self, self._transport_helper)

    def setup(self, *args, **kw):
        return self._transport_helper.setup(*args, **kw)

    def cleanup(self):
        return self._transport_helper.cleanup()

    @inlineCallbacks
    def get_transport(self, config, cls=None, start=True):
        transport = yield self._transport_helper.get_transport(
            config, cls=cls, start=True)
        self.transport_url = transport.get_transport_url(config['web_path'])
        returnValue(transport)

    def mk_request_raw(self, suffix='', params=None, data=None, method='GET'):
        """
        Make an HTTP request, ignoring this helper's ``request_defaults``.

        :param str suffix: Suffix to add to the transport's URL.
        :param dict params: A dictionary of URL parameters to append to the URL
                            as a query string or None for no URL parameters.
        :param str data: Request body or None for no request body.
        :param str method: HTTP method to use for the request.

        :raises HttpRpcTransportHelperError:
            When invoked before calling :meth:`get_transport`.
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

        :param str _suffix: Suffix to add to the transport's URL.
        :param str _data: Request body or None for no request body.
        :param str _method: HTTP method to use for the request.
        :param \*\*kw: URL query string parameters.

        :raises HttpRpcTransportHelperError:
            When invoked before calling :meth:`get_transport`.

        The ``_`` prefixes on the function parameter names are to
        make accidental clashes with URL query parameter names less
        likely.
        """
        params = self.request_defaults.copy()
        params.update(kw)
        return self.mk_request_raw(
            suffix=_suffix, params=params, data=_data, method=_method)
