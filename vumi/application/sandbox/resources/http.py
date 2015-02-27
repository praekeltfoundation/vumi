import base64
import operator
from StringIO import StringIO

from twisted.internet import reactor
from twisted.internet.defer import succeed, maybeDeferred
from twisted.web.client import WebClientContextFactory, Agent

from OpenSSL.SSL import (
    VERIFY_PEER, VERIFY_FAIL_IF_NO_PEER_CERT, VERIFY_CLIENT_ONCE, VERIFY_NONE,
    SSLv3_METHOD, SSLv23_METHOD, TLSv1_METHOD)

from treq.client import HTTPClient

from vumi.utils import HttpDataLimitError
from vumi.application.sandbox.resources.utils import SandboxResource

try:
    from twisted.web.client import BrowserLikePolicyForHTTPS
    from twisted.internet.ssl import optionsForClientTLS

    class HttpClientPolicyForHTTPS(BrowserLikePolicyForHTTPS):
        """
        This client policy is used if we have Twisted 14.0.0 or newer and are
        not explicitly disabling host verification.
        """
        def __init__(self, ssl_method=None):
            super(HttpClientPolicyForHTTPS, self).__init__()
            self.ssl_method = ssl_method

        def creatorForNetloc(self, hostname, port):
            options = {}
            if self.ssl_method is not None:
                options['method'] = self.ssl_method
            return optionsForClientTLS(
                hostname.decode("ascii"), extraCertificateOptions=options)

except ImportError:
    HttpClientPolicyForHTTPS = None


class HttpClientContextFactory(object):
    """
    This context factory is used if we have a Twisted version older than 14.0.0
    or if we are explicitly disabling host verification.
    """
    def __init__(self, verify_options=None, ssl_method=None):
        self.verify_options = verify_options
        self.ssl_method = ssl_method

    def getContext(self, hostname, port):
        context = self._get_noverify_context()

        if self.verify_options in (None, VERIFY_NONE):
            # We don't want to do anything with verification here.
            return context

        if self.verify_options is not None:
            def verify_callback(conn, cert, errno, errdepth, ok):
                return ok
            context.set_verify(self.verify_options, verify_callback)
        return context

    def _get_noverify_context(self):
        """
        Use ClientContextFactory directly and set the method if necessary.

        This will perform no host verification at all.
        """
        from twisted.internet.ssl import ClientContextFactory
        context_factory = ClientContextFactory()
        if self.ssl_method is not None:
            context_factory.method = self.ssl_method
        return context_factory.getContext()


def make_context_factory(ssl_method=None, verify_options=None):
    if HttpClientPolicyForHTTPS is None or verify_options == VERIFY_NONE:
        return HttpClientContextFactory(
            verify_options=verify_options, ssl_method=ssl_method)
    else:
        return HttpClientPolicyForHTTPS(ssl_method=ssl_method)


class HttpClientResource(SandboxResource):
    """
    Resource that allows making HTTP calls to outside services.

    All command on this resource share a common set of command
    and response fields:

    Command fields:
        - ``url``: The URL to request
        - ``verify_options``: A list of options to verify when doing
            an HTTPS request. Possible string values are ``VERIFY_NONE``,
            ``VERIFY_PEER``, ``VERIFY_CLIENT_ONCE`` and
            ``VERIFY_FAIL_IF_NO_PEER_CERT``. Specifying multiple values
            results in passing along a reduced ``OR`` value
            (e.g. VERIFY_PEER | VERIFY_FAIL_IF_NO_PEER_CERT)
        - ``headers``: A dictionary of keys for the header name and a list
            of values to provide as header values.
        - ``data``: The payload to submit as part of the request.
        - ``files``: A dictionary, submitted as multipart/form-data
            in the request:

            .. code-block:: javascript

                [{
                    "field name": {
                        "file_name": "the file name",
                        "content_type": "content-type",
                        "data": "data to submit, encoded as base64",
                    }
                }, ...]

            The ``data`` field in the dictionary will be base64 decoded
            before the HTTP request is made.

    Success reply fields:
        - ``success``: Set to ``true``
        - ``body``: The response body
        - ``code``: The HTTP response code

    Failure reply fields:
        - ``success``: set to ``false``
        - ``reason``: Reason for the failure

    Example:

    .. code-block:: javascript

        api.request(
            'http.get',
            {url: 'http://foo/'},
            function(reply) { api.log_info(reply.body); });

    """

    DEFAULT_TIMEOUT = 30  # seconds
    DEFAULT_DATA_LIMIT = 128 * 1024  # 128 KB
    agent_class = Agent
    http_client_class = HTTPClient

    def setup(self):
        self.timeout = self.config.get('timeout', self.DEFAULT_TIMEOUT)
        self.data_limit = self.config.get('data_limit',
                                          self.DEFAULT_DATA_LIMIT)

    def _make_request_from_command(self, method, command):
        url = command.get('url', None)
        if not isinstance(url, basestring):
            return succeed(self.reply(command, success=False,
                                      reason="No URL given"))
        url = url.encode("utf-8")

        verify_map = {
            'VERIFY_NONE': VERIFY_NONE,
            'VERIFY_PEER': VERIFY_PEER,
            'VERIFY_CLIENT_ONCE': VERIFY_CLIENT_ONCE,
            'VERIFY_FAIL_IF_NO_PEER_CERT': VERIFY_FAIL_IF_NO_PEER_CERT,
        }
        method_map = {
            'SSLv3': SSLv3_METHOD,
            'SSLv23': SSLv23_METHOD,
            'TLSv1': TLSv1_METHOD,
        }

        if 'verify_options' in command:
            verify_options = [verify_map[key] for key in
                              command.get('verify_options', [])]
            verify_options = reduce(operator.or_, verify_options)
        else:
            verify_options = None
        if 'ssl_method' in command:
            # TODO: Fail better with unknown method.
            ssl_method = method_map[command['ssl_method']]
        else:
            ssl_method = None

        context_factory = make_context_factory(
            verify_options=verify_options, ssl_method=ssl_method)

        headers = command.get('headers', None)
        data = command.get('data', None)
        files = command.get('files', None)

        d = self._make_request(method, url, headers=headers, data=data,
                               files=files, timeout=self.timeout,
                               context_factory=context_factory,
                               data_limit=self.data_limit)
        d.addCallback(self._make_success_reply, command)
        d.addErrback(self._make_failure_reply, command)
        return d

    def _make_request(self, method, url, headers=None, data=None, files=None,
                      timeout=None, context_factory=None,
                      data_limit=None):
        context_factory = (context_factory if context_factory is not None
                           else WebClientContextFactory())

        if headers is not None:
            headers = dict((k.encode("utf-8"), [x.encode("utf-8") for x in v])
                           for k, v in headers.items())

        if data is not None:
            data = data.encode("utf-8")

        if files is not None:
            files = dict([
                (key,
                    (value['file_name'],
                     value['content_type'],
                     StringIO(base64.b64decode(value['data']))))
                for key, value in files.iteritems()])

        agent = self.agent_class(reactor, contextFactory=context_factory)
        http_client = self.http_client_class(agent)

        d = http_client.request(method, url, headers=headers, data=data,
                                files=files, timeout=timeout)

        d.addCallback(self._ensure_data_limit, data_limit)
        return d

    def _ensure_data_limit(self, response, data_limit):
        header = response.headers.getRawHeaders('Content-Length')

        def data_limit_check(response, length):
            if data_limit is not None and length > data_limit:
                raise HttpDataLimitError(
                    "Received %d bytes, maximum of %d bytes allowed."
                    % (length, data_limit,))
            return response

        if header is None:
            d = response.content()
            d.addCallback(lambda body: data_limit_check(response, len(body)))
            return d

        content_length = header[0]
        return maybeDeferred(data_limit_check, response, int(content_length))

    def _make_success_reply(self, response, command):
        d = response.content()
        d.addCallback(
            lambda body: self.reply(command, success=True, body=body,
                                    code=response.code))
        return d

    def _make_failure_reply(self, failure, command):
        return self.reply(command, success=False,
                          reason=failure.getErrorMessage())

    def handle_get(self, api, command):
        """
        Make an HTTP GET request.

        See :class:`HttpResource` for details.
        """
        return self._make_request_from_command('GET', command)

    def handle_put(self, api, command):
        """
        Make an HTTP PUT request.

        See :class:`HttpResource` for details.
        """
        return self._make_request_from_command('PUT', command)

    def handle_delete(self, api, command):
        """
        Make an HTTP DELETE request.

        See :class:`HttpResource` for details.
        """
        return self._make_request_from_command('DELETE', command)

    def handle_head(self, api, command):
        """
        Make an HTTP HEAD request.

        See :class:`HttpResource` for details.
        """
        return self._make_request_from_command('HEAD', command)

    def handle_post(self, api, command):
        """
        Make an HTTP POST request.

        See :class:`HttpResource` for details.
        """
        return self._make_request_from_command('POST', command)

    def handle_patch(self, api, command):
        """
        Make an HTTP PATCH request.

        See :class:`HttpResource` for details.
        """
        return self._make_request_from_command('PATCH', command)
