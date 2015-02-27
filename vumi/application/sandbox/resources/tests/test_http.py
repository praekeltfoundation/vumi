import base64
import json

from OpenSSL.SSL import (
    VERIFY_PEER, VERIFY_FAIL_IF_NO_PEER_CERT, VERIFY_NONE,
    SSLv3_METHOD, SSLv23_METHOD, TLSv1_METHOD)

from twisted.web.http_headers import Headers
from twisted.internet.defer import inlineCallbacks, fail, succeed

from vumi.application.sandbox.resources.http import (
    HttpClientContextFactory, HttpClientPolicyForHTTPS, make_context_factory,
    HttpClientResource)
from vumi.application.sandbox.resources.tests.utils import (
    ResourceTestCaseBase)


class DummyResponse(object):

    def __init__(self):
        self.headers = Headers({})


class DummyHTTPClient(object):

    def __init__(self):
        self._next_http_request_result = None
        self.http_requests = []

    def set_agent(self, agent):
        self.agent = agent

    def get_context_factory(self):
        # We need to dig around inside our Agent to find the context factory.
        # Since this involves private attributes that have changed a few times
        # recently, we need to try various options.
        if hasattr(self.agent, "_contextFactory"):
            # For Twisted 13.x
            return self.agent._contextFactory
        elif hasattr(self.agent, "_policyForHTTPS"):
            # For Twisted 14.x
            return self.agent._policyForHTTPS
        elif hasattr(self.agent, "_endpointFactory"):
            # For Twisted 15.0.0 (and possibly newer)
            return self.agent._endpointFactory._policyForHTTPS
        else:
            raise NotImplementedError(
                "I can't find the context factory on this Agent. This seems"
                " to change every few versions of Twisted.")

    def fail_next(self, error):
        self._next_http_request_result = fail(error)

    def succeed_next(self, body, code=200, headers={}):

        default_headers = {
            'Content-Length': len(body),
        }
        default_headers.update(headers)

        response = DummyResponse()
        response.code = code
        for header, value in default_headers.items():
            response.headers.addRawHeader(header, value)
        response.content = lambda: succeed(body)
        self._next_http_request_result = succeed(response)

    def request(self, *args, **kw):
        self.http_requests.append((args, kw))
        return self._next_http_request_result


class TestHttpClientResource(ResourceTestCaseBase):

    resource_cls = HttpClientResource

    @inlineCallbacks
    def setUp(self):
        super(TestHttpClientResource, self).setUp()
        yield self.create_resource({})

        self.dummy_client = DummyHTTPClient()

        self.patch(self.resource_cls,
                   'http_client_class', self.get_dummy_client)

    def get_dummy_client(self, agent):
        self.dummy_client.set_agent(agent)
        return self.dummy_client

    def http_request_fail(self, error):
        self.dummy_client.fail_next(error)

    def http_request_succeed(self, body, code=200, headers={}):
        self.dummy_client.succeed_next(body, code, headers)

    def assert_not_unicode(self, arg):
        self.assertFalse(isinstance(arg, unicode))

    def get_context_factory(self):
        return self.dummy_client.get_context_factory()

    def get_context(self, context_factory=None):
        if context_factory is None:
            context_factory = self.get_context_factory()
        if hasattr(context_factory, 'creatorForNetloc'):
            # This context_factory is a new-style IPolicyForHTTPS
            # implementation, so we need to get a context from through its
            # client connection creator. The creator could either be a wrapper
            # around a ClientContextFactory (in which case we treat it like
            # one) or a ClientTLSOptions object (which means we have to grab
            # the context from a private attribute).
            creator = context_factory.creatorForNetloc('example.com', 80)
            if hasattr(creator, 'getContext'):
                return creator.getContext()
            else:
                return creator._ctx
        else:
            # This context_factory is an old-style WebClientContextFactory and
            # will build us a context object if we ask nicely.
            return context_factory.getContext('example.com', 80)

    def assert_http_request(self, url, method='GET', headers=None, data=None,
                            timeout=None, files=None):
        timeout = (timeout if timeout is not None
                   else self.resource.timeout)
        args = (method, url,)
        kw = dict(headers=headers, data=data,
                  timeout=timeout, files=files)
        [(actual_args, actual_kw)] = self.dummy_client.http_requests

        # NOTE: Files are handed over to treq as file pointer-ish things
        #       which in our case are `StringIO` instances.
        actual_kw_files = actual_kw.get('files')
        if actual_kw_files is not None:
            actual_kw_files = actual_kw.pop('files', None)
            kw_files = kw.pop('files', {})
            for name, file_data in actual_kw_files.items():
                kw_file_data = kw_files[name]
                file_name, content_type, sio = file_data
                self.assertEqual(
                    (file_name, content_type, sio.getvalue()),
                    kw_file_data)

        self.assertEqual((actual_args, actual_kw), (args, kw))

        self.assert_not_unicode(actual_args[0])
        self.assert_not_unicode(actual_kw.get('data'))
        headers = actual_kw.get('headers')
        if headers is not None:
            for key, values in headers.items():
                self.assert_not_unicode(key)
                for value in values:
                    self.assert_not_unicode(value)

    def test_make_context_factory_no_method_verify_none(self):
        context_factory = make_context_factory(verify_options=VERIFY_NONE)
        self.assertIsInstance(context_factory, HttpClientContextFactory)
        self.assertEqual(context_factory.verify_options, VERIFY_NONE)
        self.assertEqual(context_factory.ssl_method, None)
        self.assertEqual(
            self.get_context(context_factory).get_verify_mode(), VERIFY_NONE)

    def test_make_context_factory_sslv3_verify_none(self):
        context_factory = make_context_factory(
            verify_options=VERIFY_NONE, ssl_method=SSLv3_METHOD)
        self.assertIsInstance(context_factory, HttpClientContextFactory)
        self.assertEqual(context_factory.verify_options, VERIFY_NONE)
        self.assertEqual(context_factory.ssl_method, SSLv3_METHOD)
        self.assertEqual(
            self.get_context(context_factory).get_verify_mode(), VERIFY_NONE)

    def test_make_context_factory_no_method_verify_peer(self):
        # This test's behaviour depends on the version of Twisted being used.
        context_factory = make_context_factory(verify_options=VERIFY_PEER)
        context = self.get_context(context_factory)
        self.assertEqual(context_factory.ssl_method, None)
        self.assertNotEqual(context.get_verify_mode(), VERIFY_NONE)
        if HttpClientPolicyForHTTPS is None:
            # We have Twisted<14.0.0
            self.assertIsInstance(context_factory, HttpClientContextFactory)
            self.assertEqual(context_factory.verify_options, VERIFY_PEER)
            self.assertEqual(context.get_verify_mode(), VERIFY_PEER)
        else:
            self.assertIsInstance(context_factory, HttpClientPolicyForHTTPS)

    def test_make_context_factory_no_method_verify_peer_or_fail(self):
        # This test's behaviour depends on the version of Twisted being used.
        context_factory = make_context_factory(
            verify_options=(VERIFY_PEER | VERIFY_FAIL_IF_NO_PEER_CERT))
        context = self.get_context(context_factory)
        self.assertEqual(context_factory.ssl_method, None)
        self.assertNotEqual(context.get_verify_mode(), VERIFY_NONE)
        if HttpClientPolicyForHTTPS is None:
            # We have Twisted<14.0.0
            self.assertIsInstance(context_factory, HttpClientContextFactory)
            self.assertEqual(
                context_factory.verify_options,
                VERIFY_PEER | VERIFY_FAIL_IF_NO_PEER_CERT)
            self.assertEqual(
                context.get_verify_mode(),
                VERIFY_PEER | VERIFY_FAIL_IF_NO_PEER_CERT)
        else:
            self.assertIsInstance(context_factory, HttpClientPolicyForHTTPS)

    def test_make_context_factory_no_method_no_verify(self):
        # This test's behaviour depends on the version of Twisted being used.
        context_factory = make_context_factory()
        self.assertEqual(context_factory.ssl_method, None)
        if HttpClientPolicyForHTTPS is None:
            # We have Twisted<14.0.0
            self.assertIsInstance(context_factory, HttpClientContextFactory)
            self.assertEqual(context_factory.verify_options, None)
        else:
            self.assertIsInstance(context_factory, HttpClientPolicyForHTTPS)

    def test_make_context_factory_sslv3_no_verify(self):
        # This test's behaviour depends on the version of Twisted being used.
        context_factory = make_context_factory(ssl_method=SSLv3_METHOD)
        self.assertEqual(context_factory.ssl_method, SSLv3_METHOD)
        if HttpClientPolicyForHTTPS is None:
            # We have Twisted<14.0.0
            self.assertIsInstance(context_factory, HttpClientContextFactory)
            self.assertEqual(context_factory.verify_options, None)
        else:
            self.assertIsInstance(context_factory, HttpClientPolicyForHTTPS)

    @inlineCallbacks
    def test_handle_get(self):
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command('get',
                                            url='http://www.example.com')
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('http://www.example.com', method='GET')

    @inlineCallbacks
    def test_handle_post(self):
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command('post',
                                            url='http://www.example.com')
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('http://www.example.com', method='POST')

    @inlineCallbacks
    def test_handle_patch(self):
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command('patch',
                                            url='http://www.example.com')
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('http://www.example.com', method='PATCH')

    @inlineCallbacks
    def test_handle_head(self):
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command('head',
                                            url='http://www.example.com')
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('http://www.example.com', method='HEAD')

    @inlineCallbacks
    def test_handle_delete(self):
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command('delete',
                                            url='http://www.example.com')
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('http://www.example.com', method='DELETE')

    @inlineCallbacks
    def test_handle_put(self):
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command('put',
                                            url='http://www.example.com')
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('http://www.example.com', method='PUT')

    @inlineCallbacks
    def test_failed_get(self):
        self.http_request_fail(ValueError("HTTP request failed"))
        reply = yield self.dispatch_command('get',
                                            url='http://www.example.com')
        self.assertFalse(reply['success'])
        self.assertEqual(reply['reason'], "HTTP request failed")
        self.assert_http_request('http://www.example.com', method='GET')

    @inlineCallbacks
    def test_null_url(self):
        reply = yield self.dispatch_command('get')
        self.assertFalse(reply['success'])
        self.assertEqual(reply['reason'], "No URL given")

    @inlineCallbacks
    def test_https_request(self):
        # This test's behaviour depends on the version of Twisted being used.
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command('get',
                                            url='https://www.example.com')
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('https://www.example.com', method='GET')

        context_factory = self.get_context_factory()
        self.assertEqual(context_factory.ssl_method, None)
        if HttpClientPolicyForHTTPS is None:
            self.assertIsInstance(context_factory, HttpClientContextFactory)
            self.assertEqual(context_factory.verify_options, None)
        else:
            self.assertIsInstance(context_factory, HttpClientPolicyForHTTPS)

    @inlineCallbacks
    def test_https_request_verify_none(self):
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command(
            'get', url='https://www.example.com',
            verify_options=['VERIFY_NONE'])
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('https://www.example.com', method='GET')

        context = self.get_context()
        self.assertEqual(context.get_verify_mode(), VERIFY_NONE)

    @inlineCallbacks
    def test_https_request_verify_peer_or_fail(self):
        # This test's behaviour depends on the version of Twisted being used.
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command(
            'get', url='https://www.example.com',
            verify_options=['VERIFY_PEER', 'VERIFY_FAIL_IF_NO_PEER_CERT'])
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('https://www.example.com', method='GET')

        context = self.get_context()
        # We don't control verify mode in newer Twisted.
        self.assertNotEqual(context.get_verify_mode(), VERIFY_NONE)
        if HttpClientPolicyForHTTPS is None:
            self.assertEqual(
                context.get_verify_mode(),
                VERIFY_PEER | VERIFY_FAIL_IF_NO_PEER_CERT)

    @inlineCallbacks
    def test_handle_post_files(self):
        self.http_request_succeed('')
        reply = yield self.dispatch_command(
            'post', url='https://www.example.com', files={
                'foo': {
                    'file_name': 'foo.json',
                    'content_type': 'application/json',
                    'data': base64.b64encode(json.dumps({'foo': 'bar'})),
                }
            })

        self.assertTrue(reply['success'])
        self.assert_http_request(
            'https://www.example.com', method='POST', files={
                'foo': ('foo.json', 'application/json',
                        json.dumps({'foo': 'bar'})),
            })

    @inlineCallbacks
    def test_data_limit_exceeded_using_header(self):
        self.http_request_succeed('', headers={
            'Content-Length': self.resource.DEFAULT_DATA_LIMIT + 1,
        })
        reply = yield self.dispatch_command(
            'get', url='https://www.example.com',)
        self.assertFalse(reply['success'])
        self.assertEqual(
            reply['reason'],
            'Received %d bytes, maximum of %s bytes allowed.' % (
                self.resource.DEFAULT_DATA_LIMIT + 1,
                self.resource.DEFAULT_DATA_LIMIT,))

    @inlineCallbacks
    def test_data_limit_exceeded_inferred_from_body(self):
        self.http_request_succeed('1' * (self.resource.DEFAULT_DATA_LIMIT + 1))
        reply = yield self.dispatch_command(
            'get', url='https://www.example.com',)
        self.assertFalse(reply['success'])
        self.assertEqual(
            reply['reason'],
            'Received %d bytes, maximum of %s bytes allowed.' % (
                self.resource.DEFAULT_DATA_LIMIT + 1,
                self.resource.DEFAULT_DATA_LIMIT,))

    @inlineCallbacks
    def test_https_request_method_default(self):
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command(
            'get', url='https://www.example.com')
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('https://www.example.com', method='GET')

        context_factory = self.get_context_factory()
        self.assertEqual(context_factory.ssl_method, None)

    @inlineCallbacks
    def test_https_request_method_SSLv3(self):
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command(
            'get', url='https://www.example.com', ssl_method='SSLv3')
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('https://www.example.com', method='GET')

        context_factory = self.get_context_factory()
        self.assertEqual(context_factory.ssl_method, SSLv3_METHOD)

    @inlineCallbacks
    def test_https_request_method_SSLv23(self):
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command(
            'get', url='https://www.example.com', ssl_method='SSLv23')
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('https://www.example.com', method='GET')

        context_factory = self.get_context_factory()
        self.assertEqual(context_factory.ssl_method, SSLv23_METHOD)

    @inlineCallbacks
    def test_https_request_method_TLSv1(self):
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command(
            'get', url='https://www.example.com', ssl_method='TLSv1')
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('https://www.example.com', method='GET')

        context_factory = self.get_context_factory()
        self.assertEqual(context_factory.ssl_method, TLSv1_METHOD)
