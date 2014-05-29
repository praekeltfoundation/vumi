import os.path

from twisted.internet import reactor
from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.web.server import Site, NOT_DONE_YET
from twisted.web.resource import Resource
from twisted.web import http
from twisted.web.client import WebClientContextFactory, Agent
from twisted.internet.protocol import Protocol, Factory

from vumi.utils import (normalize_msisdn, vumi_resource_path, cleanup_msisdn,
                        get_operator_name, http_request, http_request_full,
                        get_first_word, redis_from_config, build_web_site,
                        LogFilterSite, PkgResources)
from vumi.persist.fake_redis import FakeRedis
from vumi.tests.helpers import VumiTestCase, import_skip


class DummyRequest(object):
    def __init__(self, postpath, prepath):
        self.postpath = postpath
        self.prepath = prepath


class TestNormalizeMsisdn(VumiTestCase):
    def test_leading_zero(self):
        self.assertEqual(normalize_msisdn('0761234567', '27'),
                         '+27761234567')

    def test_double_leading_zero(self):
        self.assertEqual(normalize_msisdn('0027761234567', '27'),
                         '+27761234567')

    def test_leading_plus(self):
        self.assertEqual(normalize_msisdn('+27761234567', '27'),
                         '+27761234567')

    def test_no_leading_plus_or_zero(self):
        self.assertEqual(normalize_msisdn('27761234567', '27'),
                         '+27761234567')

    def test_short_address(self):
        self.assertEqual(normalize_msisdn('1234'), '1234')
        self.assertEqual(normalize_msisdn('12345'), '12345')

    def test_short_address_with_leading_plus(self):
        self.assertEqual(normalize_msisdn('+12345'), '+12345')

    def test_unicode_addr_remains_unicode(self):
        addr = normalize_msisdn(u'0761234567', '27')
        self.assertEqual(addr, u'+27761234567')
        self.assertTrue(isinstance(addr, unicode))

    def test_str_addr_remains_str(self):
        addr = normalize_msisdn('0761234567', '27')
        self.assertEqual(addr, '+27761234567')
        self.assertTrue(isinstance(addr, str))


class TestUtils(VumiTestCase):
    def test_make_campaign_path_abs(self):
        vumi_tests_path = os.path.dirname(__file__)
        vumi_path = os.path.dirname(os.path.dirname(vumi_tests_path))
        self.assertEqual('/foo/bar', vumi_resource_path('/foo/bar'))
        self.assertEqual(os.path.join(vumi_path, 'vumi/resources/foo/bar'),
                         vumi_resource_path('foo/bar'))

    def test_cleanup_msisdn(self):
        self.assertEqual('27761234567', cleanup_msisdn('27761234567', '27'))
        self.assertEqual('27761234567', cleanup_msisdn('+27761234567', '27'))
        self.assertEqual('27761234567', cleanup_msisdn('0761234567', '27'))

    def test_get_operator_name(self):
        mapping = {'27': {'2782': 'VODACOM', '2783': 'MTN'}}
        self.assertEqual('MTN', get_operator_name('27831234567', mapping))
        self.assertEqual('VODACOM', get_operator_name('27821234567', mapping))
        self.assertEqual('UNKNOWN', get_operator_name('27801234567', mapping))

    def test_get_first_word(self):
        self.assertEqual('KEYWORD',
                         get_first_word('KEYWORD rest of the message'))
        self.assertEqual('', get_first_word(''))
        self.assertEqual('', get_first_word(None))

    def test_redis_from_config_str(self):
        try:
            fake_redis = redis_from_config("FAKE_REDIS")
        except ImportError, e:
            import_skip(e, 'redis')
        self.assertTrue(isinstance(fake_redis, FakeRedis))

    def test_redis_from_config_fake_redis(self):
        fake_redis = FakeRedis()
        try:
            self.assertEqual(redis_from_config(fake_redis), fake_redis)
        except ImportError, e:
            import_skip(e, 'redis')

    def get_resource(self, path, site):
        request = DummyRequest(postpath=path.split('/'), prepath=[])
        return site.getResourceFor(request)

    def test_build_web_site(self):
        resource_a = Resource()
        resource_b = Resource()
        site = build_web_site({
            'foo/a': resource_a,
            'bar/b': resource_b,
        })
        self.assertEqual(self.get_resource('foo/a', site), resource_a)
        self.assertEqual(self.get_resource('bar/b', site), resource_b)
        self.assertTrue(isinstance(site, LogFilterSite))

    def test_build_web_site_with_overlapping_paths(self):
        resource_a = Resource()
        resource_b = Resource()
        site = build_web_site({
            'foo/a': resource_a,
            'foo/b': resource_b,
        })
        self.assertEqual(self.get_resource('foo/a', site), resource_a)
        self.assertEqual(self.get_resource('foo/b', site), resource_b)
        self.assertTrue(isinstance(site, LogFilterSite))

    def test_build_web_site_with_custom_site_class(self):
        site = build_web_site({}, site_class=Site)
        self.assertTrue(isinstance(site, Site))
        self.assertFalse(isinstance(site, LogFilterSite))


class FakeHTTP10(Protocol):
    def dataReceived(self, data):
        self.transport.write(self.factory.response_body)
        self.transport.loseConnection()


class TestHttpUtils(VumiTestCase):

    class InterruptHttp(Exception):
        """Indicates that test server should halt http reply"""
        pass

    @inlineCallbacks
    def setUp(self):
        self.root = Resource()
        self.root.isLeaf = True
        site_factory = Site(self.root)
        self.webserver = yield reactor.listenTCP(
            0, site_factory, interface='127.0.0.1')
        # This is a lambda because we replace self.webserver in a test.
        self.add_cleanup(lambda: self.webserver.loseConnection())
        addr = self.webserver.getHost()
        self.url = "http://%s:%s/" % (addr.host, addr.port)

    def set_render(self, f, d=None):
        def render(request):
            request.setHeader('Content-Type', 'text/plain')
            try:
                data = f(request)
                request.setResponseCode(http.OK)
            except self.InterruptHttp:
                reactor.callLater(0, d.callback, request)
                return NOT_DONE_YET
            except Exception, err:
                data = str(err)
                request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            return data

        self.root.render = render

    @inlineCallbacks
    def test_http_request_ok(self):
        self.set_render(lambda r: "Yay")
        data = yield http_request(self.url, '')
        self.assertEqual(data, "Yay")

    @inlineCallbacks
    def test_http_request_err(self):
        def err(r):
            raise ValueError("Bad")
        self.set_render(err)
        data = yield http_request(self.url, '')
        self.assertEqual(data, "Bad")

    @inlineCallbacks
    def test_http_request_with_custom_context_factory(self):
        self.set_render(lambda r: "Yay")

        ctxt = WebClientContextFactory()

        class FakeAgent(Agent):
            def __init__(slf, reactor, contextFactory=None):
                self.assertEqual(contextFactory, ctxt)
                super(FakeAgent, slf).__init__(reactor, contextFactory)

        request = yield http_request_full(self.url, '',
                                          context_factory=ctxt,
                                          agent_class=FakeAgent)
        self.assertEqual(request.delivered_body, "Yay")
        self.assertEqual(request.code, http.OK)

    @inlineCallbacks
    def test_http_request_full_drop(self):
        def interrupt(r):
            raise self.InterruptHttp()
        got_request = Deferred()
        self.set_render(interrupt, got_request)

        got_data = http_request_full(self.url, '')

        request = yield got_request
        request.setResponseCode(http.OK)
        request.write("Foo!")
        request.transport.loseConnection()

        def callback(reason):
            self.assertTrue(
                reason.check("twisted.web._newclient.ResponseFailed"))
            done.callback(None)
        done = Deferred()

        got_data.addBoth(callback)

        yield done

    @inlineCallbacks
    def test_http_request_full_ok(self):
        self.set_render(lambda r: "Yay")
        request = yield http_request_full(self.url, '')
        self.assertEqual(request.delivered_body, "Yay")
        self.assertEqual(request.code, http.OK)

    @inlineCallbacks
    def test_http_request_full_headers(self):
        def check_ua(request):
            self.assertEqual('blah', request.getHeader('user-agent'))
            return "Yay"
        self.set_render(check_ua)

        request = yield http_request_full(self.url, '',
                                          {'User-Agent': ['blah']})
        self.assertEqual(request.delivered_body, "Yay")
        self.assertEqual(request.code, http.OK)

        request = yield http_request_full(self.url, '', {'User-Agent': 'blah'})
        self.assertEqual(request.delivered_body, "Yay")
        self.assertEqual(request.code, http.OK)

    @inlineCallbacks
    def test_http_request_full_err(self):
        def err(r):
            raise ValueError("Bad")
        self.set_render(err)
        request = yield http_request_full(self.url, '')
        self.assertEqual(request.delivered_body, "Bad")
        self.assertEqual(request.code, http.INTERNAL_SERVER_ERROR)

    @inlineCallbacks
    def test_http_request_potential_data_loss(self):
        self.webserver.loseConnection()
        factory = Factory()
        factory.protocol = FakeHTTP10
        factory.response_body = (
            "HTTP/1.0 201 CREATED\r\n"
            "Date: Mon, 23 Jan 2012 15:08:47 GMT\r\n"
            "Server: Fake HTTP 1.0\r\n"
            "Content-Type: text/html; charset=utf-8\r\n"
            "\r\n"
            "Yay")
        self.webserver = yield reactor.listenTCP(
            0, factory, interface='127.0.0.1')
        addr = self.webserver.getHost()
        self.url = "http://%s:%s/" % (addr.host, addr.port)

        data = yield http_request(self.url, '')
        self.assertEqual(data, "Yay")

    @inlineCallbacks
    def test_http_request_full_data_limit(self):
        self.set_render(lambda r: "Four")

        d = http_request_full(self.url, '', data_limit=3)

        def check_response(reason):
            self.assertTrue(reason.check('vumi.utils.HttpDataLimitError'))
            self.assertEqual(reason.getErrorMessage(),
                             "More than 3 bytes received")

        d.addBoth(check_response)
        yield d

    @inlineCallbacks
    def test_http_request_full_ok_with_timeout_set(self):
        # If we don't cancel the pending timeout check this test will fail with
        # a dirty reactor.
        self.set_render(lambda r: "Yay")
        request = yield http_request_full(self.url, '', timeout=100)
        self.assertEqual(request.delivered_body, "Yay")
        self.assertEqual(request.code, http.OK)

    @inlineCallbacks
    def test_http_request_full_timeout_before_connect(self):
        # This tests the case where the client times out before
        # successfully connecting to the server.

        # don't need to call .set_render because the request
        # will never make it to the server
        d = http_request_full(self.url, '', timeout=0)

        def check_response(reason):
            self.assertTrue(reason.check('vumi.utils.HttpTimeoutError'))

        d.addBoth(check_response)
        yield d

    @inlineCallbacks
    def test_http_request_full_timeout_after_connect(self):
        # This tests the case where the client connects but then
        # times out before the server sends any data.

        def interrupt(r):
            raise self.InterruptHttp
        request_started = Deferred()
        self.set_render(interrupt, request_started)

        client_done = http_request_full(self.url, '', timeout=0.1)

        def check_response(reason):
            self.assertTrue(reason.check('vumi.utils.HttpTimeoutError'))

        client_done.addBoth(check_response)
        yield client_done

        request = yield request_started
        request.transport.loseConnection()

    @inlineCallbacks
    def test_http_request_full_timeout_after_first_receive(self):
        # This tests the case where the client connects, receives
        # some data and creates its receiver but then times out
        # because the server takes too long to finish sending the data.

        def interrupt(r):
            raise self.InterruptHttp
        request_started = Deferred()
        self.set_render(interrupt, request_started)

        client_done = http_request_full(self.url, '', timeout=0.1)

        request = yield request_started
        request.write("some data")

        def check_server_response(reason):
            self.assertTrue(reason.check('twisted.internet.error'
                                         '.ConnectionDone'))

        request_done = request.notifyFinish()
        request_done.addBoth(check_server_response)
        yield request_done

        def check_client_response(reason):
            self.assertTrue(reason.check('vumi.utils.HttpTimeoutError'))
        client_done.addBoth(check_client_response)
        yield client_done


class TestPkgResources(VumiTestCase):

    vumi_tests_path = os.path.dirname(__file__)

    def test_absolute_path(self):
        pkg = PkgResources("vumi.tests")
        self.assertEqual('/foo/bar', pkg.path('/foo/bar'))

    def test_relative_path(self):
        pkg = PkgResources("vumi.tests")
        self.assertEqual(os.path.join(self.vumi_tests_path, 'foo/bar'),
                         pkg.path('foo/bar'))
