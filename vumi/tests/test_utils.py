import os.path

from twisted.internet import reactor
from twisted.internet.defer import Deferred, inlineCallbacks, returnValue
from twisted.internet.error import ConnectionDone
from twisted.internet.protocol import Protocol, Factory
from twisted.internet.task import Clock
from twisted.web.server import Site, NOT_DONE_YET
from twisted.web.resource import Resource
from twisted.web import http
from twisted.web.client import WebClientContextFactory, ResponseFailed

from vumi.utils import (
    normalize_msisdn, vumi_resource_path, cleanup_msisdn, get_operator_name,
    http_request, http_request_full, get_first_word, redis_from_config,
    build_web_site, LogFilterSite, PkgResources, HttpTimeoutError,
    StatusEdgeDetector)
from vumi.message import TransportStatus
from vumi.persist.fake_redis import FakeRedis
from vumi.tests.fake_connection import (
    FakeServer, FakeHttpServer, ProxyAgentWithContext, wait0)
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
    def setUp(self):
        self.fake_http = FakeHttpServer(lambda r: self._render_request(r))
        self.url = "http://example.com:9980/"

    def set_render(self, f):
        def render(request):
            request.setHeader('Content-Type', 'text/plain')
            try:
                data = f(request)
                request.setResponseCode(http.OK)
            except Exception, err:
                data = str(err)
                request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            return data
        self._render_request = render

    def set_async_render(self):
        def render_interrupt(request):
            reactor.callLater(0, d.callback, request)
            return NOT_DONE_YET
        d = Deferred()
        self.set_render(render_interrupt)
        return d

    @inlineCallbacks
    def make_real_webserver(self):
        """
        Construct a real webserver to test actual connectivity.
        """
        root = Resource()
        root.isLeaf = True
        root.render = lambda r: self._render_request(r)
        site_factory = Site(root)
        webserver = yield reactor.listenTCP(
            0, site_factory, interface='127.0.0.1')
        self.add_cleanup(webserver.loseConnection)
        addr = webserver.getHost()
        url = "http://%s:%s/" % (addr.host, addr.port)
        returnValue(url)

    def with_agent(self, f, *args, **kw):
        """
        Wrapper around http_request_full and friends that injects our fake
        connection's agent.
        """
        kw.setdefault('agent_class', self.fake_http.get_agent)
        return f(*args, **kw)

    @inlineCallbacks
    def test_http_request_to_localhost(self):
        """
        Make a request over the network (localhost) to check that we're getting
        a real agent by default.
        """
        url = yield self.make_real_webserver()
        self.set_render(lambda r: "Yay")
        data = yield http_request(url, '')
        self.assertEqual(data, "Yay")

    @inlineCallbacks
    def test_http_request_ok(self):
        self.set_render(lambda r: "Yay")
        data = yield self.with_agent(http_request, self.url, '')
        self.assertEqual(data, "Yay")

    @inlineCallbacks
    def test_http_request_err(self):
        def err(r):
            raise ValueError("Bad")
        self.set_render(err)
        data = yield self.with_agent(http_request, self.url, '')
        self.assertEqual(data, "Bad")

    @inlineCallbacks
    def test_http_request_full_to_localhost(self):
        """
        Make a request over the network (localhost) to check that we're getting
        a real agent by default.
        """
        url = yield self.make_real_webserver()
        self.set_render(lambda r: "Yay")
        request = yield http_request_full(url, '')
        self.assertEqual(request.delivered_body, "Yay")
        self.assertEqual(request.code, http.OK)
        self.set_render(lambda r: "Yay")

    @inlineCallbacks
    def test_http_request_with_custom_context_factory(self):
        self.set_render(lambda r: "Yay")
        agents = []

        ctxt = WebClientContextFactory()

        def stashing_factory(reactor, contextFactory=None):
            agent = self.fake_http.get_agent(
                reactor, contextFactory=contextFactory)
            agents.append(agent)
            return agent

        request = yield http_request_full(
            self.url, '', context_factory=ctxt, agent_class=stashing_factory)
        self.assertEqual(request.delivered_body, "Yay")
        self.assertEqual(request.code, http.OK)
        [agent] = agents
        self.assertEqual(agent.contextFactory, ctxt)

    @inlineCallbacks
    def test_http_request_full_drop(self):
        """
        If a connection drops, we get an appropriate exception.
        """
        got_request = self.set_async_render()
        got_data = self.with_agent(http_request_full, self.url, '')
        request = yield got_request
        request.setResponseCode(http.OK)
        request.write("Foo!")
        request.transport.loseConnection()

        yield self.assertFailure(got_data, ResponseFailed)

    @inlineCallbacks
    def test_http_request_full_ok(self):
        self.set_render(lambda r: "Yay")
        request = yield self.with_agent(http_request_full, self.url, '')
        self.assertEqual(request.delivered_body, "Yay")
        self.assertEqual(request.code, http.OK)

    @inlineCallbacks
    def test_http_request_full_headers(self):
        def check_ua(request):
            self.assertEqual('blah', request.getHeader('user-agent'))
            return "Yay"
        self.set_render(check_ua)

        request = yield self.with_agent(
            http_request_full, self.url, '', {'User-Agent': ['blah']})
        self.assertEqual(request.delivered_body, "Yay")
        self.assertEqual(request.code, http.OK)

        request = yield self.with_agent(
            http_request_full, self.url, '', {'User-Agent': 'blah'})
        self.assertEqual(request.delivered_body, "Yay")
        self.assertEqual(request.code, http.OK)

    @inlineCallbacks
    def test_http_request_full_err(self):
        def err(r):
            raise ValueError("Bad")
        self.set_render(err)
        request = yield self.with_agent(http_request_full, self.url, '')
        self.assertEqual(request.delivered_body, "Bad")
        self.assertEqual(request.code, http.INTERNAL_SERVER_ERROR)

    @inlineCallbacks
    def test_http_request_potential_data_loss(self):
        """
        In the absence of a Content-Length header or chunked transfer encoding,
        we need to swallow a PotentialDataLoss exception.
        """
        # We can't use Twisted's HTTP server, because that always does the
        # sensible thing. We also pretend to be HTTP 1.0 for simplicity.
        factory = Factory()
        factory.protocol = FakeHTTP10
        factory.response_body = (
            "HTTP/1.0 201 CREATED\r\n"
            "Date: Mon, 23 Jan 2012 15:08:47 GMT\r\n"
            "Server: Fake HTTP 1.0\r\n"
            "Content-Type: text/html; charset=utf-8\r\n"
            "\r\n"
            "Yay")
        fake_server = FakeServer(factory)
        agent_factory = lambda *a, **kw: ProxyAgentWithContext(
            fake_server.endpoint, *a, **kw)

        data = yield http_request(self.url, '', agent_class=agent_factory)
        self.assertEqual(data, "Yay")

    @inlineCallbacks
    def test_http_request_full_data_limit(self):
        self.set_render(lambda r: "Four")

        d = self.with_agent(http_request_full, self.url, '', data_limit=3)

        def check_response(reason):
            self.assertTrue(reason.check('vumi.utils.HttpDataLimitError'))
            self.assertEqual(reason.getErrorMessage(),
                             "More than 3 bytes received")

        d.addBoth(check_response)
        yield d

    @inlineCallbacks
    def test_http_request_full_ok_with_timeout_set(self):
        """
        If a request completes within the timeout, everything is happy.
        """
        clock = Clock()
        self.set_render(lambda r: "Yay")
        response = yield self.with_agent(
            http_request_full, self.url, '', timeout=30, reactor=clock)
        self.assertEqual(response.delivered_body, "Yay")
        self.assertEqual(response.code, http.OK)
        # Advance the clock past the timeout limit.
        clock.advance(30)

    @inlineCallbacks
    def test_http_request_full_drop_with_timeout_set(self):
        """
        If a request fails within the timeout, everything is happy(ish).
        """
        clock = Clock()
        d = self.set_async_render()
        got_data = self.with_agent(
            http_request_full, self.url, '', timeout=30, reactor=clock)
        request = yield d
        request.setResponseCode(http.OK)
        request.write("Foo!")
        request.transport.loseConnection()

        yield self.assertFailure(got_data, ResponseFailed)
        # Advance the clock past the timeout limit.
        clock.advance(30)

    def test_http_request_full_timeout_before_connect(self):
        """
        A request can time out before a connection is made.
        """
        clock = Clock()
        # Instead of setting a render function, we tell the server not to
        # accept connections.
        self.fake_http.fake_server.auto_accept = False
        d = self.with_agent(
            http_request_full, self.url, '', timeout=30, reactor=clock)
        self.assertNoResult(d)
        clock.advance(29)
        self.assertNoResult(d)
        clock.advance(1)
        self.failureResultOf(d, HttpTimeoutError)

    @inlineCallbacks
    def test_http_request_full_timeout_after_connect(self):
        """
        The client disconnects after the timeout if no data has been received
        from the server.
        """
        clock = Clock()
        request_started = self.set_async_render()
        client_done = self.with_agent(
            http_request_full, self.url, '', timeout=30, reactor=clock)
        yield request_started

        self.assertNoResult(client_done)
        clock.advance(29)
        self.assertNoResult(client_done)
        clock.advance(1)
        failure = self.failureResultOf(client_done, HttpTimeoutError)
        self.assertEqual(
            failure.getErrorMessage(), "Timeout while connecting")

    @inlineCallbacks
    def test_http_request_full_timeout_after_first_receive(self):
        """
        The client disconnects after the timeout even if some data has already
        been received.
        """
        clock = Clock()
        request_started = self.set_async_render()
        client_done = self.with_agent(
            http_request_full, self.url, '', timeout=30, reactor=clock)
        request = yield request_started
        request_done = request.notifyFinish()

        request.write("some data")
        clock.advance(1)
        yield wait0()
        self.assertNoResult(client_done)
        self.assertNoResult(request_done)

        clock.advance(28)
        self.assertNoResult(client_done)
        self.assertNoResult(request_done)
        clock.advance(1)
        failure = self.failureResultOf(client_done, HttpTimeoutError)
        self.assertEqual(
            failure.getErrorMessage(), "Timeout while receiving data")
        yield self.assertFailure(request_done, ConnectionDone)


class TestPkgResources(VumiTestCase):

    vumi_tests_path = os.path.dirname(__file__)

    def test_absolute_path(self):
        pkg = PkgResources("vumi.tests")
        self.assertEqual('/foo/bar', pkg.path('/foo/bar'))

    def test_relative_path(self):
        pkg = PkgResources("vumi.tests")
        self.assertEqual(os.path.join(self.vumi_tests_path, 'foo/bar'),
                         pkg.path('foo/bar'))


class TestStatusEdgeDetector(VumiTestCase):
    def test_status_not_change(self):
        '''If the status doesn't change, None should be returned.'''
        sed = StatusEdgeDetector()
        status1 = {
            'component': 'foo',
            'status': 'ok',
            'type': 'bar',
            'message': 'test'}
        self.assertEqual(sed.check_status(**status1), status1)

        status2 = {
            'component': 'foo',
            'status': 'ok',
            'type': 'bar',
            'message': 'another test'}
        self.assertEqual(sed.check_status(**status2), None)

    def test_status_change(self):
        '''If the status does change, the status should be returned.'''
        sed = StatusEdgeDetector()
        status1 = {
            'component': 'foo',
            'status': 'ok',
            'type': 'bar',
            'message': 'test'}
        self.assertEqual(sed.check_status(**status1), status1)

        status2 = {
            'component': 'foo',
            'status': 'degraded',
            'type': 'bar',
            'message': 'another test'}
        self.assertEqual(sed.check_status(**status2), status2)

    def test_components_separate(self):
        '''A state change in one component should not affect other
        components.'''
        sed = StatusEdgeDetector()
        comp1_status1 = {
            'component': 'foo',
            'status': 'ok',
            'type': 'bar',
            'message': 'test'}
        self.assertEqual(sed.check_status(**comp1_status1), comp1_status1)

        comp2_status1 = {
            'component': 'bar',
            'status': 'ok',
            'type': 'bar',
            'message': 'another test'}
        self.assertEqual(sed.check_status(**comp2_status1), comp2_status1)

        comp2_status2 = {
            'component': 'bar',
            'status': 'degraded',
            'type': 'bar',
            'message': 'another test'}
        self.assertEqual(sed.check_status(**comp2_status2), comp2_status2)

        comp1_status2 = {
            'component': 'foo',
            'status': 'ok',
            'type': 'bar',
            'message': 'test'}
        self.assertEqual(sed.check_status(**comp1_status2), None)

    def test_type_change(self):
        '''A change in status type should result in the status being
        returned.'''
        sed = StatusEdgeDetector()
        status1 = {
            'component': 'foo',
            'status': 'ok',
            'type': 'bar',
            'message': 'test'}
        self.assertEqual(sed.check_status(**status1), status1)

        status2 = {
            'component': 'foo',
            'status': 'ok',
            'type': 'baz',
            'message': 'test'}
        self.assertEqual(sed.check_status(**status2), status2)
