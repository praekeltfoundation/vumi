import os.path

from twisted.trial.unittest import TestCase
from twisted.internet import reactor
from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.internet.error import ConnectionDone
from twisted.web.server import Site, NOT_DONE_YET
from twisted.web.resource import Resource
from twisted.web import http


from vumi.utils import (normalize_msisdn, make_vumi_path_abs, cleanup_msisdn,
                        get_operator_name, http_request, http_request_full)


class UtilsTestCase(TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_normalize_msisdn(self):
        self.assertEqual(normalize_msisdn('0761234567', '27'),
                         '+27761234567')
        self.assertEqual(normalize_msisdn('27761234567', '27'),
                         '+27761234567')
        self.assertEqual(normalize_msisdn('+27761234567', '27'),
                         '+27761234567')
        self.assertEqual(normalize_msisdn('0027761234567', '27'),
                         '+27761234567')
        self.assertEqual(normalize_msisdn('1234'), '1234')
        self.assertEqual(normalize_msisdn('12345'), '12345')
        self.assertEqual(normalize_msisdn('+12345'), '+12345')

    def test_make_campaign_path_abs(self):
        vumi_tests_path = os.path.dirname(__file__)
        vumi_path = os.path.dirname(os.path.dirname(vumi_tests_path))
        self.assertEqual('/foo/bar', make_vumi_path_abs('/foo/bar'))
        self.assertEqual(os.path.join(vumi_path, 'foo/bar'),
                         make_vumi_path_abs('foo/bar'))

    def test_cleanup_msisdn(self):
        self.assertEqual('27761234567', cleanup_msisdn('27761234567', '27'))
        self.assertEqual('27761234567', cleanup_msisdn('+27761234567', '27'))
        self.assertEqual('27761234567', cleanup_msisdn('0761234567', '27'))

    def test_get_operator_name(self):
        mapping = {'27': {'2782': 'VODACOM', '2783': 'MTN'}}
        self.assertEqual('MTN', get_operator_name('27831234567', mapping))
        self.assertEqual('VODACOM', get_operator_name('27821234567', mapping))
        self.assertEqual('UNKNOWN', get_operator_name('27801234567', mapping))


class HttpUtilsTestCase(TestCase):

    class LoseConnection(Exception):
        """Indicates that test server should drop client connection"""
        pass

    @inlineCallbacks
    def setUp(self):
        self.root = Resource()
        self.root.isLeaf = True
        site_factory = Site(self.root)
        self.webserver = yield reactor.listenTCP(0, site_factory)
        addr = self.webserver.getHost()
        self.url = "http://%s:%s/" % (addr.host, addr.port)

    @inlineCallbacks
    def tearDown(self):
        yield self.webserver.loseConnection()

    def set_render(self, f):
        def render(request):
            request.setHeader('Content-Type', 'text/plain')
            try:
                data = f()
                request.setResponseCode(http.OK)
            except self.LoseConnection:
                request.transport.loseConnection()
                return NOT_DONE_YET
            except Exception, err:
                data = str(err)
                request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            return data

        self.root.render = render

    @inlineCallbacks
    def test_http_request_ok(self):
        self.set_render(lambda: "Yay")
        data = yield http_request(self.url, '')
        self.assertEqual(data, "Yay")

    @inlineCallbacks
    def test_http_request_err(self):
        def err():
            raise ValueError("Bad")
        self.set_render(err)
        data = yield http_request(self.url, '')
        self.assertEqual(data, "Bad")

    @inlineCallbacks
    def test_http_request_full_drop(self):
        def lose():
            raise self.LoseConnection()
        self.set_render(lose)

        done = Deferred()

        def callback(reason):
            self.assertTrue(
                reason.check("twisted.web._newclient.ResponseFailed"))
            done.callback(None)

        d = http_request_full(self.url, '')
        d.addBoth(callback)

        yield done

    @inlineCallbacks
    def test_http_request_full_ok(self):
        self.set_render(lambda: "Yay")
        request = yield http_request_full(self.url, '')
        self.assertEqual(request.delivered_body, "Yay")
        self.assertEqual(request.code, http.OK)

    @inlineCallbacks
    def test_http_request_full_err(self):
        def err():
            raise ValueError("Bad")
        self.set_render(err)
        request = yield http_request_full(self.url, '')
        self.assertEqual(request.delivered_body, "Bad")
        self.assertEqual(request.code, http.INTERNAL_SERVER_ERROR)
