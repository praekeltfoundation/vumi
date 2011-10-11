import re
from  xml.etree import ElementTree

from twisted.trial.unittest import TestCase
from twisted.web.resource import Resource
from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web.server import Site
from twisted.internet import reactor
from twisted.internet.base import DelayedCall

from vumi.utils import http_request
from vumi.transports.httprpc.vodacom_messaging import VodacomMessagingResponse
from vumi.transports.httprpc.vodacom_messaging import VodaMessHttpRpcTransport
from vumi.tests.utils import get_stubbed_worker


class MockResource(Resource):
    isLeaf = True

    def __init__(self, handler):
        Resource.__init__(self)
        self.handler = handler

    def render_GET(self, request):
        return self.handler(request)

    def render_POST(self, request):
        return self.handler(request)


class MockHttpServer(object):

    def __init__(self, handler):
        self._handler = handler
        self._webserver = None
        self.addr = None
        self.url = None

    @inlineCallbacks
    def start(self):
        root = MockResource(self._handler)
        site_factory = Site(root)
        self._webserver = yield reactor.listenTCP(0, site_factory)
        self.addr = self._webserver.getHost()
        self.url = "http://%s:%s/" % (self.addr.host, self.addr.port)

    @inlineCallbacks
    def stop(self):
        yield self._webserver.loseConnection()


class TestVodaMessHttpRpcTransport(TestCase):

    @inlineCallbacks
    def setUp(self):
        #DelayedCall.debug = True
        self.vodacom_messaging_calls = DeferredQueue()
        self.mock_vodacom_messaging = MockHttpServer(self.handle_request)
        yield self.mock_vodacom_messaging.start()
        config = {
            'transport_name': 'test_vodacom_messaging',
            'web_path': "foo",
            'web_port': 0,
            'url': self.mock_vodacom_messaging.url,
            'username': 'testuser',
            'password': 'testpass',
            }
        self.worker = get_stubbed_worker(VodaMessHttpRpcTransport, config)
        self.broker = self.worker._amqp_client.broker
        yield self.worker.startWorker()
        addr = self.worker.web_resource.getHost()
        self.worker_url = "http://%s:%s/" % (addr.host, addr.port)

    @inlineCallbacks
    def tearDown(self):
        yield self.worker.stopWorker()
        yield self.mock_vodacom_messaging.stop()

    def handle_request(self, request):
        self.vodacom_messaging_calls.put(request)
        return ''

    @inlineCallbacks
    def test_health(self):
        result = yield http_request(self.worker_url + "health", "",
                                    method='GET')
        self.assertEqual(result, "pReq:0")

    #@inlineCallbacks
    #def test_inbound(self):
        #xml = "XXXXXXXXXXXXXXXXXXX"
        #yield http_request(self.worker_url + "foo", xml, method='GET')
        #msg, = yield self.broker.wait_messages("vumi", "testgrat.inbound", 1)
        #payload = msg.payload
        #self.assertEqual(payload['transport_name'], "testgrat")
        #self.assertEqual(payload['transport_type'], "ussd")
        #self.assertEqual(payload['transport_metadata'],
                         #{"session_id": "sess1234"})
        #self.assertEqual(payload['session_event'],
                         #TransportUserMessage.SESSION_RESUME)
        #self.assertEqual(payload['from_addr'], '27345')
        #self.assertEqual(payload['to_addr'], '*120*99#')
        #self.assertEqual(payload['content'], 'foobar')


class VodacomMessagingResponseTest(TestCase):
    '''
    Test the construction of XML replies for Vodacom Messaging
    '''

    def setUp(self):
        self.config = {
                'web_host': 'vumi.p.org',
                'web_path': '/api/v1/ussd/vmes/'}

    def tearDown(self):
        pass

    def stdXML(self, obj):
        string = ElementTree.tostring(ElementTree.fromstring(str(obj)))
        return re.sub(r'\n\s*', '', string)

    def testMakeEndMessage(self):
        vmr = VodacomMessagingResponse(self.config)
        vmr.set_headertext("Goodbye")
        ref = '''
            <request>
                <headertext>Goodbye</headertext>
            </request>
            '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))

    def testMakeFreetextMessage(self):
        vmr = VodacomMessagingResponse(self.config)
        vmr.set_headertext("Please enter your name")
        vmr.accept_freetext()
        ref = '''
            <request>
                <headertext>Please enter your name</headertext>
                <options>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context="
                        command="1"
                        display="False"
                        order="1" />
                </options>
            </request>
            '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))
        vmr.set_context('username')
        ref = '''
            <request>
                <headertext>Please enter your name</headertext>
                <options>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context=username"
                        command="1"
                        display="False"
                        order="1" />
                </options>
            </request>
            '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))

    def testMakeOptionMessage(self):
        vmr = VodacomMessagingResponse(self.config)
        vmr.set_headertext("Pick a card")
        vmr.accept_freetext()
        vmr.add_option("Ace of diamonds")
        vmr.add_option("2 of clubs")
        vmr.add_option("3 of hearts")
        ref = '''
            <request>
                <headertext>Pick a card</headertext>
                <options>
                    <option
                        command="1"
                        order="1"
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context="
                        display="True"
                        >Ace of diamonds</option>
                    <option
                        command="2"
                        order="2"
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context="
                        display="True"
                        >2 of clubs</option>
                    <option
                        command="3"
                        order="3"
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context="
                        display="True"
                        >3 of hearts</option>
                </options>
            </request>
            '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))

        vmr.accept_freetext()
        ref = '''
            <request>
                <headertext>Pick a card</headertext>
                <options>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context="
                        command="1"
                        display="False"
                        order="1" />
                </options>
            </request>
            '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))

        vmr.add_option("King of spades")
        vmr.add_option("Queen of diamonds")
        vmr.add_option("Joker")
        ref = '''
            <request>
                <headertext>Pick a card</headertext>
                <options>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context="
                        command="1"
                        display="True"
                        order="1"
                        >King of spades</option>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context="
                        command="2"
                        display="True"
                        order="2"
                        >Queen of diamonds</option>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context="
                        command="3"
                        display="True"
                        order="3"
                        >Joker</option>
                </options>
            </request>
            '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))

        vmr.set_context('pickcard')
        ref = '''
            <request>
                <headertext>Pick a card</headertext>
                <options>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context=pickcard"
                        command="1"
                        display="True"
                        order="1"
                        >King of spades</option>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context=pickcard"
                        command="2"
                        display="True"
                        order="2"
                        >Queen of diamonds</option>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context=pickcard"
                        command="3"
                        display="True"
                        order="3"
                        >Joker</option>
                </options>
            </request>
            '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))

    def testMakeOrderedOptionMessage(self):
        vmr = VodacomMessagingResponse(self.config)
        vmr.set_headertext("Pick a card")
        vmr.accept_freetext()
        vmr.add_option("3 of hearts", 3)
        vmr.add_option("2 of clubs", 2)
        vmr.add_option("Ace of diamonds", 1)
        ref = '''
            <request>
                <headertext>Pick a card</headertext>
                <options>
                    <option
                        command="3"
                        order="3"
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context="
                        display="True"
                        >3 of hearts</option>
                    <option
                        command="2"
                        order="2"
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context="
                        display="True"
                        >2 of clubs</option>
                    <option
                        command="1"
                        order="1"
                        callback="http://vumi.p.org/api/v1/ussd/vmes/?context="
                        display="True"
                        >Ace of diamonds</option>
                </options>
            </request>
            '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))
