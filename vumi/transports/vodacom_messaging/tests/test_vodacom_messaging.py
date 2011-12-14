import re
from  xml.etree import ElementTree
from urllib import urlencode

from twisted.trial.unittest import TestCase
from twisted.web.resource import Resource
from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web.server import Site
from twisted.internet import reactor
from twisted.internet.base import DelayedCall
from twisted.python import log
from vumi.transports.tests.test_base import TransportTestCase
from vumi.utils import http_request
from vumi.transports.vodacom_messaging import (VodacomMessagingResponse,
    VodacomMessagingTransport)
from vumi.message import TransportUserMessage
from vumi.tests.utils import get_stubbed_worker


class TestVodacomMessagingTransport(TransportTestCase):

    timeout = 3
    transport_name = 'vodacom_messaging'
    transport_class = VodacomMessagingTransport

    @inlineCallbacks
    def setUp(self):
        yield super(TestVodacomMessagingTransport, self).setUp()
        self.config = {
            'transport_type': 'ussd',
            'ussd_string_prefix': '*120*666#',
            'web_path': "/foo",
            'web_host': "localhost",
            'web_port': 0,
            'username': 'testuser',
            'password': 'testpass',
        }
        self.transport = yield self.get_transport(self.config)
        self.transport_url = self.transport.get_transport_url().rstrip('/')

    @inlineCallbacks
    def test_inbound_new_continue(self):
        url = "%s%s?%s" % (
            self.transport_url,
            self.config['web_path'],
            urlencode({
                'ussdSessionId': 123,
                'msisdn': 555,
                'provider': 'web',
                'request': '*120*666#',
            }))
        d = http_request(url, '', method='GET')
        msg, = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg['transport_name'], self.transport_name)
        self.assertEqual(msg['transport_type'], "ussd")
        self.assertEqual(msg['transport_metadata'], {
            "session_id": "123"
        })
        self.assertEqual(msg['session_event'],
            TransportUserMessage.SESSION_NEW)
        self.assertEqual(msg['from_addr'], '555')
        self.assertEqual(msg['to_addr'], '*120*666#')
        self.assertEqual(msg['content'], '*120*666#')
        tum = TransportUserMessage(**msg.payload)
        reply = tum.reply("OK")
        self.dispatch(reply)
        response = yield d
        correct_response = '<request>\n\t<headertext>OK</headertext>\n\t' \
                '<options>\n\t\t<option command="1" order="1" ' \
                'callback="http://localhost/foo" display="False" >' \
                '</option>\n\t</options>\n</request>'
        self.assertEqual(response, correct_response)

    @inlineCallbacks
    def test_inbound_resume_continue(self):
        url = "%s%s?%s" % (
            self.transport_url,
            self.config['web_path'],
            urlencode({
                'ussdSessionId': 123,
                'msisdn': 555,
                'provider': 'web',
                'request': 1,
            })
        )
        d = http_request(url, '', method='GET')
        msg, = yield self.wait_for_dispatched_messages(1)
        payload = msg.payload
        self.assertEqual(payload['transport_name'], self.transport_name)
        self.assertEqual(payload['transport_type'], "ussd")
        self.assertEqual(payload['transport_metadata'],
                         {"session_id": "123"})
        self.assertEqual(payload['session_event'],
                         TransportUserMessage.SESSION_RESUME)
        self.assertEqual(payload['from_addr'], '555')
        self.assertEqual(payload['to_addr'], '')
        self.assertEqual(payload['content'], '1')
        tum = TransportUserMessage(**payload)
        rep = tum.reply("OK")
        self.dispatch(rep)
        response = yield d
        correct_response = '<request>\n\t<headertext>OK</headertext>\n\t' \
                '<options>\n\t\t<option command="1" order="1" ' \
                'callback="http://localhost/foo" display="False" >' \
                '</option>\n\t</options>\n</request>'
        self.assertEqual(response, correct_response)

    @inlineCallbacks
    def test_inbound_resume_close(self):
        args = "/?ussdSessionId=123&msisdn=555&provider=web&request=1"
        url = "%s%s?%s" % (
            self.transport_url,
            self.config['web_path'],
            urlencode({
                'ussdSessionId': 123,
                'msisdn': 555,
                'provider': 'web',
                'request': 1,
            })
        )
        d = http_request(url, '', method='GET')
        msg, = yield self.wait_for_dispatched_messages(1)
        payload = msg.payload
        self.assertEqual(payload['transport_name'], self.transport_name)
        self.assertEqual(payload['transport_type'], "ussd")
        self.assertEqual(payload['transport_metadata'],
                         {"session_id": "123"})
        self.assertEqual(payload['session_event'],
                         TransportUserMessage.SESSION_RESUME)
        self.assertEqual(payload['from_addr'], '555')
        self.assertEqual(payload['to_addr'], '')
        self.assertEqual(payload['content'], '1')
        tum = TransportUserMessage(**payload)
        rep = tum.reply("OK", False)
        self.dispatch(rep)
        response = yield d
        correct_response = '<request>\n\t<headertext>OK' + \
                            '</headertext>\n</request>'
        self.assertEqual(response, correct_response)


class VodacomMessagingResponseTest(TestCase):
    '''
    Test the construction of XML replies for Vodacom Messaging
    '''

    def setUp(self):
        self.web_host = 'vumi.p.org'
        self.web_path = '/api/v1/ussd/vmes/'

    def tearDown(self):
        pass

    def stdXML(self, obj):
        string = ElementTree.tostring(ElementTree.fromstring(str(obj)))
        return re.sub(r'\n\s*', '', string)

    def testMakeEndMessage(self):
        vmr = VodacomMessagingResponse(self.web_host, self.web_path)
        vmr.set_headertext("Goodbye")
        ref = '''
            <request>
                <headertext>Goodbye</headertext>
            </request>
            '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))

    def testMakeFreetextMessage(self):
        vmr = VodacomMessagingResponse(self.web_host, self.web_path)
        vmr.set_headertext("Please enter your name")
        vmr.accept_freetext()
        ref = '''
            <request>
                <headertext>Please enter your name</headertext>
                <options>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/"
                        command="1"
                        display="False"
                        order="1" />
                </options>
            </request>
            '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))
        ref = '''
        <request>
            <headertext>Please enter your name</headertext>
            <options>
            <option
                callback="http://vumi.p.org/api/v1/ussd/vmes/"
                command="1"
                display="False"
                order="1" />
            </options>
        </request>
        '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))

    def testMakeOptionMessage(self):
        vmr = VodacomMessagingResponse(self.web_host, self.web_path)
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
                        callback="http://vumi.p.org/api/v1/ussd/vmes/"
                        display="True"
                        >Ace of diamonds</option>
                    <option
                        command="2"
                        order="2"
                        callback="http://vumi.p.org/api/v1/ussd/vmes/"
                        display="True"
                        >2 of clubs</option>
                    <option
                        command="3"
                        order="3"
                        callback="http://vumi.p.org/api/v1/ussd/vmes/"
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
                        callback="http://vumi.p.org/api/v1/ussd/vmes/"
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
                        callback="http://vumi.p.org/api/v1/ussd/vmes/"
                        command="1"
                        display="True"
                        order="1"
                        >King of spades</option>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/"
                        command="2"
                        display="True"
                        order="2"
                        >Queen of diamonds</option>
                    <option
                        callback="http://vumi.p.org/api/v1/ussd/vmes/"
                        command="3"
                        display="True"
                        order="3"
                        >Joker</option>
                </options>
            </request>
            '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))

        ref = '''
        <request>
            <headertext>Pick a card</headertext>
            <options>
            <option
                callback="http://vumi.p.org/api/v1/ussd/vmes/"
                command="1"
                display="True"
                order="1"
                >King of spades</option>
            <option
                callback="http://vumi.p.org/api/v1/ussd/vmes/"
                command="2"
                display="True"
                order="2"
                >Queen of diamonds</option>
            <option
                callback="http://vumi.p.org/api/v1/ussd/vmes/"
                command="3"
                display="True"
                order="3"
                >Joker</option>
            </options>
        </request>
        '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))

    def testMakeOrderedOptionMessage(self):
        vmr = VodacomMessagingResponse(self.web_host, self.web_path)
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
                        callback="http://vumi.p.org/api/v1/ussd/vmes/"
                        display="True"
                        >3 of hearts</option>
                    <option
                        command="2"
                        order="2"
                        callback="http://vumi.p.org/api/v1/ussd/vmes/"
                        display="True"
                        >2 of clubs</option>
                    <option
                        command="1"
                        order="1"
                        callback="http://vumi.p.org/api/v1/ussd/vmes/"
                        display="True"
                        >Ace of diamonds</option>
                </options>
            </request>
            '''
        self.assertEquals(self.stdXML(vmr), self.stdXML(ref))
