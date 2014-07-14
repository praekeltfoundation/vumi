import re
from xml.etree import ElementTree
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks
from vumi.utils import http_request
from vumi.transports.vodacom_messaging import (VodacomMessagingResponse,
    VodacomMessagingTransport)
from vumi.message import TransportUserMessage
from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.helpers import VumiTestCase


class TestVodacomMessagingTransport(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.config = {
            'transport_type': 'ussd',
            'ussd_string_prefix': '*120*666#',
            'web_path': "/foo",
            'web_host': "127.0.0.1",
            'web_port': 0,
            'username': 'testuser',
            'password': 'testpass',
        }
        self.tx_helper = self.add_helper(
            TransportHelper(VodacomMessagingTransport))
        self.transport = yield self.tx_helper.get_transport(self.config)
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
        msg, = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
        self.assertEqual(msg['transport_type'], "ussd")
        self.assertEqual(msg['transport_metadata'], {
            "session_id": "123"
        })
        self.assertEqual(msg['session_event'],
            TransportUserMessage.SESSION_NEW)
        self.assertEqual(msg['from_addr'], '555')
        self.assertEqual(msg['to_addr'], '*120*666#')
        self.assertEqual(msg['content'], '*120*666#')
        self.tx_helper.make_dispatch_reply(msg, "OK")
        response = yield d
        correct_response = '<request>\n\t<headertext>OK</headertext>\n\t' \
                '<options>\n\t\t<option command="1" order="1" ' \
                'callback="http://127.0.0.1/foo" display="False" >' \
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
        msg, = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
        self.assertEqual(msg['transport_type'], "ussd")
        self.assertEqual(msg['transport_metadata'], {"session_id": "123"})
        self.assertEqual(
            msg['session_event'], TransportUserMessage.SESSION_RESUME)
        self.assertEqual(msg['from_addr'], '555')
        self.assertEqual(msg['to_addr'], '')
        self.assertEqual(msg['content'], '1')
        self.tx_helper.make_dispatch_reply(msg, "OK")
        response = yield d
        correct_response = '<request>\n\t<headertext>OK</headertext>\n\t' \
                '<options>\n\t\t<option command="1" order="1" ' \
                'callback="http://127.0.0.1/foo" display="False" >' \
                '</option>\n\t</options>\n</request>'
        self.assertEqual(response, correct_response)

    @inlineCallbacks
    def test_inbound_resume_close(self):
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
        msg, = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
        self.assertEqual(msg['transport_type'], "ussd")
        self.assertEqual(msg['transport_metadata'], {"session_id": "123"})
        self.assertEqual(
            msg['session_event'], TransportUserMessage.SESSION_RESUME)
        self.assertEqual(msg['from_addr'], '555')
        self.assertEqual(msg['to_addr'], '')
        self.assertEqual(msg['content'], '1')
        self.tx_helper.make_dispatch_reply(msg, "OK", continue_session=False)
        response = yield d
        correct_response = '<request>\n\t<headertext>OK' + \
                            '</headertext>\n</request>'
        self.assertEqual(response, correct_response)

    @inlineCallbacks
    def test_nack(self):
        msg = yield self.tx_helper.make_dispatch_outbound("outbound")
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(nack['user_message_id'], msg['message_id'])
        self.assertEqual(nack['sent_message_id'], msg['message_id'])
        self.assertEqual(nack['nack_reason'],
            'Missing fields: in_reply_to')


class VodacomMessagingResponseTest(VumiTestCase):
    '''
    Test the construction of XML replies for Vodacom Messaging
    '''

    def setUp(self):
        self.web_host = 'vumi.p.org'
        self.web_path = '/api/v1/ussd/vmes/'

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
