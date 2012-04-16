from twisted.trial.unittest import TestCase
from vumi.scripts.parse_log_messages import LogParser
from pkg_resources import resource_string
import json


class DummyLogParser(LogParser):
    def __init__(self, *args, **kwargs):
        super(DummyLogParser, self).__init__(*args, **kwargs)
        self.emit_log = []

    def parse(self):
        pass

    def emit(self, obj):
        self.emit_log.append(obj)


SAMPLE_LINE = (
    "2012-04-12 10:52:23+0000 [WorkerAMQClient,client] "
    "Inbound: <Message payload=\"{u'transport_name': u'transport_name',"
    " 'network_operator': 'MNO', u'transport_metadata': {}, u'group':"
    " None, u'from_addr': u'+27123456780', u'timestamp':"
    " datetime.datetime(2012, 4, 12, 10, 52, 23, 329989),"
    " u'to_addr': u'*120*12345*489665#', u'content': u'hello world',"
    " u'message_version': u'20110921', u'transport_type': u'ussd',"
    " u'helper_metadata': {}, u'in_reply_to': None, u'session_event':"
    " u'new', u'message_id': u'b1893fa98ff4485299e3781f73ebfbb6',"
    " u'message_type': u'user_message'}\">")


class ParseSMPPLogMessagesTestCase(TestCase):

    def test_parsing_of_line(self):
        parser = DummyLogParser({
            'from': None,
            'until': None,
            'format': 'vumi',
        })
        parser.readline(SAMPLE_LINE)

        parsed = json.loads(parser.emit_log[0])
        expected = {
            "content": "hell0 world",
            "transport_type": "ussd",
            "to_addr": "*120*12345*489665#",
            "message_id": "b1893fa98ff4485299e3781f73ebfbb6",
            "from_addr": "+27123456780"
        }
        for key in expected.keys():
            self.assertEqual(parsed.get('key'), expected.get('key'))

    def test_parsing_of_smpp_line(self):
        parser = DummyLogParser({
            'from': None,
            'until': None,
            'format': 'smpp',
        })
        parser.readline(
            "2011-11-15 02:04:48+0000 [EsmeTransceiver,client] "
            "PUBLISHING INBOUND: {'content': u'AFN9WH79', 'transport_type': "
            "'sms', 'to_addr': '1458', 'message_id': 'ec443820-62a8-4051-92e7"
            "-66adaa487d20', 'from_addr': '23xxxxxxxx'}")

        self.assertEqual(json.loads(parser.emit_log[0]), {
            "content": "AFN9WH79",
            "transport_type": "sms",
            "to_addr": "1458",
            "message_id": "ec443820-62a8-4051-92e7-66adaa487d20",
            "from_addr": "23xxxxxxxx"
        })

    def test_parse_of_smpp_lines_with_limits(self):
        sample = resource_string(__name__, 'sample-smpp-output.log')
        parser = DummyLogParser({
            'from': '2011-11-15 00:23:59',
            'until': '2011-11-15 00:24:26',
            'format': 'smpp',
            })
        for line in sample.split('\n'):
            parser.readline(line)

        self.assertEqual(len(parser.emit_log), 2)
        self.assertEqual(json.loads(parser.emit_log[0].strip())['content'],
                         "CODE2")
        self.assertEqual(json.loads(parser.emit_log[1].strip())['content'],
                         "CODE3")
