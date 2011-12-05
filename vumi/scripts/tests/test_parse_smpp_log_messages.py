from twisted.trial.unittest import TestCase
from vumi.scripts.parse_smpp_log_messages import LogParser
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


class ParseSMPPLogMessagesTestCase(TestCase):

    def test_parsing_of_line(self):
        parser = DummyLogParser({
            'from': None,
            'until': None,
        })
        parser.readline("2011-11-15 02:04:48+0000 [EsmeTransceiver,client] "
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

    def test_parse_of_line_with_limits(self):
        sample = resource_string(__name__, 'sample-smpp-output.log')
        parser = DummyLogParser({
            'from': '2011-11-15 00:23:59',
            'until': '2011-11-15 00:24:26'
        })
        for line in sample.split('\n'):
            parser.readline(line)

        self.assertEqual(len(parser.emit_log), 2)
        self.assertEqual(json.loads(parser.emit_log[0].strip())['content'],
                         "CODE2")
        self.assertEqual(json.loads(parser.emit_log[1].strip())['content'],
                         "CODE3")
