from twisted.trial.unittest import TestCase
from vumi.scripts.parse_smpp_log_messages import LogParser

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
        self.assertEqual(parser.emit_log, ["{'content': u'AFN9WH79', "\
            "'transport_type': 'sms', 'to_addr': '1458', 'message_id': "\
            "'ec443820-62a8-4051-92e7-66adaa487d20', 'from_addr': "\
            "'23xxxxxxxx'}"])

    def test_parse_of_line_with_limits(self):
        sample = """
2011-11-15 00:23:52+0000 [EsmeTransceiver,client] PUBLISHING INBOUND: {'content': u'CODE1', 'transport_type': 'sms', 'to_addr': '1458', 'message_id': '8b400347-408a-47b5-9663-ff719881d73d', 'from_addr': '23xxxxxxxx'}
2011-11-15 00:23:59+0000 [EsmeTransceiver,client] PUBLISHING INBOUND: {'content': u'CODE2', 'transport_type': 'sms', 'to_addr': '1458', 'message_id': 'c7590d37-6156-4e1e-8ad8-f11f97cc1a5b', 'from_addr': '24xxxxxxxx'}
2011-11-15 00:24:26+0000 [EsmeTransceiver,client] PUBLISHING INBOUND: {'content': u'CODE3', 'transport_type': 'sms', 'to_addr': '1458', 'message_id': 'b70b6698-ee6c-463e-be24-a86246e88a7a', 'from_addr': '25xxxxxxxx'}
2011-11-15 00:25:26+0000 [EsmeTransceiver,client] PUBLISHING INBOUND: {'content': u'CODE4', 'transport_type': 'sms', 'to_addr': '1458', 'message_id': 'f499a00d-83c2-4baf-8af3-cab27fb04394', 'from_addr': '26xxxxxxxx'}
    """
        parser = DummyLogParser({
            'from': '2011-11-15 00:23:59',
            'until': '2011-11-15 00:24:26'
        })
        for line in sample.split('\n'):
            parser.readline(line)
        self.assertEqual(len(parser.emit_log), 2)
        self.assertEqual(parser.emit_log, [
            "{'content': u'CODE2', 'transport_type': 'sms', 'to_addr': '1458', 'message_id': 'c7590d37-6156-4e1e-8ad8-f11f97cc1a5b', 'from_addr': '24xxxxxxxx'}",
            "{'content': u'CODE3', 'transport_type': 'sms', 'to_addr': '1458', 'message_id': 'b70b6698-ee6c-463e-be24-a86246e88a7a', 'from_addr': '25xxxxxxxx'}",
        ])

