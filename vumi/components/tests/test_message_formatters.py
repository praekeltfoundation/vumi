# -*- coding: utf-8 -*-

from twisted.web.test.test_web import DummyRequest

from vumi.components.message_formatters import (
    IMessageFormatter, JsonFormatter, CsvFormatter, CsvEventFormatter)

from vumi.tests.helpers import VumiTestCase, MessageHelper


class TestJsonFormatter(VumiTestCase):
    def setUp(self):
        self.msg_helper = self.add_helper(MessageHelper())
        self.request = DummyRequest([''])
        self.formatter = JsonFormatter()

    def test_implements_IMessageFormatter(self):
        self.assertTrue(IMessageFormatter.providedBy(self.formatter))

    def test_add_http_headers(self):
        self.formatter.add_http_headers(self.request)
        self.assertEqual(
            self.request.responseHeaders.getRawHeaders('Content-Type'),
            ['application/json; charset=utf-8'])

    def test_write_row_header(self):
        self.formatter.write_row_header(self.request)
        self.assertEqual(self.request.written, [])

    def test_write_row(self):
        msg = self.msg_helper.make_inbound("foo")
        self.formatter.write_row(self.request, msg)
        self.assertEqual(self.request.written, [
            msg.to_json(), "\n",
        ])


class TestCsvFormatter(VumiTestCase):
    def setUp(self):
        self.msg_helper = self.add_helper(MessageHelper())
        self.request = DummyRequest([''])
        self.formatter = CsvFormatter()

    def test_implements_IMessageFormatter(self):
        self.assertTrue(IMessageFormatter.providedBy(self.formatter))

    def test_add_http_headers(self):
        self.formatter.add_http_headers(self.request)
        self.assertEqual(
            self.request.responseHeaders.getRawHeaders('Content-Type'),
            ['text/csv; charset=utf-8'])

    def test_write_row_header(self):
        self.formatter.write_row_header(self.request)
        self.assertEqual(self.request.written, [
            "timestamp,message_id,to_addr,from_addr,in_reply_to,session_event,"
            "content,group\r\n"
        ])

    def assert_row_written(self, row, row_template, msg):
        self.assertEqual(row, [row_template % {
            'ts': msg['timestamp'].isoformat(),
            'id': msg['message_id'],
        }])

    def test_write_row(self):
        msg = self.msg_helper.make_inbound("foo")
        self.formatter.write_row(self.request, msg)
        self.assert_row_written(
            self.request.written,
            "%(ts)s,%(id)s,9292,+41791234567,,,foo,\r\n", msg)

    def test_write_row_with_in_reply_to(self):
        msg = self.msg_helper.make_inbound("foo", in_reply_to="msg-2")
        self.formatter.write_row(self.request, msg)
        self.assert_row_written(
            self.request.written,
            "%(ts)s,%(id)s,9292,+41791234567,msg-2,,foo,\r\n", msg)

    def test_write_row_with_session_event(self):
        msg = self.msg_helper.make_inbound("foo", session_event="new")
        self.formatter.write_row(self.request, msg)
        self.assert_row_written(
            self.request.written,
            "%(ts)s,%(id)s,9292,+41791234567,,new,foo,\r\n", msg)

    def test_write_row_with_group(self):
        msg = self.msg_helper.make_inbound("foo", group="#channel")
        self.formatter.write_row(self.request, msg)
        self.assert_row_written(
            self.request.written,
            "%(ts)s,%(id)s,9292,+41791234567,,,foo,#channel\r\n", msg)

    def test_write_row_with_unicode_content(self):
        msg = self.msg_helper.make_inbound(u"føø", group="#channel")
        self.formatter.write_row(self.request, msg)
        self.assert_row_written(
            self.request.written,
            u"%(ts)s,%(id)s,9292,+41791234567,,,føø,#channel\r\n".encode(
                "utf-8"),
            msg)


class TestCsvEventFormatter(VumiTestCase):
    def setUp(self):
        self.msg_helper = self.add_helper(MessageHelper())
        self.request = DummyRequest([''])
        self.formatter = CsvEventFormatter()

    def test_implements_IMessageFormatter(self):
        self.assertTrue(IMessageFormatter.providedBy(self.formatter))

    def test_add_http_headers(self):
        self.formatter.add_http_headers(self.request)
        self.assertEqual(
            self.request.responseHeaders.getRawHeaders('Content-Type'),
            ['text/csv; charset=utf-8'])

    def test_write_row_header(self):
        self.formatter.write_row_header(self.request)
        self.assertEqual(self.request.written, [
            "timestamp,event_id,status,user_message_id,nack_reason\r\n"
        ])

    def assert_row_written(self, row, row_template, event):
        self.assertEqual(row, [row_template % {
            'ts': event['timestamp'].isoformat(),
            'id': event['event_id'],
            'msg_id': event['user_message_id'],
        }])

    def test_write_row_ack(self):
        event = self.msg_helper.make_ack()
        self.formatter.write_row(self.request, event)
        self.assert_row_written(
            self.request.written,
            "%(ts)s,%(id)s,ack,%(msg_id)s,\r\n", event)

    def test_write_row_nack(self):
        event = self.msg_helper.make_nack(nack_reason="raisins")
        self.formatter.write_row(self.request, event)
        self.assert_row_written(
            self.request.written,
            "%(ts)s,%(id)s,nack,%(msg_id)s,raisins\r\n", event)

    def test_write_row_delivery_report(self):
        event = self.msg_helper.make_delivery_report()
        self.formatter.write_row(self.request, event)
        self.assert_row_written(
            self.request.written,
            "%(ts)s,%(id)s,delivery_report.delivered,%(msg_id)s,\r\n", event)

    def test_write_row_with_unicode_content(self):
        event = self.msg_helper.make_nack(nack_reason=u"føø")
        self.formatter.write_row(self.request, event)
        self.assert_row_written(
            self.request.written,
            "%(ts)s,%(id)s,nack,%(msg_id)s,føø\r\n", event)
