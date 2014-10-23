# -*- coding: utf-8 -*-

from twisted.web.test.test_web import DummyRequest

from vumi.components.message_formatters import (
    IMessageFormatter, JsonFormatter, CsvFormatter)

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
            "message_id,to_addr,from_addr,in_reply_to,session_event,content,"
            "group\r\n"
        ])

    def test_write_row(self):
        msg = self.msg_helper.make_inbound("foo")
        self.formatter.write_row(self.request, msg)
        self.assertEqual(self.request.written, [
            "%s,9292,+41791234567,,,foo,\r\n" % msg['message_id']
        ])
