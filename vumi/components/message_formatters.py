# -*- test-case-name: vumi.components.tests.test_message_formatters -*-

from csv import writer

from zope.interface import Interface, implements


class IMessageFormatter(Interface):
    """ Interface for writing messages to an HTTP request. """

    def add_http_headers(request):
        """
        Add any needed HTTP headers to the request.

        Often used to set the Content-Type header.
        """

    def write_row_header(request):
        """
        Write any header bytes that need to be written to the request before
        messages.
        """

    def write_row(request, message):
        """
        Write a :class:`TransportUserMessage` to the request.
        """


class JsonFormatter(object):
    """ Formatter for writing messages to requests as JSON. """

    implements(IMessageFormatter)

    def add_http_headers(self, request):
        resp_headers = request.responseHeaders
        resp_headers.addRawHeader(
            'Content-Type', 'application/json; charset=utf-8')

    def write_row_header(self, request):
        pass

    def write_row(self, request, message):
        request.write(message.to_json())
        request.write('\n')


class CsvFormatter(object):
    """ Formatter for writing messages to requests as CSV. """

    implements(IMessageFormatter)

    FIELDS = (
        'message_id',
        'to_addr',
        'from_addr',
        'in_reply_to',
        'session_event',
        'content',
        'group',
    )

    def add_http_headers(self, request):
        resp_headers = request.responseHeaders
        resp_headers.addRawHeader(
            'Content-Type', 'text/csv; charset=utf-8')

    def write_row_header(self, request):
        writer(request).writerow(self.FIELDS)

    def write_row(self, request, message):
        writer(request).writerow(list(
            (message[key] or '').encode('utf-8')
            for key in self.FIELDS))
