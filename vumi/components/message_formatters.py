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
        'timestamp',
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
        writer(request).writerow([
            self._format_field(field, message) for field in self.FIELDS])

    def _format_field(self, field, message):
        field_formatter = getattr(self, '_format_field_%s' % (field,), None)
        if field_formatter is not None:
            field_value = field_formatter(message)
        else:
            field_value = self._format_field_default(field, message)
        return field_value.encode('utf-8')

    def _format_field_default(self, field, message):
        return message[field] or u''

    def _format_field_timestamp(self, message):
        return message['timestamp'].isoformat()
