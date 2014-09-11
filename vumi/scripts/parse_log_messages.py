# -*- test-case-name: vumi.scripts.tests.test_parse_log_messages -*-
import sys
import re
import json
import warnings
from twisted.python import usage
from twisted.internet import reactor
from twisted.internet.defer import maybeDeferred
import datetime
from vumi.message import to_json


DATE_PATTERN = re.compile(
    r'(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2}) '
    r'(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})')
LOG_PATTERN = {
    'vumi': re.compile(
        r'(?P<date>[\d\-\:\s]+)\+0000 .* '
        r'Inbound: <Message payload="(?P<message>.*)">'),
    'smpp_inbound': re.compile(
        r'(?P<date>[\d\-\:\s]+)\+0000 .* '
        r'PUBLISHING INBOUND: (?P<message>.*)'),
    'smpp_outbound': re.compile(
        r'(?P<date>[\d\-\:\s]+)\+0000 .* '
        r'Consumed outgoing message <Message payload="(?P<message>.*)">'),
    'dispatcher_inbound_message': re.compile(
        r'(?P<date>[\d\-\:\s]+)\+0000 Processed inbound message for [a-zA-Z0-9_]+: (?P<message>.*)'),
    'dispatcher_outbound_message': re.compile(
        r'(?P<date>[\d\-\:\s]+)\+0000 Processed outbound message for [a-zA-Z0-9_]+: (?P<message>.*)'),
    'dispatcher_event': re.compile(
        r'(?P<date>[\d\-\:\s]+)\+0000 Processed event message for [a-zA-Z0-9_]+: (?P<message>.*)'),
}


class Options(usage.Options):
    optParameters = [
        ["from", "f", None,
         "Ignore any log lines prior to timestamp [YYYY-MM-DD HH:MM:SS]"],
        ["until", "u", None,
         "Ignore any log lines after timestamp [YYYY-MM-DD HH:MM:SS]"],
        ["format", None, "vumi",
         "Message format, one of: [vumi, smpp] (default vumi)"],
    ]

    longdesc = """Parses inbound messages logged by a Vumi worker from stdin
    and outputs them as JSON encoded Vumi messages to stdout. Useful
    along with the `inject_messages.py` script to replay failed inbound
    messages. The two formats supported currently are 'vumi' (which is a
    simple custom format used by some third-party workers) and 'smpp' (which is
    used for logging inbound messages by the SMPP transport).
    """


def parse_date(string, pattern):
    match = pattern.match(string)
    if match:
        return dict((k, int(v)) for k, v in match.groupdict().items())
    return {}


class LogParser(object):
    """
    Parses Vumi TransportUserMessages from a log file and writes
    simple JSON serialized Vumi TransportUserMessages to stdout.

    Regular expression may be passed in to specify log and date
    format.

    Two common output formats are the one used by SMPP for logging:

    `YYYY-MM-DD HH:MM:SS+0000 <bits of text> PUBLISHING INBOUND: <json
    message>`

    and the one used by some Vumi campaign workers:

    `YYYY-MM-DD HH:MM:SS+0000 <bits of text> Inbound: <Message
    payload="<json message>">`
    """

    def __init__(self, options, date_pattern=None, log_pattern=None):
        self.date_pattern = date_pattern or DATE_PATTERN
        if options['format'] == 'smpp':
            warnings.warn(
                'smpp format is deprecated, use smpp_inbound instead',
                category=DeprecationWarning)
            options['format'] = 'smpp_inbound'

        self.log_pattern = log_pattern or LOG_PATTERN.get(options['format'])
        self.start = options['from']
        if self.start:
            self.start = datetime.datetime(**parse_date(self.start,
                                            self.date_pattern))
        self.stop = options['until']
        if self.stop:
            self.stop = datetime.datetime(**parse_date(self.stop,
                                            self.date_pattern))
        self.parse()

    def parse(self):
        while True:
            line = sys.stdin.readline()
            if not line:
                break
            self.readline(line)

    def emit(self, obj):
        sys.stdout.write('%s\n' % (obj,))

    def readline(self, line):
        match = self.log_pattern.match(line)
        if match:
            data = match.groupdict()
            date = datetime.datetime(**parse_date(data['date'],
                                        self.date_pattern))
            if self.start and self.start > date:
                return
            if self.stop and date > self.stop:
                return
            try:
                # JSON
                self.emit(to_json(json.loads(data['message'])))
            except:
                # Raw dict being printed
                self.emit(to_json(eval(data['message'])))


if __name__ == '__main__':
    try:
        options = Options()
        options.parseOptions()
    except usage.UsageError, errortext:
        print '%s: %s' % (sys.argv[0], errortext)
        print '%s: Try --help for usage details.' % (sys.argv[0])
        sys.exit(1)

    def _eb(f):
        f.printTraceback()

    def _main():
        d = maybeDeferred(LogParser, options)
        d.addErrback(_eb)
        d.addCallback(lambda _: reactor.stop())

    reactor.callLater(0, _main)
    reactor.run()
