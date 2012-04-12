# -*- test-case-name: vumi.scripts.tests.test_parse_log_messages -*-
import sys
import re
from twisted.python import usage
from twisted.internet import reactor
from twisted.internet.defer import maybeDeferred
import datetime
from vumi.message import to_json


class Options(usage.Options):
    optParameters = [
        ["from", "f", None,
            "Parse log lines starting from timestamp [YYYY-MM-DD HH:MM:SS]"],
        ["until", "u", None,
            "Parse log lines starting from timestamp [YYYY-MM-DD HH:MM:SS]"],
    ]

DATE_PATTERN = re.compile(r'(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2}) ' +
                        r'(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})')
LOG_PATTERN = re.compile(r'(?P<date>[\d\-\:\s]+)\+0000 .* ' +
                            'Inbound: <Message payload="(?P<message>.*)">')


def parse_date(string, pattern):
    match = pattern.match(string)
    if match:
        return dict((k, int(v)) for k, v in match.groupdict().items())
    return {}


class LogParser(object):
    """
    Parses lines from the SMPP log in the following format:

    YYYY-MM-DD HH:MM:SS+0000 <bits of text> PUBLISHING INBOUND: <json message>

    And publishes the TransportUserMessage bits to stdout for reprocessing
    or re-injecting into queues.

    """

    def __init__(self, options, date_pattern=None, log_pattern=None):
        self.date_pattern = date_pattern or DATE_PATTERN
        self.log_pattern = log_pattern or LOG_PATTERN
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
        maybeDeferred(LogParser, options
                      ).addErrback(_eb
                      ).addCallback(lambda _: reactor.stop())

    reactor.callLater(0, _main)
    reactor.run()
