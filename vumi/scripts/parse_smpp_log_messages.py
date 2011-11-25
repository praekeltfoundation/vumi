# -*- test-case-name: vumi.scripts.tests.test_parse_smpp_log_messages -*-
import sys
import re
import json
from twisted.python import usage
from twisted.internet import reactor
from twisted.internet.defer import maybeDeferred
from datetime import datetime


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
                            'PUBLISHING INBOUND: (?P<message>.*)')


def parse_date(string):
    match = DATE_PATTERN.match(string)
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

    def __init__(self, options):
        self.start = options['from']
        if self.start:
            self.start = datetime(**parse_date(self.start))
        self.stop = options['until']
        if self.stop:
            self.stop = datetime(**parse_date(self.stop))
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
        match = LOG_PATTERN.match(line)
        if match:
            data = match.groupdict()
            date = datetime(**parse_date(data['date']))
            if self.start and self.start > date:
                return
            if self.stop and date > self.stop:
                return
            self.emit(json.dumps(eval(data['message'])))


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
