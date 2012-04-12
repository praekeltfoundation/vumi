# -*- test-case-name: vumi.scripts.tests.test_parse_smpp_log_messages -*-
import sys
import re
from twisted.python import usage
from twisted.internet import reactor
from twisted.internet.defer import maybeDeferred
from vumi.scripts.parse_log_messages import LogParser, Options


LOG_PATTERN = re.compile(r'(?P<date>[\d\-\:\s]+)\+0000 .* ' +
                            'PUBLISHING INBOUND: (?P<message>.*)')

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
        maybeDeferred(LogParser, options, log_pattern=LOG_PATTERN
                      ).addErrback(_eb
                      ).addCallback(lambda _: reactor.stop())

    reactor.callLater(0, _main)
    reactor.run()
