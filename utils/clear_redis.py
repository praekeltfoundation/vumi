"""Utility for clearing all keys out of redis -- do not use in production!"""

import sys
from optparse import OptionParser

import redis


def option_parser():
    parser = OptionParser(description="""
        Utility for purging all the keys in a Redis database. Useful
        when using VUMITEST_REDIS_DB=x (where x is the number of the
        Redis database) to run tests against a real redis instance
        and the tests aren't properly cleaning up after / before
        themselves.
        """.strip())
    parser.add_option("-d", "--db",
                      type="int", dest="db", default=1,
                      help="Redis DB to clear.")
    parser.add_option("-f", "--force",
                      action="store_true", dest="force", default=False,
                      help="Don't ask for confirmation.")
    return parser


def main():
    parser = option_parser()
    options, args = parser.parse_args()
    if args:
        parser.print_help()
        return 1
    if not options.force:
        confirm = raw_input("About to delete ALL redis keys in db %d. "
                            "Press Y to confirm, N to exit: " %
                            (options.db,))
        if confirm.lower() != 'y':
            return 1
    r_server = redis.Redis(db=options.db)
    keys = r_server.keys()
    for key in keys:
        r_server.delete(key)
    print "Deleted %i keys." % len(keys)
    return 0

if __name__ == "__main__":
    sys.exit(main())
