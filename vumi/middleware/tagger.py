# -*- test-case-name: vumi.middleware.tests.test_tagger -*-

import re

from vumi.middleware.base import TransportMiddleware


class TaggingMiddleware(TransportMiddleware):
    """
    Transport middleware for adding tag names to inbound messages.

    Transports that wish to eventually have incoming messages associated
    with an existing message batch by :class:`vumi.application.MessageStore`
    need to ensure that incoming messages are provided with a tag by this
    or some other middleware.

    Configuration options:

    :param string addr_pattern:
        Regular expression matching the to_addr of incoming messages.
    :type tag_template: a pair of strings
    :param string tag_template:
        Template for producing tag from successful matches of
        `addr_pattern`. Tags are a `(tagpool, tag_name)` pair and are
        produced using `(match.expand(tag_template[0]),
        match.expand(tag_template[1]))`.
    """

    def setup_middleware(self):
        self.to_addr_re = re.compile(self.config['addr_pattern'])
        self.tagpool_template, self.tagname_template = \
                               self.config['tag_template']

    def handle_inbound(self, message, endpoint):
        match = self.to_addr_re.match(message['to_addr'])
        if match is not None:
            tag = (match.expand(self.tagpool_template),
                   match.expand(self.tagname_template))
        else:
            tag = None
        message['tag'] = tag
        return message
