# -*- test-case-name: vumi.middleware.tests.test_tagger -*-

import re

from vumi.middleware.base import TransportMiddleware


class TaggingMiddleware(TransportMiddleware):
    """
    Transport middleware for adding tag names to inbound messages and
    for adding additional parameters to outbound messages based on
    their tag.

    Transports that wish to eventually have incoming messages
    associated with an existing message batch by
    :class:`vumi.application.MessageStore` or via
    :class:`vumi.middleware.StoringMiddleware` need to ensure that
    incoming messages are provided with a tag by this or some other
    middleware.

    Configuration options:

    :param string addr_pattern:
        Regular expression matching the to_addr of incoming messages.
    :type tag_template: a pair of strings
    :param string tag_template:
        Template for producing tag from successful matches of
        `addr_pattern`. Tags are a `(tagpool, tag_name)` pair and are
        produced using `(match.expand(tag_template[0]),
        match.expand(tag_template[1]))`.
    :param string tag_pattern:
        Regular expression matching the tag name of outgoing messages.
        Note: The tag pool the tag belongs to is not examined.
    :param dict msg_template:
        A dictionary of additional key-value pairs to add to the
        outgoing message payloads whose tag mataches `tag_pattern`.
        Values are expanded using `match.expand(value)`.
    """
    def setup_middleware(self):
        self.to_addr_re = re.compile(self.config['addr_pattern'])
        self.tagpool_template, self.tagname_template = \
                               self.config['tag_template']

        self.tag_re = re.compile(self.config['tag_pattern'])
        self.msg_template = self.config['msg_template']

    def handle_inbound(self, message, endpoint):
        to_addr = message.get('to_addr')
        if to_addr is not None:
            match = self.to_addr_re.match(to_addr)
        else:
            match = None
        if match is not None:
            tag = (match.expand(self.tagpool_template),
                   match.expand(self.tagname_template))
        else:
            tag = None
        message['tag'] = tag
        return message

    def handle_outbound(self, message, endpoint):
        tag = message.get('tag')
        if tag is not None:
            match = self.tag_re.match(tag[1])
        else:
            match = None
        if match is not None:
            for key, value in self.msg_template.iteritems():
                value = match.expand(value)
                message[key] = value
        return message

    @staticmethod
    def map_msg_to_tag(msg):
        """Convenience method for retrieving a tag that was added
        to a message by this middleware.
        """
        tag = msg.get('tag')
        if tag is None:
            return None
        return tuple(msg.get('tag'))
