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

    :param dict incoming:

        * **addr_pattern** (*string*): Regular expression matching the
          to_addr of incoming messages. Incoming messages with to_addr
          values that don't match the pattern are not modified.
        * **tagpool_template** (*string*): Template for producing tag pool
          from successful matches of `addr_pattern`. The string is
          expanded using `match.expand(tagpool_template)`.
        * **tagname_template** (*string*): Template for producing tag name
          from successful matches of `addr_pattern`. The string is
          expanded using `match.expand(tagname_template)`.

    :param dict outgoing:

        * **tagname_pattern** (*string*): Regular expression matching
          the tag name of outgoing messages. Outgoing messages with
          tag names that don't match the pattern are not
          modified. Note: The tag pool the tag belongs to is not
          examined.
        * **msg_template** (*dict*): A dictionary of additional key-value
          pairs to add to the outgoing message payloads whose tag
          matches `tag_pattern`.  Values which are strings are
          expanded using `match.expand(value)`.  Values which are
          dicts are recursed into. Values which are neither are left
          as is.
    """
    def setup_middleware(self):
        config_incoming = self.config['incoming']
        config_outgoing = self.config['outgoing']

        self.to_addr_re = re.compile(config_incoming['addr_pattern'])
        self.tagpool_template = config_incoming['tagpool_template']
        self.tagname_template = config_incoming['tagname_template']

        self.tag_re = re.compile(config_outgoing['tagname_pattern'])
        self.msg_template = config_outgoing['msg_template']

    def handle_inbound(self, message, connector_name):
        to_addr = message.get('to_addr')
        if to_addr is not None:
            match = self.to_addr_re.match(to_addr)
        else:
            match = None
        if match is not None:
            tag = (match.expand(self.tagpool_template),
                   match.expand(self.tagname_template))
            self.add_tag_to_msg(message, tag)
        return message

    def handle_outbound(self, message, connector_name):
        tag = self.map_msg_to_tag(message)
        if tag is not None:
            match = self.tag_re.match(tag[1])
        else:
            match = None
        if match is not None:
            self._deepupdate(match, message.payload, self.msg_template)
        return message

    @staticmethod
    def _deepupdate(match, origdict, newdict):
        # set of ids of processed dicts (to avoid recursion)
        seen = set([id(newdict)])
        stack = [(origdict, newdict)]
        while stack:
            current_dict, current_new_dict = stack.pop()
            for key, value in current_new_dict.iteritems():
                if isinstance(value, dict):
                    if id(value) in seen:
                        continue
                    next_dict = current_dict.setdefault(key, {})
                    seen.add(id(value))
                    stack.append((next_dict, value))
                elif isinstance(value, basestring):
                    current_dict[key] = match.expand(value)
                else:
                    current_dict[key] = value

    @staticmethod
    def add_tag_to_msg(msg, tag):
        """Convenience method for adding a tag to a message."""
        tag_metadata = msg['helper_metadata'].setdefault('tag', {})
        # convert tag to list so that msg == json.loads(json.dumps(msg))
        tag_metadata['tag'] = list(tag)

    @staticmethod
    def add_tag_to_payload(payload, tag):
        """Convenience method for adding a tag to a message payload."""
        helper_metadata = payload.setdefault('helper_metadata', {})
        tag_metadata = helper_metadata.setdefault('tag', {})
        tag_metadata['tag'] = list(tag)

    @staticmethod
    def map_msg_to_tag(msg):
        """Convenience method for retrieving a tag that was added
        to a message by this middleware.
        """
        tag = msg.get('helper_metadata', {}).get('tag', {}).get('tag')
        if tag is not None:
            # convert JSON list to a proper tag tuple
            return tuple(tag)
        return None
