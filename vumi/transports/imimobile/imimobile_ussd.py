# -*- test-case-name: vumi.transports.imimobile.tests.test_imimobile_ussd -*-

import re
import json
from datetime import datetime, timedelta


from twisted.python import log
from twisted.web import http
from twisted.internet.defer import inlineCallbacks

from vumi.components.session import SessionManager
from vumi.message import TransportUserMessage
from vumi.transports.httprpc import HttpRpcTransport


class ImiMobileUssdTransport(HttpRpcTransport):
    """
    HTTP transport for USSD with IMImobile in India.

    Configuration parameters:

    :param str transport_name:
        The name this transport instance will use to create its queues
    :param str web_path:
        The HTTP path to listen on.
    :param int web_port:
        The HTTP port to listen on.
    :param dict suffix_to_addrs:
        Mappings between url suffixes and to addresses.
    :param str user_terminated_session_message:
        A regex used to identify user terminated session messages. Default is
        '^Map Dialog User Abort User Reason'.
    :param str user_terminated_session_response:
        Response given back to the user if the user terminated the session.
        Default is 'Session Ended'.
    :param dict redis_manager:
        The configuration parameters for connecting to Redis.
    :param int ussd_session_timeout:
        Number of seconds before USSD session information stored in Redis
        expires. Default is 600s.
    """

    transport_type = 'ussd'

    ENCODING = 'utf-8'
    EXPECTED_FIELDS = set(['msisdn', 'msg', 'code', 'tid', 'dcs'])

    # errors
    RESPONSE_FAILURE_ERROR = "Response to http request failed."
    INSUFFICIENT_MSG_FIELDS_ERROR = "Insufficiant message fields provided."

    def validate_config(self):
        super(ImiMobileUssdTransport, self).validate_config()

        # Mappings between url suffixes and the tags used as the to_addr for
        # inbound messages (e.g. shortcodes or longcodes). This is necessary
        # since the requests from ImiMobile do not provided us with this.
        self.suffix_to_addrs = self.config['suffix_to_addrs']

        # IMImobile do not provide a parameter or header to signal termination
        # of the session by the user, other than sending "Map Dialog User Abort
        # User Reason: User specific reason" as the request's message content.
        self.user_terminated_session_re = re.compile(
            self.config.get('user_terminated_session_message',
                            '^Map Dialog User Abort User Reason'))

        self.user_terminated_session_response = self.config.get(
            'user_terminated_session_response', 'Session Ended')

    @inlineCallbacks
    def setup_transport(self):
        super(ImiMobileUssdTransport, self).setup_transport()

        # configure session manager
        r_config = self.config.get('redis_manager', {})
        r_prefix = "vumi.transports.imimobile_ussd:%s" % self.transport_name
        session_timeout = int(self.config.get("ussd_session_timeout", 600))
        self.session_manager = yield SessionManager.from_redis_config(
            r_config, r_prefix, max_session_length=session_timeout)

    @inlineCallbacks
    def teardown_transport(self):
        yield super(ImiMobileUssdTransport, self).teardown_transport()
        yield self.session_manager.stop()

    def get_to_addr(self, request):
        """
        Extracts the request url path's suffix and uses it to obtain the tag
        associated with the suffix. Returns a tuple consisting of the tag and
        a dict of errors encountered.
        """
        errors = {}

        [suffix] = request.postpath
        tag = self.suffix_to_addrs.get(suffix, None)
        if tag is None:
            errors['unknown_suffix'] = suffix

        return tag, errors

    @classmethod
    def ist_to_utc(cls, timestamp):
        """
        Accepts a timestamp in the format `[M]M/[D]D/YYYY HH:MM:SS (am|pm)` and
        in India Standard Time, and returns a datetime object normalized to
        UTC time.
        """
        return (datetime.strptime(timestamp, '%m/%d/%Y %I:%M:%S %p')
                - timedelta(hours=5, minutes=30))

    def user_has_terminated_session(self, content):
        return self.user_terminated_session_re.match(content) is not None

    @inlineCallbacks
    def handle_raw_inbound_message(self, message_id, request):
        errors = {}

        to_addr, to_addr_errors = self.get_to_addr(request)
        errors.update(to_addr_errors)

        values, field_value_errors = self.get_field_values(request,
                                                        self.EXPECTED_FIELDS)
        errors.update(field_value_errors)

        if errors:
            log.msg('Unhappy incoming message: %s' % (errors,))
            yield self.finish_request(
                message_id, json.dumps(errors), code=http.BAD_REQUEST)
            return

        from_addr = values['msisdn']
        log.msg('ImiMobileTransport receiving inbound message from %s to %s.' %
                (from_addr, to_addr))

        content = values['msg']
        if self.user_has_terminated_session(content):
            yield self.session_manager.clear_session(from_addr)
            session_event = TransportUserMessage.SESSION_CLOSE

            # IMImobile use 0 for termination of a session
            self.finish_request(
                message_id,
                self.user_terminated_session_response,
                headers={'X-USSD-SESSION': ['0']})
        else:
            # We use the msisdn (from_addr) to make a guess about the
            # whether the session is new or not.
            session = yield self.session_manager.load_session(from_addr)
            if session:
                session_event = TransportUserMessage.SESSION_RESUME
                yield self.session_manager.save_session(from_addr, session)
            else:
                session_event = TransportUserMessage.SESSION_NEW
                yield self.session_manager.create_session(
                    from_addr, from_addr=from_addr, to_addr=to_addr)

        yield self.publish_message(
            message_id=message_id,
            content=content,
            to_addr=to_addr,
            from_addr=from_addr,
            provider='imimobile',
            session_event=session_event,
            transport_type=self.transport_type,
            transport_metadata={
                'imimobile_ussd': {
                    'tid': values['tid'],
                    'code': values['code'],
                    'dcs': values['dcs'],
                }
            })

    @inlineCallbacks
    def handle_outbound_message(self, message):
        error = None
        message_id = message['message_id']

        if message.payload.get('in_reply_to') and 'content' in message.payload:
            # IMImobile use 1 for resume and 0 for termination of a session
            session_header_value = '1'

            if message['session_event'] == TransportUserMessage.SESSION_CLOSE:
                yield self.session_manager.clear_session(message['to_addr'])
                session_header_value = '0'

            response_id = self.finish_request(
                message['in_reply_to'],
                message['content'].encode(self.ENCODING),
                headers={'X-USSD-SESSION': [session_header_value]})

            if response_id is None:
                error = self.RESPONSE_FAILURE_ERROR
        else:
            error = self.INSUFFICIENT_MSG_FIELDS_ERROR

        if error is not None:
            yield self.publish_nack(message_id, error)
            return

        yield self.publish_ack(user_message_id=message_id,
                               sent_message_id=message_id)
