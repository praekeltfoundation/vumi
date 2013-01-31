import json
from datetime import datetime, timedelta

from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.components import SessionManager
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
    :param dict redis_manager:
        The configuration parameters for connecting to Redis.
    :param int ussd_session_timeout:
        Number of seconds before USSD session information stored in Redis
        expires. Default is 600s.
    """

    EXPECTED_FIELDS = set(['msisdn', 'sms', 'circle', 'opnm', 'datetime'])
    ERRORS = {
        'RESPONSE_FAILURE': "Response to http request failed.",
        'INSUFFICIENT_MSG_FIELDS': "Insufficiant message fields provided.",
    }

    @inlineCallbacks
    def setup_transport(self):
        super(ImiMobileUssdTransport, self).setup_transport()

        # Mappings between url suffixes and the tags used as the to_addr for
        # inbound messages (e.g. shortcodes or longcodes). This is necessary
        # since the requests from ImiMobile do not provided us with this.
        self.suffix_to_addrs = self.config['suffix_to_addrs']

        # configure session manager
        r_config = self.config.get('redis_manager', {})
        r_prefix = "vumi.transports.imimobile_ussd:%s" % self.transport_name
        session_timeout = int(self.config.get("ussd_session_timeout", 600))
        self.session_manager = yield SessionManager.from_redis_config(
            r_config, r_prefix, max_session_length=session_timeout)

    @inlineCallbacks
    def teardown_transport(self):
        yield self.session_manager.stop()
        yield super(ImiMobileUssdTransport, self).teardown_transport()

    def get_to_addr(self, request):
        """
        Extracts the request url path's suffix and uses it to obtain the tag
        associated with the suffix. Returns a tuple consisting of the tag and
        a dict of errors encountered.
        """
        errors = {}

        _, suffix = request.path.rsplit('/', 1)
        tag = self.suffix_to_addrs.get(suffix, None)
        if tag is None:
            errors['unknown_suffix'] = suffix

        return tag, errors

    def get_field_values(self, request):
        """
        Parses the request params and returns a tuple consisting of a dict of
        the expected fields' values and a dict of errors encountered, such as
        unexpected or missing params.
        """
        values = {}
        errors = {}

        for field in request.args:
            if field not in self.EXPECTED_FIELDS:
                errors.setdefault('unexpected_parameter', []).append(field)
            else:
                values[field] = str(request.args.get(field)[0]).encode('utf-8')

        for field in self.EXPECTED_FIELDS:
            if field not in values:
                errors.setdefault('missing_parameter', []).append(field)

        return values, errors

    @classmethod
    def ist_to_utc(cls, timestamp):
        """
        Accepts a timestamp in the format `[M]M/[D]D/YYYY HH:MM:SS (am|pm)` and
        in India Standard Time, and returns a datetime object normalized to
        UTC time.
        """
        return (datetime.strptime(timestamp, '%m/%d/%Y %I:%M:%S %p') -
                timedelta(hours=5, minutes=30))

    @inlineCallbacks
    def handle_raw_inbound_message(self, message_id, request):
        errors = {}

        to_addr, to_addr_errors = self.get_to_addr(request)
        errors.update(to_addr_errors)

        values, field_value_errors = self.get_field_values(request)
        errors.update(field_value_errors)

        if errors:
            log.msg('Unhappy incoming message: %s' % (errors,))
            yield self.finish_request(message_id, json.dumps(errors), code=400)
            return

        from_addr = values['msisdn']
        log.msg('ImiMobileTransport receiving inbound message from %s to %s.' %
                (from_addr, to_addr))

        # We use the msisdn (from_addr) to make a guess about the
        # session_event.
        # TODO: Finalise or simplify this block after integration testing
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
            content=values['sms'],
            to_addr=to_addr,
            from_addr=from_addr,
            provider='imimobile',
            session_event=session_event,
            transport_type='ussd',
            transport_metadata={
                'imimobile_ussd': {
                    'circle': values['circle'],
                    'opnm': values['opnm'],
                    'datetime': self.ist_to_utc(values['datetime']),
                }
            })

    def handle_outbound_message(self, message):
        error = None
        message_id = message['message_id']
        if message.payload.get('in_reply_to') and 'content' in message.payload:
            session_has_ended = (
                message['session_event'] == TransportUserMessage.SESSION_CLOSE)

            response_id = self.finish_request(
                message['in_reply_to'],
                message['content'].encode('utf-8'),
                headers={'X-USSD-SESSION': [0 if session_has_ended else 1]})

            if response_id is None:
                error = self.ERRORS['RESPONSE_FAILURE']
        else:
            error = self.ERRORS['INSUFFICIENT_MSG_FIELDS']

        if error is not None:
            return self.publish_nack(message_id, error)

        return self.publish_ack(user_message_id=message_id,
                                sent_message_id=message_id)
