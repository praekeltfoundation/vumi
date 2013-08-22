# -*- test-case-name: vumi.transports.smssync.tests.test_smssync -*-
import json
import datetime

from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor

from vumi import log
from vumi.message import TransportUserMessage
from vumi.utils import normalize_msisdn
from vumi.persist.txredis_manager import TxRedisManager
from vumi.transports.failures import PermanentFailure
from vumi.transports.httprpc import HttpRpcTransport


class SmsSyncMsgInfo(object):
    """Holder of attributes needed to process an SMSSync message.

    :param str account_id:
        An ID for the acocunt this message is being sent
        to / from.
    :param str smssync_secret:
        The shared SMSSync secret for the account this message
        is being sent to / from.
    :param str country_code:
        The default country_code for the account this message
        is being sent to / from.
    """
    def __init__(self, account_id, smssync_secret, country_code):
        self.account_id = account_id
        self.smssync_secret = smssync_secret
        self.country_code = country_code


class BaseSmsSyncTransport(HttpRpcTransport):
    """
    Ushahidi SMSSync Transport for getting messages into vumi.

    :param str web_path:
        The path relative to the host where this listens
    :param int web_port:
        The port this listens on
    :param str transport_name:
        The name this transport instance will use to create its queues
    :param dict redis_manager:
        Redis client configuration.
    :param float reply_delay:
        The amount of time to wait (in seconds) for a reply message
        before closing the SMSSync HTTP inbound message
        request. Replies received within this amount of time will be
        returned with the reply (default: 0.5s).
    """

    transport_type = 'sms'

    # SMSSync True and False constants
    SMSSYNC_TRUE, SMSSYNC_FALSE = ("true", "false")
    SMSSYNC_DATE_FORMAT = "%m-%d-%y %H:%M"
    MILLISECONDS = 1000

    callLater = reactor.callLater

    def validate_config(self):
        super(BaseSmsSyncTransport, self).validate_config()
        self._reply_delay = float(self.config.get('reply_delay', '0.5'))

    @inlineCallbacks
    def setup_transport(self):
        r_config = self.config.get('redis_manager', {})
        self.redis = yield TxRedisManager.from_config(r_config)
        yield super(BaseSmsSyncTransport, self).setup_transport()

    @inlineCallbacks
    def teardown_transport(self):
        yield super(BaseSmsSyncTransport, self).teardown_transport()
        yield self.redis.close_manager()

    def msginfo_for_request(self, request):
        """Returns an :class:`SmsSyncMsgInfo` instance for this request.

        May return a deferred that yields the actual result to its callback.
        """
        raise NotImplementedError("Sub-classes should implement"
                                  " msginfo_for_request")

    def msginfo_for_message(self, msg):
        """Returns an :class:`SmsSyncMsgInfo` instance for this outbound
        message.

        May return a deferred that yields the actual result to its callback.
        """
        raise NotImplementedError("Sub-classes should implement"
                                  " msginfo_for_message")

    def add_msginfo_metadata(self, payload, msginfo):
        """Update an outbound message's payload's transport_metadata to allow
        msginfo to be reconstructed from replies."""
        raise NotImplementedError("Sub-class should implement"
                                  " add_msginfo_metadata")

    def key_for_account(self, account_id):
        return "outbound_messages#%s" % (account_id,)

    @inlineCallbacks
    def _handle_send(self, message_id, request):
        msginfo = yield self.msginfo_for_request(request)
        if msginfo is None:
            log.warning("Bad account: %r (args: %r)" % (request, request.args))
            yield self._send_response(message_id, success=self.SMSSYNC_FALSE)
            return
        yield self._respond_with_pending_messages(
            msginfo, message_id, task='send', secret=msginfo.smssync_secret)

    def _check_request_args(self, request, expected_keys):
        expected_keys = set(expected_keys)
        present_keys = set(request.args.keys())
        return expected_keys.issubset(present_keys)

    def _parse_timestamp(self, request):
        smssync_timestamp = request.args['sent_timestamp'][0]
        timestamp = None
        if timestamp is None:
            try:
                timestamp = datetime.datetime.strptime(
                    smssync_timestamp, self.SMSSYNC_DATE_FORMAT)
            except ValueError:
                pass

        if timestamp is None:
            try:
                utc_ms = int(request.args['sent_timestamp'][0])
                timestamp = datetime.datetime.utcfromtimestamp(
                    utc_ms / self.MILLISECONDS)
            except ValueError:
                pass

        if timestamp is None:
            log.warning("Bad timestamp format: %r (args: %r)"
                        % (request, request.args))
            timestamp = datetime.datetime.utcnow()

        return timestamp

    @inlineCallbacks
    def _handle_receive(self, message_id, request):
        if not self._check_request_args(request, ['secret', 'sent_timestamp',
                                                  'sent_to', 'from',
                                                  'message']):
            log.warning("Bad request: %r (args: %r)" % (request, request.args))
            yield self._send_response(message_id, success=self.SMSSYNC_FALSE)
            return
        msginfo = yield self.msginfo_for_request(request)
        supplied_secret = request.args['secret'][0]
        if msginfo is None or (msginfo.smssync_secret and
                               not msginfo.smssync_secret == supplied_secret):
            log.warning("Bad secret or account: %r (args: %r)"
                        % (request, request.args))
            yield self._send_response(message_id, success=self.SMSSYNC_FALSE)
            return

        timestamp = self._parse_timestamp(request)

        normalize = lambda raw: normalize_msisdn(raw, msginfo.country_code)
        message = {
            'message_id': message_id,
            'transport_type': self.transport_type,
            'to_addr': normalize(request.args['sent_to'][0]),
            'from_addr': normalize(request.args['from'][0]),
            'content': request.args['message'][0],
            'timestamp': timestamp,
        }
        self.add_msginfo_metadata(message, msginfo)
        yield self.publish_message(**message)
        self.callLater(self._reply_delay, self._respond_with_pending_messages,
                       msginfo, message_id, success=self.SMSSYNC_TRUE)

    def _send_response(self, message_id, **kw):
        response = {'payload': kw}
        return self.finish_request(message_id, json.dumps(response))

    @inlineCallbacks
    def _respond_with_pending_messages(self, msginfo, message_id, **kw):
        """Gathers pending messages and sends a response including them."""
        outbound_ids = []
        outbound_messages = []
        account_key = self.key_for_account(msginfo.account_id)
        while True:
            msg_json = yield self.redis.lpop(account_key)
            if msg_json is None:
                break
            msg = TransportUserMessage.from_json(msg_json)
            outbound_ids.append(msg['message_id'])
            outbound_messages.append({'to': msg['to_addr'],
                                      'message': msg['content'] or ''})
        yield self._send_response(message_id, messages=outbound_messages, **kw)
        for outbound_id in outbound_ids:
            yield self.publish_ack(user_message_id=outbound_id,
                                   sent_message_id=outbound_id)

    def handle_raw_inbound_message(self, message_id, request):
        # This matches the dispatch logic in Usahidi's request
        # handler for SMSSync.
        # See https://github.com/ushahidi/Ushahidi_Web/blob/
        #             master/plugins/smssync/controllers/smssync.php
        tasks = request.args.get('task')
        task = tasks[0] if tasks else None
        if task == "send":
            return self._handle_send(message_id, request)
        else:
            return self._handle_receive(message_id, request)

    @inlineCallbacks
    def handle_outbound_message(self, message):
        msginfo = yield self.msginfo_for_message(message)
        if msginfo is None:
            err_msg = ("SmsSyncTransport couldn't determine"
                        " secret for outbound message.")
            yield self.publish_nack(user_message_id=message['message_id'],
                sent_message_id=message['message_id'], reason=err_msg)
            raise PermanentFailure(err_msg)
        else:
            account_key = self.key_for_account(msginfo.account_id)
            yield self.redis.rpush(account_key, message.to_json())


class SingleSmsSync(BaseSmsSyncTransport):
    """
    Ushahidi SMSSync Transport for a single phone.

    Additional configuration options:

    :param str smssync_secret:
        Secret of the single phone (default: '', i.e. no secret set)
    :param str account_id:
        Account id for storing outbound messages under. Defaults to
        the `smssync_secret` which is fine unless the secret changes.
    :param str country_code:
        Default country code to use when normalizing MSISDNs sent by
        SMSSync (default is the empty string, which assumes numbers
        already include the international dialing prefix).
    """

    def validate_config(self):
        super(SingleSmsSync, self).validate_config()
        # The secret is the empty string in the case where the single-phone
        # transport isn't using a secret (this fits with how the Ushahidi
        # handles the lack of a secret).
        self._smssync_secret = self.config.get('smssync_secret', '')
        self._account_id = self.config.get('account_id', self._smssync_secret)
        self._country_code = self.config.get('country_code', '').lstrip('+')

    def msginfo_for_request(self, request):
        return SmsSyncMsgInfo(self._account_id, self._smssync_secret,
                              self._country_code)

    def msginfo_for_message(self, msg):
        return SmsSyncMsgInfo(self._account_id, self._smssync_secret,
                              self._country_code)

    def add_msginfo_metadata(self, msg, msginfo):
        # The single phone SMSSync transport doesn't require any
        # transport metadata in order to reconstruct msginfo
        pass


class MultiSmsSync(BaseSmsSyncTransport):
    """
    Ushahidi SMSSync Transport for a multiple phones.

    Each phone accesses a URL that has the form `<web_path>/<account_id>/`.
    A blank secret should be entered into the SMSSync `secret` field.

    Additional configuration options:

    :param dict country_codes:
        Map from `account_id` to the country code to use when normalizing
        MSISDNs sent by SMSSync to that API URL. If an `account_id` is not
        in this map the default is to use an empty country code string).
    """

    def validate_config(self):
        super(MultiSmsSync, self).validate_config()
        self._country_codes = self.config.get('country_codes', {})

    def _country_code(self, account_id):
        return self._country_codes.get(account_id, '').lstrip('+')

    def msginfo_for_request(self, request):
        pathparts = request.path.rstrip('/').split('/')
        if not pathparts or not pathparts[-1]:
            return None
        account_id = pathparts[-1]
        return SmsSyncMsgInfo(account_id, '', self._country_code(account_id))

    def msginfo_for_message(self, msg):
        account_id = self.account_from_message(msg)
        if account_id is None:
            return None
        return SmsSyncMsgInfo(account_id, '', self._country_code(account_id))

    def add_msginfo_metadata(self, msg, msginfo):
        # The single phone SMSSync transport doesn't require any
        # transport metadata in order to reconstruct msginfo
        self.add_account_to_payload(msg, msginfo.account_id)

    @staticmethod
    def account_from_message(msg):
        return msg['transport_metadata'].get('account_id')

    @classmethod
    def add_account_to_message(cls, msg, account_id):
        return cls.add_account_to_payload(msg.payload, account_id)

    @staticmethod
    def add_account_to_payload(payload, account_id):
        transport_metadata = payload.setdefault('transport_metadata', {})
        transport_metadata['account_id'] = account_id
