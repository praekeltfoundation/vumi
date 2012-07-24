# -*- test-case-name: vumi.transports.smssync.tests.test_smssync -*-
import json
import datetime

from twisted.internet.defer import inlineCallbacks

from vumi.message import TransportUserMessage
from vumi.persist.txredis_manager import TxRedisManager
from vumi.transports.failures import PermanentFailure
from vumi.transports.httprpc import HttpRpcTransport


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
    """

    transport_type = 'sms'

    # SMSSync True and False constants
    SMSSYNC_TRUE, SMSSYNC_FALSE = ("true", "false")
    SMSSYNC_DATE_FORMAT = "%m-%d-%y-%H:%M"

    @inlineCallbacks
    def setup_transport(self):
        r_config = self.config.get('redis_manager', {})
        self.redis = yield TxRedisManager.from_config(r_config)
        yield super(BaseSmsSyncTransport, self).setup_transport()

    def secret_for_request(self, request):
        raise NotImplementedError("Sub-classes should implement"
                                  " secret_for_request")

    def secret_for_message(self, msg):
        raise NotImplementedError("Sub-classes should implement"
                                  " secret_for_message")

    def check_secret(self, secret, supplied_secret):
        return secret == supplied_secret

    def key_for_secret(self, secret):
        return "secret#%s" % (secret,)

    @inlineCallbacks
    def _handle_send(self, message_id, request):
        secret = self.secret_for_request(request)
        if secret is None:
            yield self._send_response(message_id, success=self.SMSSYNC_FALSE)
            return
        outbound_ids = []
        outbound_messages = []
        secret_key = self.key_for_secret(secret)
        while True:
            msg_json = yield self.redis.lpop(secret_key)
            if msg_json is None:
                break
            msg = TransportUserMessage.from_json(msg_json)
            outbound_ids.append(msg['message_id'])
            outbound_messages.append({'to': msg['to_addr'],
                                      'message': msg['content'] or ''})
        yield self._send_response(message_id, task='send', secret=secret,
                                  messages=outbound_messages)
        for outbound_id in outbound_ids:
            yield self.publish_ack(user_message_id=outbound_id,
                                   sent_message_id=outbound_id)

    @inlineCallbacks
    def _handle_receive(self, message_id, request):
        secret = self.secret_for_request(request)
        supplied_secret = request.args['secret'][0]
        if secret is None or not self.check_secret(secret, supplied_secret):
            yield self._send_response(message_id, success=self.SMSSYNC_FALSE)
            return
        timestamp = datetime.datetime.strptime(
            request.args['sent_timestamp'][0], self.SMSSYNC_DATE_FORMAT)
        message = {
            'message_id': message_id,
            'transport_type': self.transport_type,
            'to_addr': request.args['sent_to'][0],
            'from_addr': request.args['from'][0],
            'content': request.args['message'][0],
            'timestamp': timestamp,
        }
        yield self.publish_message(**message)
        yield self._send_response(message_id, success=self.SMSSYNC_TRUE)

    def _send_response(self, message_id, **kw):
        response = {'payload': kw}
        return self.finish_request(message_id, json.dumps(response))

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

    def handle_outbound_message(self, message):
        secret = self.secret_for_message(message)
        if secret is None:
            raise PermanentFailure("SmsSyncTransport couldn't determine"
                                   " secret for outbound message.")
        else:
            secret_key = self.key_for_secret(secret)
            return self.redis.rpush(secret_key, message.to_json())


class SingleSmsSync(BaseSmsSyncTransport):
    """
    Ushahidi SMSSync Transport for a single phone.

    Additional configuration options:

    :param str smssync_secret:
        Secret of the single phone (default: '', i.e. no secret set)
    """

    def validate_config(self):
        super(SingleSmsSync, self).validate_config()
        # The secret is the empty string in the case where the single-phone
        # transport isn't using a secret (this fits with how the Ushahidi
        # handles the lack of a secret).
        self._secret = self.config.get('smssync_secret', '')

    def secret_for_request(self, request):
        return self._secret

    def secret_for_message(self, msg):
        return self._secret


class MultiSmsSync(BaseSmsSyncTransport):
    """
    Ushahidi SMSSync Transport for a multiple phones.
    """

    def secret_for_request(self, request):
        pathparts = request.path.split('/')
        if pathparts:
            return pathparts[-1]
        return None

    def secret_for_message(self, msg):
        return msg['transport_metadata'].get('smssync_secret')
