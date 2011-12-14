# -*- test-case-name: vumi.transports.gsm.tests.test_gsm -*-
# -*- coding: utf-8 -*-
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.threads import deferToThread
from twisted.internet.task import LoopingCall
from twisted.python import log
from twisted.python.failure import Failure
from vumi.transports.base import Transport
from vumi.utils import get_deploy_int, normalize_msisdn
from vumi.message import TransportUserMessage
from uuid import uuid4
import gammu
import redis


class GSMTransport(Transport):
    """
    GSM Transport for use with Gammu_.
    If a `gammu` section is provided in the configuration file then that will
    be used, if not then gammu will read `~/.gammurc` as per the Gammu docs.

    Sample config file::

        transport_name: gsm_transport
        poll_interval: 120 # seconds, defaults to 60
        country_code: 27 # the international phone number country prefix
        phone_number: 27761234567 # the phone number of the SIM
        gammu:
            UseGlobalDebugFile: 0
            DebugFile: ''
            SyncTime: 0
            Connection: 'at'
            LockDevice: 0
            DebugLevel: ''
            Device: '/dev/cu.HUAWEIMobile-Modem'
            StartInfo: 0
            Model: ''

    .. _Gammu: http://www.wammu.eu

    """

    # the name of the list we're using in Redis to store
    # the outbound messages. We do this to ensure we don't
    # lose messages during reboots
    redis_outbound_queue = 'outbound_queue'

    # the name of the list we're using in Redis to keep
    # parts of multipart messages are they are arriving
    redis_inbound_multipart_queue = 'multipart_queue'

    # the key we're using to link MessageReferences
    # to Vumi message_id's for delivery reports
    redis_delivery_report_map = 'delivery_reports'

    # how long do we want to keep track of delivery
    # report message ids so we can accurately link
    # back to them for internal processing?
    delivery_report_expiry = 7 * 24 * 60 * 60  # 7 days

    # NOTE: not sure what to do about the <ESC> character yet.
    # valid characters for the GSM 03.38 charset
    gsm_03_38_singlebyte_charset = frozenset([
        '0', '@', 'Δ', 'SP', '0', '¡', 'P', '', 'p',
        '1', '£', '_', '!', '1', 'A', 'Q', 'a', 'q',
        '2', '$', 'Φ', '"', '2', 'B', 'R', 'b', 'r',
        '3', '¥', 'Γ', '#', '3', 'C', 'S', 'c', 's',
        '4', 'è', 'Λ', '¤', '4', 'D', 'T', 'd', 't',
        '5', 'é', 'Ω', '%', '5', 'E', 'U', 'e', 'u',
        '6', 'ù', 'Π', '&', '6', 'F', 'V', 'f', 'v',
        '7', 'ì', 'Ψ', '\'', '7', 'G', 'W', 'g', 'w',
        '8', 'ò', 'Σ', '(', '8', 'H', 'X', 'h', 'x',
        '9', 'Ç', 'Θ', ')', '9', 'I', 'Y', 'i', 'y',
        'A', 'LF', 'Ξ', '*', ':', 'J', 'Z', 'j', 'z',
        'B', 'Ø', '<ESC>', '+', ';', 'K', 'Ä', 'k', 'ä',
        'C', 'ø', 'Æ', ',', '<', 'L', 'Ö', 'l', 'ö',
        'D', 'CR', 'æ', '-', '=', 'M', 'Ñ', 'm', 'ñ',
        'E', 'Å', '', '.', '>', 'N', 'Ü', 'n', 'ü',
        'F', 'å', 'É', '/', '?', 'O', '§', 'o', 'à',
        ' ',  # space
    ])

    # GSM 03.38 characters that are prefixed with an
    # <ESC> character and are therefore double byte
    gsm_03_38_doublebyte_charset = frozenset([
        '€', '[', '\\', ']', '^', '{', '|', '}', '~',
    ])

    gsm_03_38_charset = gsm_03_38_singlebyte_charset.union(
                            gsm_03_38_doublebyte_charset)

    def is_gsm_charset(self, content):
        return set(content) <= self.gsm_03_38_charset

    def count_chars_needed(self, content):
        single_bytes = len([c for c in content
                                if c in self.gsm_03_38_singlebyte_charset])
        double_bytes = len([c for c in content
                                if c in self.gsm_03_38_doublebyte_charset])
        return single_bytes + (2 * double_bytes)

    def validate_config(self):
        """
        Make sure the config values are all present
        and are valid
        """
        log.msg('Validating config')
        # Map message types to callbacks that handle that message type
        self.dispatch_map = {
            'Deliver': self.receive_message,
            'Status_Report': self.receive_delivery_report,
        }

        self.redis_config = self.config.get('redis', {})
        self.gammu_config = self.config.get('gammu')
        self.poll_interval = int(self.config.get('poll_interval', 60))
        self.country_code = str(self.config.get('country_code'))
        self.phone_number = str(self.config.get('phone_number'))
        self.phone = None

    @inlineCallbacks
    def setup_transport(self):
        log.msg('Setting up transport')
        dbindex = get_deploy_int(self._amqp_client.vhost)
        redis_config = self.config.get('redis', {})
        self.r_server = yield redis.Redis(db=dbindex, **redis_config)
        self.r_prefix = "%(transport_name)s" % self.config
        self.start_polling()

    def r_key(self, *key):
        return ':'.join([self.r_prefix] + map(str, key))

    def dr_rkey(self, *args):
        """
        Redis key for delivery reports
        """
        return self.r_key(self.redis_delivery_report_map, *map(str, args))

    def start_polling(self):
        phone = gammu.StateMachine()
        self.poller = LoopingCall(self.receive_and_send_messages, phone)
        self.poller.start(self.poll_interval, True)

    def noop(self, message):
        log.msg('Doing nothing with %s' % (message,))

    @inlineCallbacks
    def connect_phone(self, phone):
        log.msg('Connecting the phone')
        if self.gammu_config:
            phone.SetConfig(0, self.gammu_config)
        else:
            phone.ReadConfig()
        yield deferToThread(phone.Init)
        returnValue(phone)

    @inlineCallbacks
    def disconnect_phone(self, phone):
        log.msg('Disconnecting the phone')
        yield deferToThread(phone.Terminate)
        returnValue(phone)

    @inlineCallbacks
    def receive_and_send_messages(self, phone):
        log.msg('Receiving and sending messages')
        try:
            self.phone = yield self.connect_phone(phone)
            received = yield self.read_until_empty(self.phone)
            sent = yield self.send_outbound(self.phone)
            self.store_message_references(sent)
            yield self.disconnect_phone(self.phone)
            returnValue((received, sent))
        except (gammu.ERR_TIMEOUT, gammu.ERR_DEVICEOPENERROR,
                gammu.ERR_DEVICEWRITEERROR):
            log.err()
        finally:
            self.phone = None

    @inlineCallbacks
    def read_until_empty(self, phone):
        history = []
        while True:
            if history:
                last_sms = history[-1]
                sms = yield self.get_next_sms(phone, False,
                                                last_sms['Location'])
            else:
                sms = yield self.get_next_sms(phone, True)

            if not sms:
                break

            handler = self.dispatch_map.get(sms['Type'], self.noop)
            yield handler(phone, sms)
            history.append(sms)
        returnValue(history)

    @inlineCallbacks
    def get_next_sms(self, phone, start, location=0):
        # We use the flattened pseudo folder which means that all contents
        # of all folders are flattened into one 'fake' folder.
        try:
            message = yield deferToThread(phone.GetNextSMS, 0, start, location)
            # GetNextSMS has quirky behaviour where it only returns a single
            # SMS but returns it in a list, unpack here.
            if message:
                [sms] = message
                returnValue(sms)
            else:
                returnValue(None)
        except gammu.ERR_EMPTY:
            log.err('No remaining SMS messages')

    @inlineCallbacks
    def receive_message(self, phone, message):
        if self.is_part_of_multipart(message):
            self.store_multipart_part(message)
            reassembled_message = self.reassemble_multipart(message)
            if reassembled_message:
                self.publish_inbound_message(reassembled_message)
        else:
            self.publish_inbound_message(message)

        yield self.delete_message(phone, message)

    def publish_inbound_message(self, message):
        self.publish_message(
            to_addr=normalize_msisdn(self.phone_number,
                        country_code=self.country_code),
            from_addr=normalize_msisdn(str(message['Number']),
                        country_code=self.country_code),
            content=message['Text'],
            transport_type='sms',
            message_id=uuid4().get_hex(),
            transport_metadata={
                # when it was received on the modem
                'received_at': message['DateTime'],
                # when it was retrieved from the modem
                'read_at': message['SMSCDateTime'],
            })

    def is_part_of_multipart(self, message):
        udh = message.get('UDH')
        if udh:
            all_parts = udh.get('AllParts')
            if all_parts:
                return all_parts > 1

    def store_multipart_part(self, message):
        key = self.r_key(self.redis_inbound_multipart_queue,
                            message['MessageReference'])
        part_number = message['UDH']['PartNumber']
        text = message['Text']
        self.r_server.hset(key, part_number, text)

    def is_multipart_complete(self, message):
        key = self.r_key(self.redis_inbound_multipart_queue,
                            message['MessageReference'])
        total_parts = message['UDH']['AllParts']
        return self.r_server.hlen(key) == total_parts

    def reassemble_multipart(self, message):
        key = self.r_key(self.redis_inbound_multipart_queue,
                            message['MessageReference'])
        if self.is_multipart_complete(message):
            parts = self.r_server.hgetall(key)
            text = ''.join([part for idx, part in sorted(parts.items())])
            message.update({
                'Text': text,
                'Length': len(text),
            })
            self.r_server.delete(key)
            return message

    def construct_gammu_messages(self, message):
        if (self.is_gsm_charset(message['content']) and
            self.count_chars_needed(message['content']) <= 160):
            return [self.construct_gammu_sms_message(message)]
        else:
            return self.construct_gammu_multipart_messages(message)

    def construct_gammu_multipart_messages(self, message):
        smsinfo = {
            'Class': 1,
            'Unicode': not self.is_gsm_charset(message['content']),
            'Entries': [{
                'ID': 'ConcatenatedTextLong',
                'Buffer': message['content'],
            }]
        }
        return gammu.EncodeSMS(smsinfo)

    def construct_gammu_sms_message(self, message):
        return {
            'Text': message['content'],
        }

    @inlineCallbacks
    def send_outbound(self, phone):
        queue_key = self.r_key(self.redis_outbound_queue)
        sent_messages = []
        while self.r_server.llen(queue_key):
            json_data = self.r_server.lpop(queue_key)
            message = TransportUserMessage.from_json(json_data)
            log.msg('Sending SMS to %s' % (message['to_addr'],))

            # keep track of which Gammu messages were sent out for
            # what Vumi message
            gammu_messages = {}
            try:
                for gammu_message in self.construct_gammu_messages(message):
                    overrides = {
                        'Number': message['to_addr'],
                        # Send using the Phone's known SMSC
                        'SMSC': {
                            'Location': 1
                        },
                        # this will create submit message with request
                        # for delivery report
                        'Type': 'Status_Report',
                    }
                    gammu_message.update(overrides)

                    # keep the message reference for delivery reports
                    send_sms_response = yield deferToThread(phone.SendSMS,
                                                            gammu_message)
                    gammu_messages[send_sms_response] = gammu_message

                    # multiparts result in multiple acks
                    yield self.publish_ack(user_message_id=message['message_id'],
                                            sent_message_id=send_sms_response)

                # collect for audit trail
                sent_messages.append((message, gammu_messages))
            except gammu.GSMError, e:
                failure = Failure(e)
                self.send_failure(message, failure.value,
                    failure.getTraceback())

        returnValue(sent_messages)

    def store_message_references(self, sent_messages):
        # FIXME: there's got to be a better way to do this, probably some
        #        Redis pattern I'm not aware of.
        for vumi_message, gammu_message_dict in sent_messages:
            for message_reference, gammu_message in gammu_message_dict.items():
                # used for looking up the message id for a message reference nr
                ref2id_key = self.dr_rkey('ref2id', message_reference)
                self.r_server.set(ref2id_key, vumi_message['message_id'])
                # used for storing the reference numbers & status for an id
                id2refs_key = self.dr_rkey('id2refs',
                                            vumi_message['message_id'])
                self.r_server.hset(id2refs_key, message_reference, 'pending')
                # set to auto expire
                self.r_server.expire(ref2id_key, self.delivery_report_expiry)
                self.r_server.expire(id2refs_key, self.delivery_report_expiry)

    def receive_delivery_report(self, phone, delivery_report):
        log.msg('Received delivery report: %s' % (repr(delivery_report),))
        # what we get from the phone
        message_reference = delivery_report['MessageReference']

        # NOTE:
        # the GSM spec is either undocumented or vastly well hidden on
        # the internet. I'm unable to find any other status's for
        # delivery reports. Making DR report handling very difficult.
        # Either that or I'm googling for the wrong stuff.
        status = {
            'Delivered': 'delivered',
        }.get(delivery_report['Text'], 'failed')

        # what vumi message this is linked to
        message_id_key = self.dr_rkey('ref2id', message_reference)
        message_id = self.r_server.get(message_id_key)
        # update this part we've just received
        parts_key = self.dr_rkey('id2refs', message_id)
        self.r_server.hset(parts_key, message_reference, status)
        # what the total parts for this outbound message are
        parts = self.r_server.hvals(parts_key)
        # if they're all delivered then issue a delivery report
        if set(parts) == set(['delivered']):
            self.publish_delivery_report(message_id, 'delivered')
        # pending is someting we set, if there are no entries
        # with pending we're assuming it's a failure since it
        # isn't delivered. If it's a temporary update then
        # at a later point in time we'll get all the 'delivered's
        # and then issue a correct delivery report.
        elif 'pending' not in parts:
            self.publish_delivery_report(message_id, status)
        self.delete_message(phone, delivery_report)

    @inlineCallbacks
    def delete_message(self, phone, message):
        log.msg('Deleting %s from %s' % (message['Location'], phone))
        yield deferToThread(phone.DeleteSMS, 0, message['Location'])

    @inlineCallbacks
    def teardown_transport(self):
        # depending on when the shutdown was issued
        # these might not've been created yet.
        poller = getattr(self, 'poller', None)
        phone = getattr(self, 'phone', None)

        if poller and poller.running:
            poller.stop()

        if phone:
            try:
                yield self.disconnect_phone(phone)
            except gammu.ERR_NOTCONNECTED:
                log.err()

    def handle_outbound_message(self, message):
        """
        Send an outbound message out as an SMS
        via the GSM Modem.

        Since we only poll the modem every `poll_interval` seconds
        there's the possibility of messages being lost when the transport
        restarts if messages have been ack'd off the queue but
        haven't been sent out through the phone yet.

        As a result we stash them in redis and whenever we have a connection
        to the phone we pull them out again and send them off.
        """
        key = self.r_key(self.redis_outbound_queue)
        self.r_server.rpush(key, message.to_json())
