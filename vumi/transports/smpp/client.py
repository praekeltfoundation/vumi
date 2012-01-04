# -*- test-case-name: vumi.transports.smpp.test.test_client -*-

import re
import json
import uuid
import redis

from twisted.python import log
from twisted.internet import reactor
from twisted.internet.protocol import Protocol, ReconnectingClientFactory
from twisted.internet.task import LoopingCall

import binascii
from smpp.pdu import unpack_pdu
from smpp.pdu_builder import (BindTransceiver,
                                DeliverSMResp,
                                SubmitSM,
                                SubmitMulti,
                                EnquireLink,
                                EnquireLinkResp,
                                QuerySM,
                                )
from smpp.pdu_inspector import (MultipartMessage,
                                detect_multipart,
                                multipart_key,
                                )

from vumi.utils import get_deploy_int


# TODO this will move to pdu_inspector in python-smpp
ESME_command_status_map = {
    "ESME_ROK": "No Error",
    "ESME_RINVMSGLEN": "Message Length is invalid",
    "ESME_RINVCMDLEN": "Command Length is invalid",
    "ESME_RINVCMDID": "Invalid Command ID",
    "ESME_RINVBNDSTS": "Incorrect BIND Status for given command",
    "ESME_RALYBND": "ESME Already in Bound State",
    "ESME_RINVPRTFLG": "Invalid Priority Flag",
    "ESME_RINVREGDLVFLG": "Invalid Registered Delivery Flag",
    "ESME_RSYSERR": "System Error",
    "ESME_RINVSRCADR": "Invalid Source Address",
    "ESME_RINVDSTADR": "Invalid Dest Addr",
    "ESME_RINVMSGID": "Message ID is invalid",
    "ESME_RBINDFAIL": "Bind Failed",
    "ESME_RINVPASWD": "Invalid Password",
    "ESME_RINVSYSID": "Invalid System ID",
    "ESME_RCANCELFAIL": "Cancel SM Failed",
    "ESME_RREPLACEFAIL": "Replace SM Failed",
    "ESME_RMSGQFUL": "Message Queue Full",
    "ESME_RINVSERTYP": "Invalid Service Type",
    "ESME_RINVNUMDESTS": "Invalid number of destinations",
    "ESME_RINVDLNAME": "Invalid Distribution List name",
    "ESME_RINVDESTFLAG": "Destination flag is invalid (submit_multi)",
    "ESME_RINVSUBREP": "Invalid 'submit with replace' request (i.e. submit_sm"
                       " with replace_if_present_flag set)",
    "ESME_RINVESMCLASS": "Invalid esm_class field data",
    "ESME_RCNTSUBDL": "Cannot Submit to Distribution List",
    "ESME_RSUBMITFAIL": "submit_sm or submit_multi failed",
    "ESME_RINVSRCTON": "Invalid Source address TON",
    "ESME_RINVSRCNPI": "Invalid Source address NPI",
    "ESME_RINVDSTTON": "Invalid Destination address TON",
    "ESME_RINVDSTNPI": "Invalid Destination address NPI",
    "ESME_RINVSYSTYP": "Invalid system_type field",
    "ESME_RINVREPFLAG": "Invalid replace_if_present flag",
    "ESME_RINVNUMMSGS": "Invalid number of messages",
    "ESME_RTHROTTLED": "Throttling error (ESME has exceeded allowed message"
                       " limits)",
    "ESME_RINVSCHED": "Invalid Scheduled Delivery Time",
    "ESME_RINVEXPIRY": "Invalid message validity period (Expiry time)",
    "ESME_RINVDFTMSGID": "Predefined Message Invalid or Not Found",
    "ESME_RX_T_APPN": "ESME Receiver Temporary App Error Code",
    "ESME_RX_P_APPN": "ESME Receiver Permanent App Error Code",
    "ESME_RX_R_APPN": "ESME Receiver Reject Message Error Code",
    "ESME_RQUERYFAIL": "query_sm request failed",
    "ESME_RINVOPTPARSTREAM": "Error in the optional part of the PDU Body.",
    "ESME_ROPTPARNOTALLWD": "Optional Parameter not allowed",
    "ESME_RINVPARLEN": "Invalid Parameter Length.",
    "ESME_RMISSINGOPTPARAM": "Expected Optional Parameter missing",
    "ESME_RINVOPTPARAMVAL": "Invalid Optional Parameter Value",
    "ESME_RDELIVERYFAILURE": "Delivery Failure (used for data_sm_resp)",
    "ESME_RUNKNOWNERR": "Unknown Error",
}


class EsmeTransceiver(Protocol):

    callLater = reactor.callLater

    def __init__(self, seq, config, vumi_options):
        self.build_maps()
        self.name = 'Proto' + str(seq)
        log.msg('__init__ %s' % self.name)
        self.defaults = {}
        self.state = 'CLOSED'
        log.msg('%s STATE: %s' % (self.name, self.state))
        self.seq = seq
        self.config = config
        self.vumi_options = vumi_options
        self.inc = int(self.config['smpp_increment'])
        self.smpp_bind_timeout = int(self.config.get('smpp_bind_timeout', 30))
        self.incSeq()
        self.datastream = ''
        self.__connect_callback = None
        self.__submit_sm_resp_callback = None
        self.__delivery_report_callback = None
        self.__deliver_sm_callback = None
        self._send_failure_callback = None
        self.error_handlers = {
                "ok": self.dummy_ok,
                "mess_permfault": self.dummy_mess_permfault,
                "mess_tempfault": self.dummy_mess_tempfault,
                "conn_permfault": self.dummy_conn_permfault,
                "conn_tempfault": self.dummy_conn_tempfault,
                "conn_throttle": self.dummy_conn_throttle,
                "unknown": self.dummy_unknown,
                }
        self.r_server = redis.Redis("localhost",
                db=get_deploy_int(self.vumi_options['vhost']))
        log.msg("Connected to Redis")
        self.r_prefix = "%s@%s:%s" % (
                self.config['system_id'],
                self.config['host'],
                self.config['port'])
        log.msg("r_prefix = %s" % self.r_prefix)

    # Dummy error handler functions, just log invocation
    def dummy_ok(self, *args, **kwargs):
        pass

    # Dummy error handler functions, just log invocation
    def dummy_mess_permfault(self, *args, **kwargs):
            m = "%s.%s(*args=%s, **kwargs=%s)" % (
                __name__,
                "dummy_mess_permfault",
                args,
                kwargs)
            log.msg(m)

    # Dummy error handler functions, just log invocation
    def dummy_mess_tempfault(self, *args, **kwargs):
            m = "%s.%s(*args=%s, **kwargs=%s)" % (
                __name__,
                "dummy_mess_tempfault",
                args,
                kwargs)
            log.msg(m)

    # Dummy error handler functions, just log invocation
    def dummy_conn_permfault(self, *args, **kwargs):
            m = "%s.%s(*args=%s, **kwargs=%s)" % (
                __name__,
                "dummy_conn_permfault",
                args,
                kwargs)
            log.msg(m)

    # Dummy error handler functions, just log invocation
    def dummy_conn_tempfault(self, *args, **kwargs):
            m = "%s.%s(*args=%s, **kwargs=%s)" % (
                __name__,
                "dummy_conn_tempfault",
                args,
                kwargs)
            log.msg(m)

    # Dummy error handler functions, just log invocation
    def dummy_conn_throttle(self, *args, **kwargs):
            m = "%s.%s(*args=%s, **kwargs=%s)" % (
                __name__,
                "dummy_conn_throttle",
                args,
                kwargs)
            log.msg(m)

    # Dummy error handler functions, just log invocation
    def dummy_unknown(self, *args, **kwargs):
            m = "%s.%s(*args=%s, **kwargs=%s)" % (
                __name__,
                "dummy_unknown",
                args,
                kwargs)
            log.msg(m)

    def build_maps(self):
        self.ESME_command_status_dispatch_map = {
            "ESME_ROK": self.dispatch_ok,
            "ESME_RINVMSGLEN": self.dispatch_mess_permfault,
            "ESME_RINVCMDLEN": self.dispatch_mess_permfault,
            "ESME_RINVCMDID": self.dispatch_mess_permfault,

            "ESME_RINVBNDSTS": self.dispatch_conn_tempfault,
            "ESME_RALYBND": self.dispatch_conn_tempfault,

            "ESME_RINVPRTFLG": self.dispatch_mess_permfault,
            "ESME_RINVREGDLVFLG": self.dispatch_mess_permfault,

            "ESME_RSYSERR": self.dispatch_conn_permfault,

            "ESME_RINVSRCADR": self.dispatch_mess_permfault,
            "ESME_RINVDSTADR": self.dispatch_mess_permfault,
            "ESME_RINVMSGID": self.dispatch_mess_permfault,

            "ESME_RBINDFAIL": self.dispatch_conn_permfault,
            "ESME_RINVPASWD": self.dispatch_conn_permfault,
            "ESME_RINVSYSID": self.dispatch_conn_permfault,

            "ESME_RCANCELFAIL": self.dispatch_mess_permfault,
            "ESME_RREPLACEFAIL": self.dispatch_mess_permfault,

            "ESME_RMSGQFUL": self.dispatch_conn_throttle,

            "ESME_RINVSERTYP": self.dispatch_conn_permfault,

            "ESME_RINVNUMDESTS": self.dispatch_mess_permfault,
            "ESME_RINVDLNAME": self.dispatch_mess_permfault,
            "ESME_RINVDESTFLAG": self.dispatch_mess_permfault,
            "ESME_RINVSUBREP": self.dispatch_mess_permfault,
            "ESME_RINVESMCLASS": self.dispatch_mess_permfault,
            "ESME_RCNTSUBDL": self.dispatch_mess_permfault,

            "ESME_RSUBMITFAIL": self.dispatch_mess_tempfault,

            "ESME_RINVSRCTON": self.dispatch_mess_permfault,
            "ESME_RINVSRCNPI": self.dispatch_mess_permfault,
            "ESME_RINVDSTTON": self.dispatch_mess_permfault,
            "ESME_RINVDSTNPI": self.dispatch_mess_permfault,

            "ESME_RINVSYSTYP": self.dispatch_conn_permfault,

            "ESME_RINVREPFLAG": self.dispatch_mess_permfault,

            "ESME_RINVNUMMSGS": self.dispatch_mess_tempfault,

            "ESME_RTHROTTLED": self.dispatch_conn_throttle,

            "ESME_RINVSCHED": self.dispatch_mess_permfault,
            "ESME_RINVEXPIRY": self.dispatch_mess_permfault,
            "ESME_RINVDFTMSGID": self.dispatch_mess_permfault,

            "ESME_RX_T_APPN": self.dispatch_mess_tempfault,

            "ESME_RX_P_APPN": self.dispatch_mess_permfault,
            "ESME_RX_R_APPN": self.dispatch_mess_permfault,
            "ESME_RQUERYFAIL": self.dispatch_mess_permfault,
            "ESME_RINVOPTPARSTREAM": self.dispatch_mess_permfault,
            "ESME_ROPTPARNOTALLWD": self.dispatch_mess_permfault,
            "ESME_RINVPARLEN": self.dispatch_mess_permfault,
            "ESME_RMISSINGOPTPARAM": self.dispatch_mess_permfault,
            "ESME_RINVOPTPARAMVAL": self.dispatch_mess_permfault,

            "ESME_RDELIVERYFAILURE": self.dispatch_mess_tempfault,
            "ESME_RUNKNOWNERR": self.dispatch_mess_tempfault,
        }

    def command_status_dispatch(self, pdu):
        method = self.ESME_command_status_dispatch_map.get(
                pdu['header']['command_status'],
                self.dispatch_unknown)
        handler = method()
        if pdu['header']['command_status'] != "ESME_ROK":
            log.msg("ERROR handler:%s pdu:%s" % (handler, pdu))
        return handler

    # This maps SMPP error states to VUMI error states
    # For now assume VUMI understands:
    # connection -> temp fault or permanent fault
    # message -> temp fault or permanent fault
    # and the need to throttle the traffic on the connection
    def dispatch_ok(self):
        return self.error_handlers.get("ok")

    def dispatch_conn_permfault(self):
        return self.error_handlers.get("conn_permfault")

    def dispatch_mess_permfault(self):
        return self.error_handlers.get("mess_permfault")

    def dispatch_conn_tempfault(self):
        return self.error_handlers.get("conn_tempfault")

    def dispatch_mess_tempfault(self):
        return self.error_handlers.get("mess_tempfault")

    def dispatch_conn_throttle(self):
        return self.error_handlers.get("conn_throttle")

    def update_error_handlers(self, handler_dict={}):
        self.error_handlers.update(handler_dict)

    def dispatch_unknown(self):
        return self.error_handlers.get("unknown")

    def getSeq(self):
        return self.seq[0]

    # TODO From VUMI 0.4 onwards incSeq and smpp_offset/smpp_increment
    # will fall away and getSeq will run off the Redis incr function
    # with one shared value per system_id@host:port account credential
    def incSeq(self):
        self.seq[0] += self.inc
        # SMPP supports a max sequence_number of: FFFFFFFF = 4,294,967,295
        # so start recycling @ 4,000,000,000 just to keep the numbers round
        if self.seq[0] > 4000000000:
            self.seq[0] = self.seq[0] % self.inc + self.inc

    def popData(self):
        data = None
        if(len(self.datastream) >= 16):
            command_length = int(binascii.b2a_hex(self.datastream[0:4]), 16)
            if(len(self.datastream) >= command_length):
                data = self.datastream[0:command_length]
                self.datastream = self.datastream[command_length:]
        return data

    def handleData(self, data):
        pdu = unpack_pdu(data)
        log.msg('INCOMING <<<< %s' % binascii.b2a_hex(data))
        log.msg('INCOMING <<<< %s' % pdu)
        error_handler = self.command_status_dispatch(pdu)
        error_handler(pdu=pdu)
        if pdu['header']['command_id'] == 'bind_transceiver_resp':
            self.handle_bind_transceiver_resp(pdu)
        if pdu['header']['command_id'] == 'submit_sm_resp':
            self.handle_submit_sm_resp(pdu)
        if pdu['header']['command_id'] == 'submit_multi_resp':
            self.handle_submit_multi_resp(pdu)
        if pdu['header']['command_id'] == 'deliver_sm':
            self.handle_deliver_sm(pdu)
        if pdu['header']['command_id'] == 'enquire_link':
            self.handle_enquire_link(pdu)
        if pdu['header']['command_id'] == 'enquire_link_resp':
            self.handle_enquire_link_resp(pdu)
        log.msg('%s STATE: %s' % (self.name, self.state))

    def loadDefaults(self, defaults):
        self.defaults = dict(self.defaults, **defaults)

    def setConnectCallback(self, connect_callback):
        self.__connect_callback = connect_callback

    def setSubmitSMRespCallback(self, submit_sm_resp_callback):
        self.__submit_sm_resp_callback = submit_sm_resp_callback

    def setDeliveryReportCallback(self, delivery_report_callback):
        self.__delivery_report_callback = delivery_report_callback

    def setDeliverSMCallback(self, deliver_sm_callback):
        self.__deliver_sm_callback = deliver_sm_callback

    def setSendFailureCallback(self, send_failure_callback):
        self._send_failure_callback = send_failure_callback

    def connectionMade(self):
        self.state = 'OPEN'
        log.msg('%s STATE: %s' % (self.name, self.state))
        pdu = BindTransceiver(self.getSeq(), **self.defaults)
        log.msg(pdu.get_obj())
        self.incSeq()
        self.sendPDU(pdu)
        self._lose_conn = self.callLater(
            self.smpp_bind_timeout, self.lose_unbound_connection, 'BOUND_TRX')

    def lose_unbound_connection(self, required_state):
        if self.state != required_state:
            log.msg('Breaking connection due to binding delay, %s != %s\n' % (
                self.state, required_state))
            self._lose_conn = None
            self.transport.loseConnection()

    def connectionLost(self, *args, **kwargs):
        self.state = 'CLOSED'
        log.msg('%s STATE: %s' % (self.name, self.state))
        try:
            self.lc_enquire.stop()
            del self.lc_enquire
            log.msg('%s stop & del enquire link looping call' % self.name)
        except:
            pass

    def dataReceived(self, data):
        self.datastream += data
        data = self.popData()
        while data != None:
            self.handleData(data)
            data = self.popData()

    def sendPDU(self, pdu):
        data = pdu.get_bin()
        log.msg('OUTGOING >>>> %s' % unpack_pdu(data))
        self.transport.write(data)

    def handle_bind_transceiver_resp(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            self.state = 'BOUND_TRX'
            self.lc_enquire = LoopingCall(self.enquire_link)
            self.lc_enquire.start(55.0)
            if self._lose_conn is not None:
                self._lose_conn.cancel()
                self._lose_conn = None
            self.__connect_callback(self)
        log.msg('%s STATE: %s' % (self.name, self.state))

    def handle_submit_sm_resp(self, pdu):
        self.pop_unacked()
        message_id = pdu.get('body', {}).get(
                'mandatory_parameters', {}).get('message_id')
        self.__submit_sm_resp_callback(
                sequence_number=pdu['header']['sequence_number'],
                command_status=pdu['header']['command_status'],
                command_id=pdu['header']['command_id'],
                message_id=message_id)
        if pdu['header']['command_status'] == 'ESME_ROK':
            pass

    def handle_submit_multi_resp(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            pass

    def _decode_message(self, message, data_coding):
        """
        Messages can arrive with one of a number of specified
        encodings. We only handle a subset of these.

        From the SMPP spec:

        00000000 (0) SMSC Default Alphabet
        00000001 (1) IA5(CCITTT.50)/ASCII(ANSIX3.4)
        00000010 (2) Octet unspecified (8-bit binary)
        00000011 (3) Latin1(ISO-8859-1)
        00000100 (4) Octet unspecified (8-bit binary)
        00000101 (5) JIS(X0208-1990)
        00000110 (6) Cyrllic(ISO-8859-5)
        00000111 (7) Latin/Hebrew (ISO-8859-8)
        00001000 (8) UCS2(ISO/IEC-10646)
        00001001 (9) PictogramEncoding
        00001010 (10) ISO-2022-JP(MusicCodes)
        00001011 (11) reserved
        00001100 (12) reserved
        00001101 (13) Extended Kanji JIS(X 0212-1990)
        00001110 (14) KSC5601
        00001111 (15) reserved

        Particularly problematic are the "Octet unspecified" encodings.
        """
        codec = {
            1: 'ascii',
            3: 'latin1',
            8: 'utf-16be',  # Actually UCS-2, but close enough.
            }.get(data_coding, None)
        if codec is None or message is None:
            log.msg("WARNING: Not decoding message with data_coding=%s" % (
                    data_coding,))
        else:
            try:
                return message.decode(codec)
            except Exception, e:
                log.msg("Error decoding message with data_coding=%s" % (
                        data_coding,))
                log.err(e)
        return message

    def handle_deliver_sm(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            sequence_number = pdu['header']['sequence_number']
            message_id = str(uuid.uuid4())
            pdu_resp = DeliverSMResp(sequence_number,
                    **self.defaults)
            self.sendPDU(pdu_resp)
            pdu_params = pdu['body']['mandatory_parameters']
            delivery_report = re.search(
                    # SMPP v3.4 Issue 1.2 pg. 167 is wrong on id length
                      'id:(?P<id>\S{,65}) +sub:(?P<sub>...)'
                    + ' +dlvrd:(?P<dlvrd>...)'
                    + ' +submit date:(?P<submit_date>\d*)'
                    + ' +done date:(?P<done_date>\d*)'
                    + ' +stat:(?P<stat>[A-Z]{7})'
                    + ' +err:(?P<err>...)'
                    + ' +[Tt]ext:(?P<text>.{,20})'
                    + '.*',
                    pdu_params['short_message'] or ''
                    )
            if delivery_report:
                self.__delivery_report_callback(
                        destination_addr=pdu_params['destination_addr'],
                        source_addr=pdu_params['source_addr'],
                        delivery_report=delivery_report.groupdict(),
                        )
            elif detect_multipart(pdu):
                redis_key = "%s#multi_%s" % (
                        self.r_prefix, multipart_key(detect_multipart(pdu)))
                log.msg("Redis multipart key: %s" % (redis_key))
                value = json.loads(self.r_server.get(redis_key) or 'null')
                log.msg("Retrieved value: %s" % (repr(value)))
                multi = MultipartMessage(value)
                multi.add_pdu(pdu)
                completed = multi.get_completed()
                if completed:
                    self.r_server.delete(redis_key)
                    log.msg("Reassembled Message: %s" % (completed['message']))
                    # and we can finally pass the whole message on
                    self.__deliver_sm_callback(
                            destination_addr=completed['to_msisdn'],
                            source_addr=completed['from_msisdn'],
                            short_message=completed['message'],
                            message_id=message_id,
                            )
                else:
                    self.r_server.set(redis_key, json.dumps(multi.get_array()))
            else:
                decoded_msg = self._decode_message(pdu_params['short_message'],
                                                   pdu_params['data_coding'])
                self.__deliver_sm_callback(
                        destination_addr=pdu_params['destination_addr'],
                        source_addr=pdu_params['source_addr'],
                        short_message=decoded_msg,
                        message_id=message_id,
                        )

    def handle_enquire_link(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            sequence_number = pdu['header']['sequence_number']
            pdu_resp = EnquireLinkResp(sequence_number)
            self.sendPDU(pdu_resp)

    def handle_enquire_link_resp(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            pass

    def get_unacked_count(self):
            return int(self.r_server.llen("%s#unacked" % self.r_prefix))

    def push_unacked(self, sequence_number=-1):
            self.r_server.lpush("%s#unacked" % self.r_prefix, sequence_number)
            log.msg("%s#unacked pushed to: %s" % (
                self.r_prefix, self.get_unacked_count()))

    def pop_unacked(self):
            self.r_server.lpop("%s#unacked" % self.r_prefix)
            log.msg("%s#unacked popped to: %s" % (
                self.r_prefix, self.get_unacked_count()))

    def submit_sm(self, **kwargs):
        if self.state in ['BOUND_TX', 'BOUND_TRX']:
            sequence_number = self.getSeq()
            pdu = SubmitSM(sequence_number, **dict(self.defaults, **kwargs))
            self.incSeq()
            self.sendPDU(pdu)
            self.push_unacked(sequence_number)
            return sequence_number
        return 0

    def submit_multi(self, dest_address=[], **kwargs):
        if self.state in ['BOUND_TX', 'BOUND_TRX']:
            sequence_number = self.getSeq()
            pdu = SubmitMulti(sequence_number, **dict(self.defaults, **kwargs))
            for item in dest_address:
                if isinstance(item, str):
                    # assume strings are addresses not lists
                    pdu.addDestinationAddress(
                            item,
                            dest_addr_ton=self.defaults['dest_addr_ton'],
                            dest_addr_npi=self.defaults['dest_addr_npi'],
                            )
                elif isinstance(item, dict):
                    if item.get('dest_flag') == 1:
                        pdu.addDestinationAddress(
                                item.get('destination_addr', ''),
                                dest_addr_ton=item.get('dest_addr_ton',
                                    self.defaults['dest_addr_ton']),
                                dest_addr_npi=item.get('dest_addr_npi',
                                    self.defaults['dest_addr_npi']),
                                )
                    elif item.get('dest_flag') == 2:
                        pdu.addDistributionList(item.get('dl_name'))
            self.incSeq()
            self.sendPDU(pdu)
            return sequence_number
        return 0

    def enquire_link(self, **kwargs):
        if self.state in ['BOUND_TX', 'BOUND_TRX']:
            sequence_number = self.getSeq()
            pdu = EnquireLink(sequence_number, **dict(self.defaults, **kwargs))
            self.incSeq()
            self.sendPDU(pdu)
            return sequence_number
        return 0

    def query_sm(self, message_id, source_addr, **kwargs):
        if self.state in ['BOUND_TX', 'BOUND_TRX']:
            sequence_number = self.getSeq()
            pdu = QuerySM(sequence_number,
                    message_id=message_id,
                    source_addr=source_addr,
                    **dict(self.defaults, **kwargs))
            self.incSeq()
            self.sendPDU(pdu)
            return sequence_number
        return 0


class EsmeTransceiverFactory(ReconnectingClientFactory):

    def __init__(self, config, vumi_options):
        self.config = config
        self.vumi_options = vumi_options
        if int(self.config['smpp_increment']) \
                < int(self.config['smpp_offset']):
            raise Exception("increment may not be less than offset")
        if int(self.config['smpp_increment']) < 1:
            raise Exception("increment may not be less than 1")
        if int(self.config['smpp_offset']) < 0:
            raise Exception("offset may not be less than 0")
        self.esme = None
        self.__connect_callback = None
        self.__disconnect_callback = None
        self.__submit_sm_resp_callback = None
        self.__delivery_report_callback = None
        self.__deliver_sm_callback = None
        self.seq = [int(self.config['smpp_offset'])]
        log.msg("Set sequence number: %s" % (self.seq))
        self.initialDelay = float(
            self.config.get('initial_reconnect_delay', 5))
        self.maxDelay = max(45, self.initialDelay)
        self.defaults = {
                'host': '127.0.0.1',
                'port': 2775,
                'dest_addr_ton': 0,
                'dest_addr_npi': 0,
                }

    def loadDefaults(self, defaults):
        self.defaults = dict(self.defaults, **defaults)

    def setLastSequenceNumber(self, last):
        self.seq = [last]
        log.msg("Set sequence number: %s" % (self.seq))

    def setConnectCallback(self, connect_callback):
        self.__connect_callback = connect_callback

    def setDisconnectCallback(self, disconnect_callback):
        self.__disconnect_callback = disconnect_callback

    def setSubmitSMRespCallback(self, submit_sm_resp_callback):
        self.__submit_sm_resp_callback = submit_sm_resp_callback

    def setDeliveryReportCallback(self, delivery_report_callback):
        self.__delivery_report_callback = delivery_report_callback

    def setDeliverSMCallback(self, deliver_sm_callback):
        self.__deliver_sm_callback = deliver_sm_callback

    def setSendFailureCallback(self, send_failure_callback):
        self._send_failure_callback = send_failure_callback

    def startedConnecting(self, connector):
        print 'Started to connect.'

    def buildProtocol(self, addr):
        print 'Connected'
        self.esme = EsmeTransceiver(self.seq, self.config, self.vumi_options)
        self.esme.loadDefaults(self.defaults)
        self.esme.setConnectCallback(
                connect_callback=self.__connect_callback)
        self.esme.setSubmitSMRespCallback(
                submit_sm_resp_callback=self.__submit_sm_resp_callback)
        self.esme.setDeliveryReportCallback(
                delivery_report_callback=self.__delivery_report_callback)
        self.esme.setDeliverSMCallback(
                deliver_sm_callback=self.__deliver_sm_callback)
        self.resetDelay()
        return self.esme

    def clientConnectionLost(self, connector, reason):
        print 'Lost connection.  Reason:', reason
        self.__disconnect_callback()
        ReconnectingClientFactory.clientConnectionLost(
                self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        print 'Connection failed. Reason:', reason
        ReconnectingClientFactory.clientConnectionFailed(
                self, connector, reason)
