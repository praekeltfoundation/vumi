import re
import json
import redis

from twisted.python import log
from twisted.internet import reactor, defer
from twisted.internet.protocol import Protocol, ReconnectingClientFactory
from twisted.internet.task import LoopingCall

from smpp.pdu_builder import *
from smpp.pdu_inspector import *

from vumi.utils import *


class EsmeTransceiver(Protocol):

    def __init__(self, seq, config, vumi_options):
        self.name = 'Proto' + str(seq)
        log.msg('__init__', self.name)
        self.defaults = {}
        self.state = 'CLOSED'
        log.msg(self.name, 'STATE :', self.state)
        self.seq = seq
        self.config = config
        self.vumi_options = vumi_options
        self.inc = int(self.config['smpp_increment'])
        self.datastream = ''
        self.__connect_callback = None
        self.__submit_sm_resp_callback = None
        self.__delivery_report_callback = None
        self.__deliver_sm_callback = None

        self.r_server = redis.Redis("localhost",
                db=get_deploy_int(self.vumi_options['vhost']))
        log.msg("Connected to Redis")
        self.r_prefix = "%s@%s:%s" % (
                self.config['system_id'],
                self.config['host'],
                self.config['port'])
        log.msg("r_prefix = %s" % self.r_prefix)


    def set_handler(self, handler):
        self.handler = handler


    def getSeq(self):
        return self.seq[0]


    def incSeq(self):
        self.seq[0] += self.inc


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
        log.msg('INCOMING <<<<', pdu)
        if pdu['header']['command_id'] == 'bind_transceiver_resp':
            self.handle_bind_transceiver_resp(pdu)
        if pdu['header']['command_id'] == 'submit_sm_resp':
            self.handle_submit_sm_resp(pdu)
        if pdu['header']['command_id'] == 'submit_multi_resp':
            self.handle_submit_multi_resp(pdu)
        if pdu['header']['command_id'] == 'deliver_sm':
            self.handle_deliver_sm(pdu)
        if pdu['header']['command_id'] == 'enquire_link_resp':
            self.handle_enquire_link_resp(pdu)
        log.msg(self.name, 'STATE :', self.state)


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


    def setLoopingQuerySMCallback(self, looping_query_sm_callback):
        self.__looping_query_sm_callback = looping_query_sm_callback


    def connectionMade(self):
        self.state = 'OPEN'
        log.msg(self.name, 'STATE :', self.state)
        pdu = BindTransceiver(self.getSeq(), **self.defaults)
        log.msg(pdu.get_obj())
        self.incSeq()
        self.sendPDU(pdu)


    def connectionLost(self, *args, **kwargs):
        self.state = 'CLOSED'
        log.msg(self.name, 'STATE :', self.state)
        try:
            self.lc_enquire.stop()
            del self.lc_enquire
            log.msg(self.name, 'stop & del enquire link looping call')
        except:
            pass
        #try:
            #self.lc_query.stop()
            #del self.lc_query
            #print self.name, 'stop & del query sm looping call'
        #except:
            #pass


    def dataReceived(self, data):
        self.datastream += data
        data = self.popData()
        while data != None:
            self.handleData(data)
            data = self.popData()


    def sendPDU(self, pdu):
        data = pdu.get_bin()
        log.msg('OUTGOING >>>>', unpack_pdu(data))

        self.transport.write(data)


    def handle_bind_transceiver_resp(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            self.state = 'BOUND_TRX'
            self.lc_enquire = LoopingCall(self.enquire_link)
            self.lc_enquire.start(55.0)
            #self.lc_query = LoopingCall(self.query_sm_group)
            #self.lc_query.start(1.0)
            self.__connect_callback(self)
        log.msg(self.name, 'STATE :', self.state)


    def handle_submit_sm_resp(self, pdu):
        message_id = pdu.get('body',{}).get('mandatory_parameters',{}).get('message_id')
        self.__submit_sm_resp_callback(
                sequence_number = pdu['header']['sequence_number'],
                command_status = pdu['header']['command_status'],
                command_id = pdu['header']['command_id'],
                message_id = message_id)
        if pdu['header']['command_status'] == 'ESME_ROK':
            pass


    def handle_submit_multi_resp(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            pass


    def handle_deliver_sm(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            sequence_number = pdu['header']['sequence_number']
            pdu_resp = DeliverSMResp(sequence_number, **self.defaults)
            self.sendPDU(pdu_resp)
            delivery_report = re.search( # SMPP v3.4 Issue 1.2 pg. 167 is wrong on id length
                       'id:(?P<id>\S{,65}) +sub:(?P<sub>...)'
                    +' +dlvrd:(?P<dlvrd>...)'
                    +' +submit date:(?P<submit_date>\d*)'
                    +' +done date:(?P<done_date>\d*)'
                    +' +stat:(?P<stat>[A-Z]{7})'
                    +' +err:(?P<err>...)'
                    +' +[Tt]ext:(?P<text>.{,20})'
                    +'.*',
                    pdu['body']['mandatory_parameters']['short_message']
                    )
            if delivery_report:
                self.__delivery_report_callback(
                        destination_addr = pdu['body']['mandatory_parameters']['destination_addr'],
                        source_addr = pdu['body']['mandatory_parameters']['source_addr'],
                        delivery_report = delivery_report.groupdict()
                        )
            elif detect_multipart(pdu):
                redis_key = "%s#multi_%s" % (self.r_prefix, multipart_key(detect_multipart(pdu)))
                log.msg("Redis multipart key: %s" % (redis_key))
                value = json.loads(self.r_server.get(redis_key) or 'null')
                log.msg("Retrieved value: %s" % (repr(value)))
                multi = MultipartMessage(value)
                multi.add_pdu(pdu)
                completed = multi.get_completed()
                if completed:
                    self.r_server.delete(redis_key)
                    log.msg("Re-assembled Message: %s" % (completed['message']))
                    # and we can finally pass the whole message on
                    self.__deliver_sm_callback(
                            destination_addr = completed['to_msisdn'],
                            source_addr = completed['from_msisdn'],
                            short_message = completed['message']
                            )
                else:
                    self.r_server.set(redis_key, json.dumps(multi.get_array()))
            else:
                self.__deliver_sm_callback(
                        destination_addr = pdu['body']['mandatory_parameters']['destination_addr'],
                        source_addr = pdu['body']['mandatory_parameters']['source_addr'],
                        short_message = pdu['body']['mandatory_parameters']['short_message']
                        )


    def handle_enquire_link_resp(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            pass


    def submit_sm(self, **kwargs):
        if self.state in ['BOUND_TX', 'BOUND_TRX']:
            sequence_number = self.getSeq()
            pdu = SubmitSM(sequence_number, **dict(self.defaults, **kwargs))
            self.incSeq()
            self.sendPDU(pdu)
            return sequence_number
        return 0


    def submit_multi(self, dest_address=[], **kwargs):
        if self.state in ['BOUND_TX', 'BOUND_TRX']:
            sequence_number = self.getSeq()
            pdu = SubmitMulti(sequence_number, **dict(self.defaults, **kwargs))
            for item in dest_address:
                if isinstance(item, str): # assume strings are addresses not lists
                    pdu.addDestinationAddress(
                            item,
                            dest_addr_ton = self.defaults['dest_addr_ton'],
                            dest_addr_npi = self.defaults['dest_addr_npi'],
                            )
                elif isinstance(item, dict):
                    if item.get('dest_flag') == 1:
                        pdu.addDestinationAddress(
                                item.get('destination_addr', ''),
                                dest_addr_ton = item.get('dest_addr_ton',
                                    self.defaults['dest_addr_ton']),
                                dest_addr_npi = item.get('dest_addr_npi',
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
                    message_id = message_id,
                    source_addr = source_addr,
                    **dict(self.defaults, **kwargs))
            self.incSeq()
            self.sendPDU(pdu)
            return sequence_number
        return 0


    def query_sm_group(self, **kwargs):
        self.__looping_query_sm_callback()
        return 0


class EsmeTransceiverFactory(ReconnectingClientFactory):

    def __init__(self, config, vumi_options):
        self.config = config
        self.vumi_options = vumi_options
        if int(self.config['smpp_increment']) < int(self.config['smpp_offset']):
            raise Exception("increment may not be less than offset")
        if int(self.config['smpp_increment']) < 1:
            raise Exception("increment may not be less than 1")
        if int(self.config['smpp_offset']) < 1:
            raise Exception("offset may not be less than 1")
        self.esme = None
        self.__connect_callback = None
        self.__disconnect_callback = None
        self.__submit_sm_resp_callback = None
        self.__delivery_report_callback = None
        self.__deliver_sm_callback = None
        self.__looping_query_sm_callback = None
        self.seq = [int(self.config['smpp_offset'])]
        log.msg("Set sequence number: %s, config: %s" % (self.seq, self.config))
        self.initialDelay = 30.0
        self.maxDelay = 45
        self.defaults = {
                'host':'127.0.0.1',
                'port':2775,
                'dest_addr_ton':0,
                'dest_addr_npi':0,
                }


    def loadDefaults(self, defaults):
        self.defaults = dict(self.defaults, **defaults)


    def setLatestSequenceNumber(self, latest):
        self.seq = [latest]
        log.msg("Set sequence number: %s, config: %s" % (self.seq, self.config))


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


    def setLoopingQuerySMCallback(self, looping_query_sm_callback):
        self.__looping_query_sm_callback = looping_query_sm_callback


    def startedConnecting(self, connector):
        print 'Started to connect.'


    def buildProtocol(self, addr):
        print 'Connected'
        self.esme = EsmeTransceiver(self.seq, self.config, self.vumi_options)
        self.esme.loadDefaults(self.defaults)
        self.esme.setConnectCallback(
                connect_callback = self.__connect_callback)
        self.esme.setSubmitSMRespCallback(
                submit_sm_resp_callback = self.__submit_sm_resp_callback)
        self.esme.setDeliveryReportCallback(
                delivery_report_callback = self.__delivery_report_callback)
        self.esme.setDeliverSMCallback(
                deliver_sm_callback = self.__deliver_sm_callback)
        self.esme.setLoopingQuerySMCallback(
                looping_query_sm_callback = self.__looping_query_sm_callback)
        self.resetDelay()
        return self.esme


    def clientConnectionLost(self, connector, reason):
        print 'Lost connection.  Reason:', reason
        self.__disconnect_callback()
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)


    def clientConnectionFailed(self, connector, reason):
        print 'Connection failed. Reason:', reason
        ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)


