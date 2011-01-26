
from twisted.internet import reactor, defer
from twisted.internet.protocol import Protocol, ReconnectingClientFactory
from twisted.internet.task import LoopingCall

from smpp.pdu_builder import *


class EsmeTransceiver(Protocol):

    def __init__(self, seq=[1]):
        self.name = 'Proto' + str(seq)
        print '__init__', self.name
        self.defaults = {}
        self.state = 'CLOSED'
        print self.name, 'STATE :', self.state
        self.seq = seq
        self.datastream = ''
        self.__connect_callback = None
        self.__submit_sm_resp_callback = None
        self.__deliver_sm_callback = None


    def set_handler(self, handler):
        self.handler = handler


    def getSeq(self):
        return self.seq[0]


    def incSeq(self):
        self.seq[0] +=1


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
        print 'INCOMING <<<<', pdu
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
        print self.name, 'STATE :', self.state


    def loadDefaults(self, defaults):
        self.defaults = dict(self.defaults, **defaults)


    def setConnectCallback(self, connect_callback):
        self.__connect_callback = connect_callback


    def setSubmitSMRespCallback(self, submit_sm_resp_callback):
        self.__submit_sm_resp_callback = submit_sm_resp_callback


    def setDeliverSMCallback(self, deliver_sm_callback):
        self.__deliver_sm_callback = deliver_sm_callback


    def connectionMade(self):
        self.state = 'OPEN'
        print self.name, 'STATE :', self.state
        pdu = BindTransceiver(self.getSeq(), **self.defaults)
        print pdu.get_obj()
        self.incSeq()
        self.sendPDU(pdu)


    def connectionLost(self, *args, **kwargs):
        self.state = 'CLOSED'
        print self.name, 'STATE :', self.state
        try:
            self.lc_enquire.stop()
            del self.lc_enquire
            print self.name, 'stop & del enquire link looping call'
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
        print 'OUTGOING >>>>', unpack_pdu(data)
        self.transport.write(data)


    def handle_bind_transceiver_resp(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            self.state = 'BOUND_TRX'
            self.lc_enquire = LoopingCall(self.enquire_link)
            self.lc_enquire.start(55.0)
            #self.submit_sm(
                    #short_message = 'Hello from twisted-smpp',
                    #destination_addr = '27999123456',
                    #)
            self.__connect_callback(self)
        print self.name, 'STATE :', self.state


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
        #self.__deliver_sm_callback(pdu = pdu)


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


class EsmeTransceiverFactory(ReconnectingClientFactory):

    def __init__(self):
        self.esme = None
        self.__connect_callback = None
        self.__disconnect_callback = None
        self.__submit_sm_resp_callback = None
        self.__deliver_sm_callback = None
        self.seq = [1]
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


    def setSequenceNumber(self, sequence_number):
        self.seq = [sequence_number]


    def setConnectCallback(self, connect_callback):
        self.__connect_callback = connect_callback


    def setDisconnectCallback(self, disconnect_callback):
        self.__disconnect_callback = disconnect_callback


    def setSubmitSMRespCallback(self, submit_sm_resp_callback):
        self.__submit_sm_resp_callback = submit_sm_resp_callback


    def setDeliverSMCallback(self, deliver_sm_callback):
        self.__deliver_sm_callback = deliver_sm_callback


    def startedConnecting(self, connector):
        print 'Started to connect.'


    def buildProtocol(self, addr):
        print 'Connected'
        self.esme = EsmeTransceiver(self.seq)
        self.esme.factory = self
        self.esme.loadDefaults(self.defaults)
        self.esme.setConnectCallback(
                connect_callback = self.__connect_callback)
        self.esme.setSubmitSMRespCallback(
                submit_sm_resp_callback = self.__submit_sm_resp_callback)
        self.esme.setDeliverSMCallback(
                deliver_sm_callback = self.__deliver_sm_callback)
        self.resetDelay()
        return self.esme


    def clientConnectionLost(self, connector, reason):
        print 'Lost connection.  Reason:', reason
        self.__disconnect_callback()
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)


    def clientConnectionFailed(self, connector, reason):
        print 'Connection failed. Reason:', reason
        ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)


