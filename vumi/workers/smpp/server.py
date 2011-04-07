
from twisted.python import log
from twisted.internet import reactor, defer
from twisted.internet.protocol import Protocol, ServerFactory

from smpp.pdu_builder import *

class SmscServer(Protocol):

    def __init__(self):
        log.msg('__init__', 'SmscServer')
        self.datastream = ''


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
        if pdu['header']['command_id'] == 'bind_transceiver':
            self.handle_bind_transceiver(pdu)
        if pdu['header']['command_id'] == 'submit_sm':
            self.handle_submit_sm(pdu)
        #if pdu['header']['command_id'] == 'deliver_sm':
            #self.handle_deliver_sm(pdu)
        if pdu['header']['command_id'] == 'enquire_link':
            self.handle_enquire_link(pdu)
        #log.msg(self.name, 'STATE :', self.state)


    def handle_bind_transceiver(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            sequence_number = pdu['header']['sequence_number']
            system_id = system_id = pdu['body']['mandatory_parameters']['system_id']
            pdu_resp = BindTransceiverResp(sequence_number,
                    system_id = system_id
                    )
            self.sendPDU(pdu_resp)


    def handle_enquire_link(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            sequence_number = pdu['header']['sequence_number']
            pdu_resp = EnquireLinkResp(sequence_number)
            self.sendPDU(pdu_resp)
            self.handle_submit_sm(pdu)


    def handle_submit_sm(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            sequence_number = pdu['header']['sequence_number']
            message_id = "1234"
            pdu_resp = SubmitSMResp(sequence_number, message_id)
            self.sendPDU(pdu_resp)


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


class SmscServerFactory(ServerFactory):
    protocol = SmscServer
    #def buildProtocol(self, addr):
        #smsc = SmscServer()
        #return smsc


