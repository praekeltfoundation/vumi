"""
Some utilities and things for testing various bits of SMPP.
"""

from twisted.internet.defer import DeferredQueue
from smpp.pdu_inspector import unpack_pdu

from vumi.transports.smpp.clientserver.server import SmscServer


class SmscTestServer(SmscServer):
    """
    SMSC subclass that records inbound and outbound PDUs for later assertion.
    """

    def __init__(self, delivery_report_string=None):
        self.pdu_queue = DeferredQueue()
        SmscServer.__init__(self, delivery_report_string)

    def handleData(self, data):
        self.pdu_queue.put({
                'direction': 'inbound',
                'pdu': unpack_pdu(data),
                })
        return SmscServer.handleData(self, data)

    def sendPDU(self, pdu):
        self.pdu_queue.put({
                'direction': 'outbound',
                'pdu': pdu.get_obj(),
                })
        return SmscServer.sendPDU(self, pdu)
