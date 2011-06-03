import time
import yaml
from twisted.trial.unittest import TestCase
from vumi.workers.cellulant.worker import XMPPtoCellulantUSSDWorker, CellulantUSSDtoXMPPWorker, unpackCellulantUSSDMessage, packCellulantUSSDMessage
from vumi.message import Message

class _XMPPtoCellulantUSSDWorker(XMPPtoCellulantUSSDWorker):
    def __init__(self):pass

class _CellulantUSSDtoXMPPWorker(CellulantUSSDtoXMPPWorker):
    def __init__(self):pass

class CellulantTestCase(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_cellulant_conversions(self):

        x_to_u = _XMPPtoCellulantUSSDWorker()
        u_to_x = _CellulantUSSDtoXMPPWorker()

        kwargs = {"message":"hello", "recipient":"*360#", "sender":"254788111110"}
        m1 = Message(**kwargs)
        m2 = x_to_u.consume_message(m1)
        m3 = u_to_x.consume_message(m2)

        self.assertEquals(m1.payload,
            {'message': 'hello', 'recipient': '*360#', 'sender': '254788111110'})
        self.assertEquals(m2.payload,
            {'message': '1A3E55B|3|254788111110|hello|INVA'})
        self.assertEquals(m3.payload,
            {'message': 'hello', 'recipient': '254788111110'})


        M1 = Message(**kwargs)
        M2 = packCellulantUSSDMessage(M1)
        M3 = unpackCellulantUSSDMessage(M2)

        self.assertEquals(M1.payload,
            {'message': 'hello', 'recipient': '*360#', 'sender': '254788111110'})
        self.assertEquals(M2.payload,
            {'message': '1A3E55B|3|254788111110|hello|INVA'})
        self.assertEquals(M3.payload,
            {'message': 'hello', 'recipient': '254788111110'})
