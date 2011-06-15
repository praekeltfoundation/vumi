
from smpp.pdu_builder import *
from twisted.trial.unittest import TestCase

tlv = DeliverSM(1, short_message='the first message part')
sar = DeliverSM(1, short_message='\x00\x03\xff\x02\x01the first message part')
csm = DeliverSM(1, short_message='\x05\x00\x03\xff\x02\x01the first message part')
csm16 = DeliverSM(1, short_message='\x06\x00\x04\xff\xff\x02\x01the first message part')



def detect_multipart(pdu):
    short_message = pdu.get_obj()['body']['mandatory_parameters']['short_message']

    print repr(pdu.get_obj())

    if (short_message[0:1] == '\x00'
    and short_message[1:2] == '\x03'
    and len(short_message) >= 5):
        mdict = {'multipart_type':'SAR'}
        mdict['reference_number'] = int(binascii.b2a_hex(short_message[2:3]), 16)
        mdict['total_number'] = int(binascii.b2a_hex(short_message[3:4]), 16)
        mdict['part_number'] = int(binascii.b2a_hex(short_message[4:5]), 16)
        mdict['part_message'] = short_message[5:]
        return mdict


    if (short_message[0:1] == '\x05'
    and short_message[1:2] == '\x00'
    and short_message[2:3] == '\x03'
    and len(short_message) >= 6):
        mdict = {'multipart_type':'CSM'}
        mdict['reference_number'] = int(binascii.b2a_hex(short_message[3:4]), 16)
        mdict['total_number'] = int(binascii.b2a_hex(short_message[4:5]), 16)
        mdict['part_number'] = int(binascii.b2a_hex(short_message[5:6]), 16)
        mdict['part_message'] = short_message[6:]
        return mdict

    if (short_message[0:1] == '\x06'
    and short_message[1:2] == '\x00'
    and short_message[2:3] == '\x04'
    and len(short_message) >= 7):
        mdict = {'multipart_type':'CSM16'}
        mdict['reference_number'] = int(binascii.b2a_hex(short_message[3:5]), 16)
        mdict['total_number'] = int(binascii.b2a_hex(short_message[5:6]), 16)
        mdict['part_number'] = int(binascii.b2a_hex(short_message[6:7]), 16)
        mdict['part_message'] = short_message[7:]
        return mdict

    return None




print '\n', detect_multipart(sar)
print '\n', detect_multipart(csm)
print '\n', detect_multipart(csm16)
