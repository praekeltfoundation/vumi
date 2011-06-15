import operator

from smpp.pdu_builder import *
from twisted.trial.unittest import TestCase

tlv = DeliverSM(1, short_message='the first message part')
tlv.set_sar_msg_ref_num(65017)
tlv.set_sar_total_segments(22)
tlv.set_sar_segment_seqnum(21)
sar = DeliverSM(1, short_message='\x00\x03\xff\x02\x01the first message part')
csm = DeliverSM(1, short_message='\x05\x00\x03\xff\x02\x01the first message part')
csm16 = DeliverSM(1, short_message='\x06\x00\x04\xff\xff\x02\x01the first message part')



def detect_multipart(pdu):
    short_message = pdu['body']['mandatory_parameters']['short_message']
    optional_parameters = {}
    for d in pdu['body'].get('optional_parameters',[]):
        optional_parameters[d['tag']] = d['value']

    #print repr(pdu)

    try:
        mdict = {'multipart_type':'TLV'}
        mdict['reference_number'] = optional_parameters['sar_msg_ref_num']
        mdict['total_number'] = optional_parameters['sar_total_segments']
        mdict['part_number'] = optional_parameters['sar_segment_seqnum']
        mdict['part_message'] = short_message
        return mdict
    except:
        pass

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




print '\n', detect_multipart(unpack_pdu(tlv.get_bin()))
print '\n', detect_multipart(unpack_pdu(sar.get_bin()))
print '\n', detect_multipart(unpack_pdu(csm.get_bin()))
print '\n', detect_multipart(unpack_pdu(csm16.get_bin()))

sar_1 = DeliverSM(1, short_message='\x00\x03\xff\x04\x01There she was just a')
sar_2 = DeliverSM(1, short_message='\x00\x03\xff\x04\x02 walking down the street,')
sar_3 = DeliverSM(1, short_message='\x00\x03\xff\x04\x03 singing doo wa diddy')
sar_4 = DeliverSM(1, short_message='\x00\x03\xff\x04\x04 diddy dum diddy do')

mess_list = []
mess_list.append(detect_multipart(unpack_pdu(sar_3.get_bin())))
mess_list.append(detect_multipart(unpack_pdu(sar_4.get_bin())))
mess_list.append(detect_multipart(unpack_pdu(sar_2.get_bin())))
mess_list.append(detect_multipart(unpack_pdu(sar_1.get_bin())))

print mess_list
print ''.join([i['part_message'] for i in mess_list])
print '\n'
mess_list.sort(key=operator.itemgetter('part_number'))
print mess_list
print ''.join([i['part_message'] for i in mess_list])
