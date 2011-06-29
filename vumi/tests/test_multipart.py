import operator

from smpp.pdu_builder import *
from smpp.pdu_inspector import *
from twisted.trial.unittest import TestCase

tlv = DeliverSM(1, short_message='the first message part')
tlv.set_sar_msg_ref_num(65017)
tlv.set_sar_total_segments(22)
tlv.set_sar_segment_seqnum(21)
sar = DeliverSM(1, short_message='\x00\x03\xff\x02\x01the first message part')
csm = DeliverSM(1, short_message='\x05\x00\x03\xff\x02\x01the first message part')
csm16 = DeliverSM(1, short_message='\x06\x00\x04\xff\xff\x02\x01the first message part')


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
