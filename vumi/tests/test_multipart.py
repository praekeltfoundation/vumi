
from smpp.pdu_builder import *
from twisted.trial.unittest import TestCase

p = DeliverSM(1, short_message='\x00\x03\x11\x02\x01iiiiii')

print unpack_pdu(p.get_bin())


def detect_multipart(pdu):

    short_message = pdu.get_obj()['body']['mandatory_parameters']['short_message']
    print repr(short_message.split(''))
    if (short_message[0:1] == '\x00'
        and short_message[1:2] == '\x03'):
        print "DDDDD"

    _2 = short_message[1:2]
    _3 = short_message[2:3]
    _4 = short_message[3:4]
    _5 = short_message[4:5]

def total_parts(pdu):
    pass

def part_number(pdu):
    pass


detect_multipart(p)
