"""
SMPP transport API.
"""

from vumi.transports.smpp.smpp_transport import (
    SmppTransceiverTransport, SmppTransmitterTransport, SmppReceiverTransport,
    SmppTransceiverTransportWithOldConfig as SmppTransport)

__all__ = [
    'SmppTransport',
    'SmppTransceiverTransport',
    'SmppTransmitterTransport',
    'SmppReceiverTransport',
]
