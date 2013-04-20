Transports
==========

Transports provide the means for Vumi to send and receive messages
from people, usually via a third-party such as a mobile network
operator or instant message service provider.

Vumi comes with support for numerous transports built-in. These
include SMPP (SMS), SSMI (USSD), SMSSync (SMS over your Android
phone), XMPP (Google Chat and Jabber), Twitter, IRC, telnet and
numerous SMS and USSD transports for specific mobile network
aggregators.


Transports for common protocols
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. toctree::
    :maxdepth: 1

    base.rst
    smpp.rst
    httprpc.rst
    oldhttp.rst


Transports for specific aggregators
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. toctree::
    :maxdepth: 1

    airtel.rst
    apposit.rst
    cellulant.rst
    vas2nets.rst
