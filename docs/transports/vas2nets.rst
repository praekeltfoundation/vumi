Vas2Nets
========

A WASP providing connectivity in Nigeria via an HTTP API.

Vas2nets Transport
^^^^^^^^^^^^^^^^^^

.. py:module:: vumi.transports.vas2nets

.. autoclass:: Vas2NetsTransport


Notes
^^^^^

Valid single byte characters::

    string.ascii_lowercase,     # a-z
    string.ascii_uppercase,     # A-Z
    '0123456789',
    'äöüÄÖÜàùòìèé§Ññ£$@',
    ' ',
    '/?!#%&()*+,-:;<=>.',
    '\n\r'

Valid double byte characters, will limit SMS to max length of 70
instead of 160 if used::

    '|{}[]€\~^'

If any characters are published that aren't in this list the transport
will raise a `Vas2NetsEncodingError`. If characters are published that
are in the double byte set the transport will print warnings in the
log.


Example configuration
^^^^^^^^^^^^^^^^^^^^^

::

    transport_name: vas2nets
    web_receive_path: /api/v1/sms/vas2nets/receive/
    web_receipt_path: /api/v1/sms/vas2nets/receipt/
    web_port: 8123

    url: <provided by vas2nets>
    username: <provided by vas2nets>
    password: <provided by vas2nets>
    owner: <provided by vas2nets>
    service: <provided by vas2nets>
    subservice: <provided by vas2nets>
