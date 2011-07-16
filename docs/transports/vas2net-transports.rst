Vas2Nets
========

A WASP providing connectivity in Nigeria.


Incoming SMSs
*************

:routing key: `sms.inbound.<transport-name>.<to_msisdn>`

::

    {
        'transport_message_id': 'alpha numeric',
        'transport_timestamp': 'iso 8601 format',
        'transport_network_id': 'MNO unique id, used for number portability',
        'transport_keyword': 'keyword if provided by vas2nets',
        'to_msisdn': '+27761234567',
        'from_msisdn': '+27761234567',
        'message': 'message content'
    }

Delivery Reports
****************

:routing key: `sms.receipt.<transport-name>`

::

    {
        'transport_message_id': 'alpha numeric',
        'transport_status': 'numeric',
        'transport_status_message': 'text status accompanying numeric status',
        'transport_timestamp': 'iso 8601 format',
        'transport_network_id': 'MNO unique id, used for number portability',
        'to_msisdn': '+27761234567',
        'id': 'transport message id if this was a reply, else internal id'
    }

Outbound SMSs
*************

:queue: `sms.outbound.<transport-name>`

::
    
    {
        'to_msisdn': '...',
        'from_msisdn': '...',
        'reply_to': 'reply to transport_message_id',
        'id': 'internal message id',
        'transport_network_id': 'MNO unique id, used for number portability',
        'message': 'the body of the sms text'
    }

On successfull delivery of an SMS, Vas2Nets gives us the `transport_message_id` 
of the message that was delivered. We need to store this as its our only point
of reference when the delivery receipt is returned, this is published on with
the following info:

:routing key: `sms.ack.<transport-name>`

::

    {
        'id': 'internal message id',
        'transport_message_id': 'transport message id, alpha numeric'
    }


Notes:
~~~~~~

Valid single byte characters::

    string.ascii_lowercase,     # a-z
    string.ascii_uppercase,     # A-Z
    '0123456789',
    'äöüÄÖÜàùòìèé§Ññ£$@',
    ' ',
    '/?!#%&()*+,-:;<=>.',
    '\n\r'
    
Valid double byte characters, will limit SMS to max length of 70 instead of 
160 if used::

    '|{}[]€\~^'

If any characters are published that aren't in this list the transport will raise a `Vas2NetsEncodingError`. If characters are published that are in the double byte set the transport will print warnings in the log.

Configuration parameters
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    transport_name: vas2nets
    web_receive_path: /api/v1/sms/vas2nets/receive/
    web_receipt_path: /api/v1/sms/vas2nets/receipt/
    web_port: 8123

    owner: <provided by vas2nets>
    service: <provided by vas2nets>
    subservice: <provided by vas2nets>


