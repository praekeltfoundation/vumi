SMPP
====

An SMPP transport for version 3.4 of the protocol, operating in Transceiver mode.


Incoming SMSs
*************

:routing key: `sms.inbound.<transport-name>.<to_msisdn>`

::

    {
        'destination_addr': '<recipient MSISDN as formatted by the MNO>',
        'source_addr': '<sender MSISDN as formatted by the MNO>',
        'short_message': 'the text of the message'
    }

Delivery Reports
****************

:routing key: `sms.receipt.<transport-name>`

::
    
    {
        'destination_addr': '<recipient MSISDN as formatted by the MNO>',
        'source_addr': '<sender MSISDN as formatted by the MNO>',
        'delivery_report': {
            'id': '<message ID as allocated by the SMSC>',
            'sub': '<number of SMSs originally submitted>',
            'dlvrd': '<number of SMSs delivered>',
            'submit_date': 'YYMMDDhhmm',
            'done_date': 'YYMMDDhhmm',
            'stat': '<receipt message states>',
            'err': '<network specific error code>',
            'text': '<first 20 characters of the SMS>'
        }
    }

Outbound SMSs
*************

:queue: `sms.outbound.<transport-name>`

::
    
    {
        'id': '<the internal message id>',
        'to_msisdn': '<the destination MSISDN>', 
        'from_msisdn': '<the sender MSISDN>'
        'message': '<the message content>',
    }

On successfull delivery of an SMS, the SMPP transport gives us the `transport_message_id` of the message that was delivered. We need to store this as its our only point of reference when the delivery receipt is returned, this is published on with the following info:

:routing key: `sms.ack.<transport-name>`

::

    {
        'id': 'internal message id',
        'transport_message_id': 'transport message id, alpha numeric'
    }


Notes
~~~~~

* This transport does no MSISDN normalization
* This transport tries to guess the outbound MSISDN for any SMS sent using a operator prefix lookup.

Configuration parameters
~~~~~~~~~~~~~~~~~~~~~~~~

::

    system_id: <provided by SMSC>
    password: <provided by SMSC>
    host: smpp.smppgateway.com
    port: 2775
    system_type: <provided by SMSC>

    # Optional variables, some SMSCs require these to be set.
    interface_version: "34"
    dest_addr_ton: 1
    dest_addr_npi: 1
    registered_delivery: 1

    TRANSPORT_NAME: smpp_transport

    # Number Recognition
    COUNTRY_CODE: "27"

    OPERATOR_NUMBER:
        VODACOM: "<outbound MSISDN to be used for this MNO>"
        MTN: "<outbound MSISDN to be used for this MNO>"
        CELLC: "<outbound MSISDN to be used for this MNO>"
        VIRGIN: "<outbound MSISDN to be used for this MNO>"
        8TA: "<outbound MSISDN to be used for this MNO>"
        UNKNOWN: "<outbound MSISDN to be used for this MNO>"

    OPERATOR_PREFIX:
        2771:
            27710: MTN
            27711: VODACOM
            27712: VODACOM
            27713: VODACOM
            27714: VODACOM
            27715: VODACOM
            27716: VODACOM
            27717: MTN
            27719: MTN

        2772: VODACOM
        2773: MTN
        2774:
            27740: CELLC
            27741: VIRGIN
            27742: CELLC
            27743: CELLC
            27744: CELLC
            27745: CELLC
            27746: CELLC
            27747: CELLC
            27748: CELLC
            27749: CELLC

        2776: VODACOM
        2778: MTN
        2779: VODACOM
        2781:
            27811: 8TA
            27812: 8TA
            27813: 8TA
            27814: 8TA

        2782: VODACOM
        2783: MTN
        2784: CELLC


