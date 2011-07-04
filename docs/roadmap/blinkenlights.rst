Blinkenlights
=============

Failure is guaranteed, what will define our success when things fail is how we respond. We can only respond as good as we can gauge the performance of the individual components that make up Vumi. 

Blinkenlights is a technical management module for Vumi that gives us that insight. It will give us accurate and realtime data on the general health and well being of all of the different moving parts.

Implementation Details
**********************

Blinkenlights connects to a dedicated exchange on our message broker. All messages broadcast to this exchange are meant for Blinkenlights to consume.

Every component connected to our message broker has a dedicated channel for broadcasting status updates to Blinkenlights. Blinkenlights will consume these messages and make them available for viewing in a browser. 

.. note:: Blinkenlights will probably remain intentionally ugly as we do not want people to mistake this for a dashboard.

Typical Blinkenlights Message Payload
-------------------------------------

The messages that Blinkenlights are JSON encoded dictionaries. An example Blinkenlights message only requires three keys::

    { 
        "name": "SMPP Transport 1",
        "uuid": "0f148162-a25b-11e0-ba57-0017f2d90f78",
        "timestamp": [2011, 6, 29, 15, 3, 23]
    } 

:name:
    The name of the component connected to AMQP. Preferably unique.
:uuid:
    An identifier for this component, must be unique.
:timestamp:
    A UTC timestamp as a list in the following format: [YYYY, MM, DD, HH, MM, SS]. We use a list as Javascript doesn't have a built-in date notation for JSON.

The components should publish a status update in the form of a JSON dictionary every minute. If an update hasn't been received for two minutes then the component will be flagged as being in an error state.

Any other keys and values can be added to the dictionary, they'll be published in a tabular format. Each transport is free to add whatever relevant key/value pairs. For example, for SMPP a relevant extra key/value pair could be messages per second processed.