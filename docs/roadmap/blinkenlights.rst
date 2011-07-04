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

::name:: 

