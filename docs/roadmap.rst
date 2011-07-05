Vumi Roadmap
============

This roadmap is tentative. The different milestones to be developed as part of the roadmap are described in more detail in the following sections.

Milestones in order of descending priority:

.. toctree::
    :maxdepth: 1
    
    roadmap/blinkenlights.rst
    roadmap/dynamic-workers.rst
    roadmap/identity-datastore.rst
    roadmap/conversation-datastore.rst
    roadmap/custom-app-logic.rst
    roadmap/accounting.rst
    roadmap/datastore-access.rst

Version 0.1 (**Current**)
-------------------------

* SMPP Transport (version 3.4 in transceiver mode)
    * Send & receive SMS messages.
    * Send & receive USSD messages over SMPP.
    * Supports SAR (segmentation and reassembly, allowing receiving of SMS messages larger than 160 characters).
    * Graceful reconnecting of a failed SMPP bind.
    * Delivery reports of SMS messages
* XMPP Transport
* GSM Transport (currently uses `pygsm <http://pypi.python.org/pypi/pygsm>`_, looking at `gammu <http://wammu.eu>`_ as a replacement)
    * Interval based polling of new SMS messages that a GSM modem has received.
    * Immediate sending of outbound SMS messages.
* Twitter Transport
    * Live tracking of any combination of keywords or hashtags on twitter.
* USSD Transports for various aggregators covering 12 African countries.
* HTTP API for SMS messaging:
    * Sending SMS messages via a given transport.
    * Receiving SMS messages via an HTTP callback.
    * Receiving SMS delivery reports via an HTTP callback.
    * Querying received SMS messages.
    * Querying the delivery status of sent SMS messages.

Version 0.2 (31 July 2011)
--------------------------

* Connect `frontend <http://ux.vumi.org>`_ to backend.
* APIs for supporting frontend.
    * User account creation & authentication.
    * Group account creation & membership.
    * Group membership importing from an Excel sheet and Google Docs.
    * Conversation creation & scheduling.
    * Engagement tracking of conversation per transport. (e.g. Conversation X had 100 replies over Transport A and 300 replies via Transport B)
* System metrics as per :doc:`roadmap/blinkenlights`
* Ability to dynamically start new instances of existing transport types as per :doc:`roadmap/dynamic-workers`
* Ability to identify a single user across multiple transports as per :doc:`roadmap/identity-datastore`

Version 0.3 (Aug / Sept 2011)
-----------------------------

* :doc:`roadmap/conversation-datastore`.
* :doc:`roadmap/datastore-access`.
* :doc:`roadmap/custom-app-logic`.
* :doc:`roadmap/accounting`
