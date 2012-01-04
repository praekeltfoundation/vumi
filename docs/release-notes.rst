Release Notes
=============

Version 0.3
-----------

:Date released: 4 Jan 2012

* defined common message format.
* added user session management.
* added transport worker base class.
* added application worker base class.
* made workers into Twisted services.
* re-organized example application workers into a separate package and
  updated all examples to use common message format
* deprecated Django-based vumi.webapp
* added and deprecated UglyModel
* re-organized transports into a separate package and updated all
  transports except TruTeq to use common message (TruTeq will be
  migrated in 0.4 or a 0.3 point release).
* added satisfactory HTTP API(s)
* removed SMPP transport's dependency on Django


Version 0.2
-----------

:Date released: 19 September 2011

* System metrics as per :doc:`roadmap/blinkenlights`.
* Realtime dashboarding via Geckoboard.


Version 0.1
-----------

:Date released: 4 August 2011

* SMPP Transport (version 3.4 in transceiver mode)

    * Send & receive SMS messages.
    * Send & receive USSD messages over SMPP.
    * Supports SAR (segmentation and reassembly, allowing receiving of
      SMS messages larger than 160 characters).
    * Graceful reconnecting of a failed SMPP bind.
    * Delivery reports of SMS messages.

* XMPP Transport

    * Providing connectivity to Gtalk, Jabber and any other XMPP based
      service.

* IRC Transport

    * Currently used to log conversations going on in various IRC
      channels.

* GSM Transport (currently uses `pygsm
  <http://pypi.python.org/pypi/pygsm>`_, looking at `gammu
  <http://wammu.eu>`_ as a replacement)

    * Interval based polling of new SMS messages that a GSM modem has
      received.
    * Immediate sending of outbound SMS messages.

* Twitter Transport

    * Live tracking of any combination of keywords or hashtags on
      twitter.

* USSD Transports for various aggregators covering 12 African
  countries.
* HTTP API for SMS messaging:

    * Sending SMS messages via a given transport.
    * Receiving SMS messages via an HTTP callback.
    * Receiving SMS delivery reports via an HTTP callback.
    * Querying received SMS messages.
    * Querying the delivery status of sent SMS messages.
