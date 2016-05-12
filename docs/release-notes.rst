Release Notes
=============

Version 0.6
-----------

NOTE: Version 0.6.x is backward-compatible with 0.5.x for the most part, with
some caveats. The first few releases will be removing a bunch of obsolete and
deprecated code and replacing some of the internals of the base worker. While
this will almost certainly not break the majority of things built on vumi, old
code or code that relies too heavily on the details of worker setup may need to
be fixed.

:Version: 0.6.8
:Date released: 12 May 2016

* Allow disabling of delivery report handling as sometimes these cause more noise 
  than signal.
* Embed the original SMPP transports delivery report status into the message 
  transport metadata. This is useful information that applications may chose
  to act on.

:Version: 0.6.7
:Date released: 19 April 2016

* Re-fix the bug in the Vumi Bridge transport that prevents it making outbound
  requests.

:Version: 0.6.6
:Date released: 18 April 2016

* Fix bug in Vumi Bridge transport that prevented it making outbound requests.

:Version: 0.6.5
:Date released: 15 April 2016

* Update the Vumi Bridge transport to perform teardown more carefully (including
  tearing down the Redis manager and successfully tearing down even if start up
  failed halfway).
* Add support for older SSL CA certificates when using the Vumi Bridge
  transport to connect to Vumi Go.

:Version: 0.6.4
:Date released: 8 April 2016

* Fix object leak caused by creating lots of Redis submanagers.
* Remove deprecated manhole middleware.
* Update fake_connections wrapping of abortConnection to work with Twisted
  16.1.

:Version: 0.6.3
:Date released: 31 March 2016

* Refactor and update the Vumi Bridge non-streaming HTTP API client, including
  adding status events and a web_path configuration option for use with Junebug.
* Remove the deprecated Vumi Bridge streaming HTTP API client.
* Add a Dockerfile entrypoint script.
* Rename the TWISTD_APPLICATION Dockerfile variable to TWISTD_COMMAND.
* Pin the version of Vumi installed in the Dockerfile.
* Update manhole middleware so that tests pass with Twisted 16.0.

:Version: 0.6.2
:Date released: 3 March 2016

* Add support for uniformly handling Redis ResponseErrors across different
  Redis implementations.

:Version: 0.6.1
:Date released: 2 March 2016

* Removed support for Python 2.6.
* Publish status messages from WeChat transport (for use with Junebug).
* A support for the rename command to FakeRedis.
* Add Dockerfile for running Vumi.
* Fixed typo in "How we do releases" documentation.

:Version: 0.6.0
:Date released: 7 Dec 2015

* Removed various obsolete test helper code in preparation for AMQP client
  changes.
* Started writing release notes again.

Version 0.5
-----------

No release notes for three and a half years. Sorry. :-(

Version 0.4
-----------

:Version: 0.4.0
:Date released: 16 Apr 2012

* added support for once-off scheduling of messages.
* added MultiWorker.
* added support for grouped messages.
* added support for middleware for transports and applicatons.
* added middleware for storing of all transport messages.
* added support for tag pools.
* added Mediafone transport.
* added support for setting global vumi worker options via a YAML
  configuration file.
* added a keyword-based message dispatcher.
* added a grouping dispatcher that assists with A/B testing.
* added support for sending outbound messages that aren't replies to
  application workers.
* extended set of message parameters supported by the http_relay worker.
* fixed twittytwister installation error.
* fixed bug in Integrat transport that caused it to send two new
  session messages.
* ported the TruTeq transport to the new message format.
* added support for longer messages to the Opera transport.
* wrote a tutorial.
* documented middleware and dispatchers.
* cleaned up of SMPP transport.
* removed UglyModel.
* removed Django-based vumi.webapp.
* added support for running vumi tests using tox.


Version 0.3
-----------

:Version: 0.3.1
:Date released: 12 Jan 2012

* Use yaml.safe_load everywhere YAML config files are loaded. This
  fixes a potential security issue which allowed those with write
  access to Vumi configuration files to run arbitrary Python code as
  the user running Vumi.
* Fix bug in metrics manager that unintentionally allowed two metrics
  with the same name to be registered.

:Version: 0.3.0
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

:Version: 0.2.0
:Date released: 19 September 2011

* System metrics as per :doc:`roadmap/blinkenlights`.
* Realtime dashboarding via Geckoboard.


Version 0.1
-----------

:Version: 0.1.0
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
