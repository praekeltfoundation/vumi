Vumi Roadmap
============

The roadmap outlines features intended for upcoming releases of
Vumi. Information on older releases can be found in
:doc:`release-notes`.


Version 0.4
-----------

:Projected date: end of February 2012

* once-off scheduling of messages. *(done)*
* addition of MultiWorker. *(done)*
* addition of support for grouped messages. *(done)*
* remove UglyModel. *(done)*
* clean-up of SMPP transport.
* remove Django-based vumi.webapp.
* add HBase support with ability to use Redis for small
  deployments. See :doc:`roadmap/datastore-access`.
* add storing of all transport messages.
* add support for tag pools.


Version 0.5
-----------

:Projected date: end of April 2012

* add ability to identify a single user across multiple transports as
  per :doc:`roadmap/identity-datastore`.
* associate messages with billing accounts. See
  :doc:`roadmap/accounting`.
* support custom application logic in Javascript. See
  :doc:`roadmap/custom-app-logic`.
* support dynamic addition and removal of workers. See
  :doc:`roadmap/dynamic-workers`.


Future
------

Future plans that have not yet been scheduled for a specific milestone
are outlined in the following sections. Parts of these features may
already have been implemented or have been included in the detailed
roadmap above:

 .. toctree::
    :maxdepth: 1

    roadmap/blinkenlights.rst
    roadmap/dynamic-workers.rst
    roadmap/identity-datastore.rst
    roadmap/conversation-datastore.rst
    roadmap/custom-app-logic.rst
    roadmap/accounting.rst
    roadmap/datastore-access.rst
