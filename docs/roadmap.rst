Vumi Roadmap
============

The roadmap outlines features intended for upcoming releases of
Vumi. Information on older releases can be found in
:doc:`release-notes`.

Version 0.3
-----------

:Projected date: 9 Dec 2011

* defined common message format. *(done)*
* added user session management. *(done)*
* added transport worker base class. *(done)*
* added application worker base class. *(done)*
* re-organized example application workers into a separate package and
  updated all examples to use common message format *(done)*
* deprecated Django-based vumi.webapp *(done)*
* added and deprecated UglyModel *(done)*
* re-organized  transports into  a  separate package  and updated  all
  transports  to  use  common  message  format (TruTeq,  IRC  and  GSM
  transports still pending)
* add satisfactory HTTP API(s)
* remove SMPP transport's dependency on Django


Version 0.4
-----------

:Projected date: end of February 2012

* add HBase support. See :doc:`roadmap/datastore-access`.
* add storing of all transport messages.
* once-off scheduling of messages.
* remove UglyModel.
* remove Django-based vumi.webapp.
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
