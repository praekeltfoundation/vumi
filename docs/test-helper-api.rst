Test helper API reference
=========================

Basic test helpers
------------------

.. automodule:: vumi.tests.helpers
   :members:
   :no-show-inheritance:
   :member-order: bysource
   :exclude-members: VumiTestCase, DEFAULT

   .. autoclass:: VumiTestCase()
      :members:

   .. data:: DEFAULT

      This constant is a placeholder value for parameter defaults.
      We can't just use ``None`` because we may want to override a non-``None`` default with an explicit ``None`` value.


Application worker test helpers
-------------------------------

.. automodule:: vumi.application.tests.helpers
   :members:
   :no-show-inheritance:
   :member-order: bysource


Transport worker test helpers
-----------------------------

.. automodule:: vumi.transports.tests.helpers
   :members:
   :no-show-inheritance:
   :member-order: bysource

.. automodule:: vumi.transports.httprpc.tests.helpers
   :members:
   :no-show-inheritance:
   :member-order: bysource


Dispatcher worker test helpers
------------------------------

.. automodule:: vumi.dispatchers.tests.helpers
   :members:
   :no-show-inheritance:
   :member-order: bysource
