Test helper API reference
=========================

.. automodule:: vumi.tests.helpers
   :members:
   :no-show-inheritance:
   :member-order: bysource
   :exclude-members: VumiTestCase, DEFAULT

   .. autoclass:: VumiTestCase()
      :members:

   .. data:: DEFAULT

      This constant is a placeholder value for parameter defaults.
      (We can't just use ``None`` because we may want to override a non-``None`` default with an explicit ``None`` value.)
