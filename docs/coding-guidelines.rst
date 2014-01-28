.. Coding guidelines

Coding Guidelines
=================

Code contributions to Vumi should:

* Adhere to the :pep:`8` coding standard.
* Come with unittests.
* Come with docstrings.

Vumi docstring format
---------------------

* For classes, __init__ should be documented in the class docstring.
* Function docstrings should look like::

   def format_exception(etype, value, tb, limit=None):
       """Format the exception with a traceback.

       :type etype: exception class
       :param etype: exception type
       :param value: exception value
       :param tb: traceback object
       :param limit: maximum number of stack frames to show
       :type limit: integer or None
       :rtype: list of strings
       """

Unit tests
----------

:doc:`test-helper-api`
