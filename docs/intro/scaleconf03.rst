===========================================
ScaleConf Workshop - Writing an Application
===========================================

In this section we'll be writing a minimal Twitter clone in Python using SMS
as the communication channel. `@imsickofmaps`_ suggested we call
it "smitter".

Setting up the SMS Transport
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, to use SMS we need to setup the SMS transport. This is the same
procedure as setting up the USSD transport. Use this as the template and
save it in a file called "sms_transport.yaml" (note that we're using
a different value for 'transport_name'):

.. literalinclude:: sample-sms-transport.yaml

Add the following bits to the "supervisord.conf" file created earlier:

.. literalinclude:: sample-supervisord-twitter.conf

Writing your application
~~~~~~~~~~~~~~~~~~~~~~~~

Create a file called `scaleconf.py` put in the following code and replace
the place holder text with your South African phone number as a string:

.. literalinclude:: scaleconf01.py


Tell supervisord to add the new processes::

    $ supervisorctl -c etc/supervisord.conf update

Within a few seconds you should receive an SMS from your application.
If you reply to this number then you'll receive "thanks" back.


Making things more interesting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

What we want to do is the following:

1.  People join by SMSing '+' to your SMS longcode.
2.  People leave by SMSing in '-' to your SMS longcode.
3.  Anything else is broadcast to all known users currently joined.

.. literalinclude:: scaleconf02.py

Tell supervisord to restart the smitter application::

    $ supervisorctl -c etc/supervisord.conf restart smitter

SMS '+' to the SMS longcode and you should receive a confirmation.
Ask someone else to join and test if the broadcasting and leaving works.

More Features!
~~~~~~~~~~~~~~

Now that we've got a very minimal application running, here are some features
you could look at adding to make improve it.

1.  Add the ability to form groups.
2.  Add the ability to invite people to join via SMS.
3.  Add persistence via :class:`vumi.persist.txredis_manager.TxRedisManager` to
    allow multiple Smitter processes to run in Supervisord_ while sharing data.

.. _`@imsickofmaps`: https://twitter.com/imsickofmaps
.. _Supervisord: http://www.supervisord.org/
