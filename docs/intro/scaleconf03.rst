===========================================
ScaleConf Workshop - Writing an Application
===========================================

In this section we'll be writing a minimal Twitter clone in Python using SMS
as the communication channel. `@imsickofmaps`_ suggested we call
it "smitter".

First, to use SMS we need to setup the SMS transport. This is the same
procedure as setting up the USSD transport. Use this as the template and
save it in a file called "sms_transport.yaml" (note that we're using
a different value for 'transport_name'):

.. literalinclude:: sample-sms-transport.yaml

Add the following bits to the "supervisord.conf" file created earlier:

.. literalinclude:: sample-supervisord-twitter.conf

Create a file called `scaleconf.py` and put in the following code:

.. literalinclude:: scaleconf01.py

Tell supervisord to add the new processes::

    $ supervisorctl -c etc/supervisord.conf update

Within a few seconds you should receive an SMS from your application.
If you reply to this number then you'll receive "thanks" back.
