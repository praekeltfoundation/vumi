====================================
Writing your first Vumi app - Part 2
====================================

This is the second part in a series of tutorials demonstrating how to develop Vumi apps.

If you haven't done so already you might want to work through :doc:`part 1 of this tutorial</intro/tutorial01>` before proceeding.

In this part of the tutorial we'll be creating a simple chat bot communicating over `Google Talk`_.

More specifically we'll be utilizing Vumi's XMPP *transport worker* to log into a `Google Talk`_ account and listen for incoming chat messages. When messages are received an `Alice Bot`_ based *application worker* will determine an appropriate response based on the incoming message. The XMPP *transport worker* will then send the response. For another `Google Talk`_ user chatting with the Vumi connected account it should *appear* as if she is conversing with another human being.

.. note::

    Remember your virtual environment should be active. Activate it by running running ``source ve/bin/activate``.

XMPP Transport Worker
=====================

Continuing from :doc:`part 1 of this tutorial</intro/tutorial01>`, instead of using the Telnet *transport worker* we'll be using Vumi's built-in XMPP *transport worker* to communicate over Google Talk.

In order to use the XMPP *transport worker* you first need to create a configuration file.

To do this, create a ``transport.yaml`` file in your current directory and edit it to look like this (replacing ``"username"`` and ``"password"`` with your specific details)::

    transport_name: xmpp_transport
    username: "username"
    password: "password"
    status: Playing with Vumi.
    host: talk.google.com
    port: 5222

Going through that line by line:

``transport_name: xmpp_transport`` - specifies the transport name. This identifies the *transport worker* for subsequent connection by *application workers*.

``username: "username"`` - the `Google Talk`_ account username to which the *transport worker* will connect.

``password: "password"`` - the `Google Talk`_ account password.

``status: Playing with Vumi`` - causes the `Google Talk`_ account's chat status to change to ``Playing with Vumi.``

``host: talk.google.com`` - The XMPP host to connect to. `Google Talk`_ uses ``talk.google.com``.

``port: 5222`` - The XMPP port to connect to. `Google Talk`_ uses ``5222``.


.. note::

    Vumi utilizes YAML_ based configuration files to provide configuration parameters to workers, both transport and application. YAML is a human friendly data serialization standard that works quite well for specifying configurations.

Now start the XMPP *transport worker* with the created configuration by executing the following command::

    $ twistd -n --pidfile=transportworker.pid vumi_worker --worker-class vumi.transports.xmpp.XMPPTransport --config=./transport.yaml

.. admonition:: SASLNoAcceptableMechanism Exceptions

    In the event of this command raising a ``twisted.words.protocols.jabber.sasl.SASLNoAcceptableMechanism`` exception you should upgrade your pyOpenSSL_ package by executing ``pip install --upgrade pyOpenSSL`` from the command line.

.. note::

    This is different from the example in :doc:`part 1 of this tutorial</intro/tutorial01>` in that we no longer set any configuration options through the command line. Instead all configuration is contained in the specified ``transport.yaml`` config file.

This causes a Vumi XMPP *transport worker* to connect to the configuration specified `Google Talk`_ account and listen for messages. You should now be able to start messaging the account from another `Google Talk`_ account using any `Google Talk`_ client (although no response will be generated until the *application worker* is instantiated).

Alice Bot Application Worker
============================

Continuing from :doc:`part 1 of this tutorial</intro/tutorial01>`, instead of using the *echo application worker* we'll be creating our own worker to generate *seemingly intelligent* responses.

.. admonition:: Philosophy

    Remember *application workers* are responsible for processing messages received from *transport workers* and generating replies - it holds the application logic. When developing Vumi applications you'll mostly be implementing *application workers* to process messages based on your use case. For the most part you'll be relying on Vumi's built-in *transport workers* to take care of the communications medium. This enables you to forget about the hairy details of the communications medium and instead focus on the fun stuff.

Before we proceed let's install our dependencies. We'll be using PyAIML_ to provide our bot with *knowledge*. Install it by executing the following command::

    $ pip install http://sourceforge.net/projects/pyaiml/files/PyAIML%20%28unstable%29/0.8.6/PyAIML-0.8.6.tar.gz

We also need a *brain* for our bot. Download a precompiled brain by executing the following command::

    $ wget https://github.com/downloads/praekelt/public-eggs/alice.brn

.. note::

    For the sake of simplicity we're using an existing brain. You can however compile your own brain by downloading the `free Alice AIML set <https://code.google.com/p/aiml-en-us-foundation-alice/>`_ and *learning* it as described in the `PyAIML examples <http://pyaiml.sourceforge.net/#examples>`_. Perhaps you rather want a `Fake Captain Kirk <https://code.google.com/p/aiml-en-us-foundation-fakekirk/>`_.

Now we can move on to creating the *application worker*. Create a ``workers.py`` file in your current directory and edit it to look like this::

    import aiml
    from vumi.application.base import ApplicationWorker

    class AliceApplicationWorker(ApplicationWorker):

        def __init__(self, *args, **kwargs):
            self.bot = aiml.Kernel()
            self.bot.bootstrap(brainFile="alice.brn")
            return super(AliceApplicationWorker, self).__init__(*args, **kwargs)

        def consume_user_message(self, message):
            message_content = message['content']
            message_user = message.user()
            response = self.bot.respond(message_content, message_user)
            self.reply_to(message, response)

The code is straightforward. *Application workers* are represented by a class that subclasses :class:`vumi.application.base.ApplicationWorker`. In this example the ``__init__`` method is overridden to initialize our bot's brain. The heart of *application workers* though is the ``consume_user_message`` method, which is passed messages for processing as they are received by *transport workers*. The message argument contains details on the received message. In this example the content of the message is retrieved from ``message['content']``, and the `Google Talk`_ user sending the message is determined by calling ``message.user()``. A response is then generated for the specific user utilizing the bot by calling ``self.bot.respond(message_content, message_user)``. This response is then sent as a reply to the original message by calling ``self.reply_to(message, response)``. The *transport worker* then takes care of sending the response to the correct user over the communications medium.

.. admonition:: Philosophy

    The *application worker* has very little knowledge about and does not need to know the specifics of the communications medium. In this example we could just as easily have communicated over SMS or even Twitter without having to change the *application worker's* implementation.

Now start the `Alice Bot`_ *application worker* in a new command line session by executing the following command::

    $ twistd -n --pidfile=applicationworker.pid vumi_worker --worker-class workers.AliceApplicationWorker --set-option=transport_name:xmpp_transport

.. note::
    Again note how the *application worker* is connected to the previously defined, already running *transport worker* by specifying ``--set-option=transport_name:xmpp_transport``.

Now with both the *transport worker* and *application worker* running you should be able to send a chat message to the `Google Talk`_ account configured in ``transport.yaml`` and receive a *seemingly intelligent* response generated by our `Alice Bot`_.

Coming soon
===========

The tutorial ends here for the time being. Future installments of the tutorial
will cover:

* Advanced applications.
* Scaling and deploying.

In the meantime, you might want to check out :doc:`some other docs <../index>`.

.. _Alice Bot: http://www.alicebot.org/
.. _Google Talk: https://www.google.com/talk/
.. _pyOpenSSL: http://pypi.python.org/pypi/pyOpenSSL
.. _PyAIML: http://pyaiml.sourceforge.net/
.. _YAML: http://yaml.org/
