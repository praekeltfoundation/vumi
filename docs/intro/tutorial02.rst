====================================
Writing your first Vumi app - Part 2
====================================

This is the second part in a series of tutorials demonstrating how to develop Vumi apps.

If you haven't done so already you might want to work through :doc:`part 1 of this tutorial</intro/tutorial01>` before proceeding.

In this part of the tutorial we'll be creating a simple chat bot communicating over `Google Talk`_.

More specifically we'll be utilizing Vumi's XMPP *transport worker* to log into a `Google Talk`_ account and listen for incomming chat messages. When messages are received an `Alice Bot`_ based *application worker* will determine an appropriate response based on the incomming message. The XMPP *transport worker* will then send the response. For another `Google Talk`_ user chatting with the Vumi connected account it should *appear* as if she is conversing with another human being. 

XMPP Transport Worker
=====================

Continuing from :doc:`part 1 of this tutorial</intro/tutorial01>`, instead of using the Telnet *transport worker* we'll be using the Vumi builtin XMPP *transport worker* to communicate over Google Talk.

In order to use the XMPP *transport worker* you first need to create a configuration file. 

To do this, create a ``transport.yaml`` file in your current directory and edit it to look like this (replacing ``"username"`` and ``"password"`` with your specific details)::

    transport_name: xmpp_transport
    username: "username"
    password: "password"
    status: Playing with Vumi.
    host: talk.google.com
    port: 5222

Going through that line by line:

``transport_name: xmpp_transport`` - specifies the transport name. Again this is used to identify the *transport worker* for subsequent connection by *application workers*.

``username: "username"`` - the `Google Talk`_ account username to which the *transport worker* will connect.

``password: "password"`` - the `Google Talk`_ account password to which the *transport worker* will connect.

``status: Playing with Vumi`` - causes the `Google Talk`_ account's chat status to change to `Playing with Vumi.`
    
``host: talk.google.com`` - The XMPP host to connect to. `Google Talk`_ uses ``talk.google.com``.

``port: 5222`` - The XMPP port to connect to. `Google Talk`_ uses ``5222``.


.. note::

    Vumi utilizes YAML_ based configuration files to provide configuration parameters to workers, both transport and application. YAML is a human friendly data serialization standard that works quite well for specifying configurations.

Now you can start the XMPP *transport worker* with the created configuration by executing the following command::

    $ twistd -n start_worker --worker-class vumi.transports.xmpp.XMPPTransport --config=./transport.yaml

.. admonition:: SASLNoAcceptableMechanism Exceptions

    In the event of this command raising a ``twisted.words.protocols.jabber.sasl.SASLNoAcceptableMechanism`` exception you should upgrade your pyOpenSSL package, which you can do by executing ``pip install --upgrade pyOpenSSL`` from the command line.

.. note::

    This is different from the example in :doc:`part 1 of this tutorial</intro/tutorial01>` in that we no longer set any configuration options through the command line. Instead all configuration is contained in the specified ``transport.yaml`` config file.

This causes a Vumi XMPP *transport worker* to connect to the configuration specified `Google Talk`_ account and listen for messages. You should now be able to start messaging the account from another `Google Talk`_ account using any `Google Talk`_ client (although no response will be generated until the *application worker* is instantiated).

Alice Bot Application Worker
============================

.. _Alice Bot: http://www.alicebot.org/
.. _Google Talk: https://www.google.com/talk/
.. _YAML: http://yaml.org/
