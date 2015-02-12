====================================
Writing your first Vumi app - Part 1
====================================

This is the first part in a series of tutorials demonstrating how to develop Vumi apps.

We'll assume you have a working knowledge of Python_, RabbitMQ_ and VirtualEnv_.

.. admonition:: Where to get help

    If you're having trouble at any point feel free to drop by `#vumi`_ on irc.freenode.net to chat with other Vumi users who might be able to help.

In this part of the tutorial we'll be creating and testing a simple working environment.

Environment Setup
=================

Before we proceed let's create an isolated working environment using VirtualEnv_.

From the command line ``cd`` into a directory where you'd like to store your code then run the following command::

    $ virtualenv --no-site-packages ve

This will create a ``ve`` directory where any libraries you install will go, thus isolating your environment.
Once the virtual environment has been created activate it by running ``source ve/bin/activate``.

.. note::

    For this to work VirtualEnv_ needs to be installed. You can tell it's installed by executing ``virtualenv`` from the command line. If that command runs successfully with no errors VirtualEnv_ is installed. If not you can install it by executing ``sudo pip install virtualenv`` from the command line.

.. note::

    From this point onwards your virtual environment should always be active. The virtualenv is activated by running ``source ve/bin/activate``.

Now that you created and activated the virtual environment install Vumi with the following command::

    $ pip install -e git+git://github.com/praekelt/vumi.git@develop#egg=vumi

.. note::

    This will install the development version of Vumi containing the latest-and-greatest features. Although the development branch is kept stable it is not recommended for production environments.

If this is your first Vumi application you need to take care of some initial RabbitMQ_ setup. Namely you need to add a ``vumi`` user and a ``develop`` virtual host and grant the required permissions. Vumi includes a script to do this for you which you can execute with the following command::

    $ sudo ./ve/src/vumi/utils/rabbitmq.setup.sh

.. note::

    Vumi workers communicate over RabbitMQ_ and requires it being installed and running. You can tell it's installed and its current status by executing ``sudo rabbitmq-server`` from the command line. If the command is not found you can install RabbitMQ by executing ``sudo apt-get install rabbitmq-server`` from the command line (assuming you are on a Debian based distribution).

Testing the Environment
=======================

Let's verify this worked. As a test you can create a Telnet worker and an *echo* application, both of which are included in Vumi.

.. admonition:: Philosophy

    A complete Vumi instance consists of a *transport worker* and an *application worker* which are managed as separate processes. A *transport worker* is responsible for sending messages to and receiving messages from some communications medium. An *application worker* processes messages received from a *transport worker* and generates replies.

Start the Telnet *transport worker* by executing the following command::

    $ twistd -n --pidfile=transportworker.pid vumi_worker --worker-class vumi.transports.telnet.TelnetServerTransport --set-option=transport_name:telnet --set-option=telnet_port:9010

This utilizes Twisted_ to start a Telnet process listening on port 9010. Specifically it uses Vumi's builtin ``TelnetServerTransport`` to handle communication with Telnet clients. Note that we specify ``telnet`` as the transport name when providing ``--set-option=transport_name:telnet``. When starting the *application worker* as described next the same name should be used, thus connecting the *transport worker* with the *application worker*.

.. admonition:: Philosophy

    A *transport worker* is responsible for sending messages over and receiving messages from some communication medium. For this example we are using a very simple transport that communicates over Telnet. Other transport mechanisms Vumi supports include SMPP, XMPP, Twitter, IRC, HTTP and a variety of mobile network aggregator specific messaging protocols. In subsequent parts of this tutorial we'll be using the XMPP transport to communicate over Google Talk.

In a command line session you should now be able to connect to the *transport worker* via Telnet::

    $ telnet localhost 9010

If you keep an eye on the *transport worker's* output you should see the following as clients connect::

    2012-03-06 12:06:32+0200 [twisted.internet.protocol.ServerFactory] Registering client connected from '127.0.0.1:57995'

.. note::

    At this point only the *transport worker* is running so Telnet input will not be processed yet. To process the input and generate an echo we need to start the *application worker*.

In a new command line session start the echo *application worker* by executing the following command::

    $ twistd -n --pidfile=applicationworker.pid vumi_worker --worker-class vumi.demos.words.EchoWorker --set-option=transport_name:telnet

This utilizes Twisted_ to start a Vumi ``EchoWorker`` process connected to the previously created Telnet *transport worker*.

.. admonition:: Philosophy

    An *application worker* is responsible for processing messages received from a *transport worker* and generating replies - it holds the application logic. For this example we are using an *echo* worker that will simply echo messages it receives back to the *transport worker*. In subsequent parts of this tutorial we'll be utilizing A.I. to generate *seemingly intelligent* replies.

Now if you enter something in your previously created Telnet session you should immediately receive an *echo*. The *application worker's* output should reflect the activity, for example when entering ``hallo world``::

    2012-03-06 12:10:39+0200 [WorkerAMQClient,client] User message: hallo world


That concludes part 1 of this tutorial. In :doc:`part 2</intro/tutorial02>` we'll be creating a `Google Talk`_ chat bot.

.. _`#vumi`: irc://irc.freenode.net/vumi
.. _Google Talk: https://www.google.com/talk/
.. _Python: https://python.org/
.. _RabbitMQ: https://www.rabbitmq.com/
.. _Twisted: https://twistedmatrix.com/trac/
.. _VirtualEnv: https://pypi.python.org/pypi/virtualenv
