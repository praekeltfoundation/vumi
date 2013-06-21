.. _first-smpp-bind:

Forwarding SMSs from an SMPP bind to a URL
==========================================

A simple use case for Vumi is to aggregate incoming SMSs and forward them via HTTP POST to a URL.

In this use case we are going to:

    1. Use a SMSC simulator for local development.
    2. Configure Vumi accept all incoming and outgoing messages on an SMPP bind.
    3. Setup a worker that forwards all incoming messages to a URL via HTTP POST.
    4. Setup Supervisord to manage all the different processes.

.. note::

    Vumi relies for a large part on AMQP for its routing capabilities and some basic understanding is assumed. Have a look at http://blog.springsource.com/2010/06/14/understanding-amqp-the-protocol-used-by-rabbitmq/ for a more detailed explanation of AMQP.


Installing the SMSC simulator
-----------------------------

Go to the `./utils` directory in the Vumi repository and run the bash script called `install_smpp_simulator.sh`. This will install the SMSC simulator from http://seleniumsoftware.com on your local machine. This simulator does exactly the same as a normal SMSC would do with the exception that it doesn't actually relay the messages to mobile networks.::

    $ cd ./utils
    $ ./install_smpp_simulator.sh

This will have installed the application in the `./utils/smppsim/SMPPSim` directory.

By default the SMPP simulator tries to open port 88 for it's HTTP console, since you often need administrative rights to open ports lower than 1024 let's change that to 8080 instead.

Line 60 of `./utils/smppsim/SMPPSim/conf/smppsim.props` says::

    HTTP_PORT=88

Change this to::

    HTTP_PORT=8080

Another change we need to make is on line 83::

    ESME_TO_ESME=TRUE

Needs to be changed to, FALSE::

    ESME_TO_ESME=FALSE

Having this set to True sometimes causes the SMSC and Vumi to bounce messages back and forth without stopping.

.. note::
    The simulator is a Java application and we're assuming you have Java installed correctly.

Configuring Vumi
----------------

Vumi applications are made up of at least two components, the **Transport** which deals with in & outbound messages and the **Application** which acts on the messages received and potentially generates replies.

SMPP Transport
~~~~~~~~~~~~~~

Vumi's SMPP Transport can be configured by a YAML file, `./config/example_smpp.yaml`. For this example, this is what our SMPP configuration looks like:

.. literalinclude:: ../config/example_smpp.yaml

The SMPP Transport publishes inbound messages in Vumi's common message format and accepts the same format for outbound messages.

Here is a sample message:

.. literalinclude:: transports/sample-inbound-message.json

HTTP Relay Application
~~~~~~~~~~~~~~~~~~~~~~

Vumi ships with a simple application which forwards all messages it receives as JSON to a given URL with the option of using HTTP Basic Authentication when doing so. This application is also configured using the YAML file:

.. literalinclude:: ../config/example_http_relay.yaml

Setting up the webserver that responds to the HTTP request that the `HTTPRelayApplication` makes is left as an exercise for the reader. The `HTTPRelayApplication` has the ability to automatically respond to incoming messages based on the HTTP response received.

To do this:

    1. The resource must return with a status of 200
    2. The resource must set an HTTP Header `X-Vumi-HTTPRelay-Reply` and it must be set to `true` (case insensitive)
    3. Any content that is returned in the body of the response is sent back as a message. If you want to limit this to 140 characters for use with SMS then that is the HTTP resource's responsibility.

Supervisord!
------------

Let's use Supervisord to ensure all the different parts keep running.
Here is the configuration file `supervisord.example.conf`:

.. literalinclude:: ../etc/supervisord.example.conf

Ensure you're in your python `virtualenv` and start it with the following command::

    $ supervisord -c etc/supervisord.example.conf

You'll be able to see the HTTP management console at http://localhost:9010/ or at the command line with::

    $ supervisorctl -c etc/supervisord.example.conf

Let's give it a try:
--------------------

1. Go to http://localhost:8080 and send an SMS to Vumi via "Inject an MO message".
2. Type a message, it doesn't matter what `destination_addr` you chose, all incoming messages will be routed using the SMPP Transport's `transport_name` to the application subscribed to those messages. The HTTPRelayApplication will HTTP POST to the URL provided.
