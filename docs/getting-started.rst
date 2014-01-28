.. How to start running and using Vumi

First steps with Vumi
=====================

The simplest Vumi system consists of a *transport worker* and an
*application worker*.

A *transport worker* is responsible for sending messages to and
receiving messages from users in the big wide world. For this example
we'll be using a very simple transport that interacts with a user over
telnet. Other transport mechanisms Vumi supports include SMPP, XMPP,
Twitter, IRC, HTTP and a variety of mobile network aggregator specific
messaging protocols.

The *application worker* processes messages from a transport and sends
replies -- it holds the application logic. Our application worker will
simply echo messages it receives back to the user than sent them. More
complicated demonstration application workers are available in
:mod:`vumi.demos`.

It is also possible for application logic to be kept external to Vumi
and to communicate with Vumi via its HTTP relay application (see
:mod:`vumi.application.http_relay`).

Bigger systems will include multiple transport workers and application
workers and also *failure workers* and *dispatchers* but we will
ignore these for the moment.

Vumi workers communicate over *RabbitMQ* so first ensure that the
RabbitMQ server is installed and running. Next setup RabbitMQ for
Vumi using::

  sudo ./utils/rabbitmq.setup.sh

from your clone of the Vumi repository. You should now be ready to
start the Vumi workers.

Open a terminal window and start the transport worker by running::

  twistd -n --pidfile=telnettransport.pid vumi_worker --worker-class vumi.transports.telnet.TelnetServerTransport --set-option=transport_name:telnet --set-option=telnet_port:9010

If all is well, open a second terminal window and start the application worker::

  twistd -n --pidfile=echoworker.pid vumi_worker --worker-class vumi.demos.words.EchoWorker --set-option=transport_name:telnet 

Your first Vumi setup should now be running. You can test it by
opening a third window and connecting with a telnet client::

  telnet localhost 9010

Lines you type should be echoed back to you.
