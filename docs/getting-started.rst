.. How to start running and using Vumi

First steps with Vumi
=====================

Starting a first Vumi instance::

  twistd -n --pidfile=echoworker.pid start_worker --worker-class vumi.demos.words.EchoWorker --set-option=transport_name:telnet &
  twistd -n --pidfile=telnettransport.pid start_worker --worker-class vumi.transports.telnet.TelnetServerTransport --set-option=transport_name:telnet --set-option=telnet_port:9010 &
  telnet localhost 9010
