=========================================
ScaleConf Workshop - General Introduction
=========================================

.. note::

    These instructions were written for the first Vumi workshop on the 21st
    of April 2013, right after the ScaleConf_ conference in Cape Town.

    Spotted an error? Please feel free to contribute to the `documentation
    <https://github.com/praekelt/vumi/>`_.


What is Vumi?
=============

Vumi is a scalable, multi channel messaging platform. It has been designed to
allow large scale mobile messaging in the majority world. It is actively being
developed by the `Praekelt Foundation`_ and other contributors.
It is available as Open Source software under the BSD license.

What were the design goals?
===========================

The `Praekelt Foundation`_ has a lot of experience building mobile messaging
campaigns in the areas such as mobile health, education and democracy.
Unfortunately, a lot of this experience comes from having built systems that
caused problems in terms of scale and/or maintenance.

Key learnings from these mistakes led to a number of guiding principles in
the design of Vumi, such as:

1.  The campaign application logic should be decoupled from how it
    communicates with the end user.
2.  The campaign application and the means of communication with the end-user
    should each be re-usable in a different context.
3.  The system should be able to scale by adding more commodity machines,
    i.e. it should `scale horizontally <http://en.wikipedia.org/wiki/
    Scalability#Horizontal_and_vertical_scaling>`_.

The above mentioned guiding principles resulted in a number of core concepts
that make up a Vumi application.

A Vumi Message
~~~~~~~~~~~~~~

A Vumi Message is the means of communication inside Vumi. Esentially a Vumi
Message is just a bit of `JSON`_ that contains information on where a message
was received from, who it was addressed to, what the message contents were
and some extra metadata to allow it to be routed from and end-user to an
application and back again.

Transports
~~~~~~~~~~

Transports provide the communication channel to end users by integrating into
various services such as chat systems, mobile network operators or possibly
even traditional voice phone lines.

Transports are tasked with translating an inbound request into a standardized
Vumi Message and vice-versa.

A simple example would be an SMS, which when received is converted into a bit
of JSON_ that looks something like this:

.. literalinclude:: ../transports/sample-inbound-message.json

Applications
~~~~~~~~~~~~

Applications are tasked with either generating messages to be sent to or
acting on the messages received from end users via the transports.

As a general rule the Applications should not care about which transport
the message was received from, it merely acts on the message contents and
provides a suitable reply.

A reply message looks something like this:

.. literalinclude:: ../applications/sample-reply-to-message.json

Dispatchers
~~~~~~~~~~~

Dispatchers are an optional means of connecting Transports and Applications.
They allow for more complicated routing between the two.

A simple scenario is an application that receives from a USSD transport but
requires the option of also replying via an SMS transport. A dispatcher would
allow one to contruct this.

Dispatchers do this by inspecting the messages exchanged between the Transport
and the Application and then deciding where it needs to go.

::

    +----------------+
    | SMS Transport  |<----+   +------------+    +-------------+
    +----------------+     +-->|            |    |             |
                               | Dispatcher |<-->| Application |
    +----------------+     +-->|            |    |             |
    | USSD Transport |<----+   +------------+    +-------------+
    +----------------+


How does it work?
=================

All of these different components are built using the Python_ programming
language using Twisted_, an event driven networking library.

The messages between the different components are exchanged and routed using
RabbitMQ_ a high performance AMQP_ message broker.

For data storage Redis_ is used for data that are generally temporary but and
may potentially be lost. Riak_ is used for things that need strong
availability guarantees.

A sample use case of Redis_ would be to store session state whereas Riak_
would be used to store all messages sent and received indefinitely.

Supervisord_ is used to manage all the different processes and provide any
easy commandline tool to start and stop them.

Let's get started!
==================

As part of the workshop we will provide you with a South African USSD code
and an SMS longcode. In the next section we'll help you get Vumi
:doc:`running on your local machine</intro/scaleconf02>` so you can
start developing your first application!


.. _ScaleConf: http://www.scaleconf.org/
.. _`Praekelt Foundation`: http://www.praekeltfoundation.org/
.. _JSON: http://en.wikipedia.org/wiki/JSON
.. _Python: http://www.python.org
.. _Twisted: http://www.twistedmatrix.com/
.. _RabbitMQ: http://www.rabbitmq.com/
.. _AMQP: http://en.wikipedia.org/wiki/AMQP
.. _Redis: http://www.redis.io/
.. _Riak: http://www.basho.com/riak
.. _Supervisord: http://www.supervisord.org/
