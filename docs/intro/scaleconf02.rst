=========================================
ScaleConf Workshop - Getting Set Up
=========================================

.. note::

    These instructions were written for the first Vumi workshop on the 21st
    of April 2013, right after the ScaleConf_ conference in Cape Town.

    Spotted an error? Please feel free to contribute to the `documentation
    <https://github.com/praekelt/vumi/>`_.


Installation
============

If you're feeling particularly brave you can by all means set up Vumi from
scratch on your local machine by cloning the GitHub repository_ and installing
all the necessary packages.

However we would *strongly* recommend you use the VirtualBox_ image that's
been ready made for the workshop in combination with Vagrant_.
It has all of the necessary dependencies already installed for you on an
Ubuntu Precise machine image.

Using Vagrant & VirtualBox
==========================

Ensure you have the latest versions of both Vagrant and VirtualBox installed
before starting.

To add and start the vumi dev box do the following::

    $ git clone git://github.com/praekelt/vumi.git
    $ cd vumi
    $ vagrant box add vumi ~/path/to/the/vumi-dev.box
    $ vagrant up

When this completes you'll have a virtual machine running Ubuntu Precise 64
with the Vumi_ repository mounted at `/var/praekelt/vumi`

Log in to your machine with::

    $ vagrant ssh

Setting up your Transport
=========================

Now things will only get interesting once you can start interacting with them
via your phone. Let's get the SMS and USSD transports set up::

    $ cd /var/praekelt/vumi/
    $ virtualenv ve
    $ source ve/bin/activate
    $ pip install -r requirements.pip

Grab a coffee while you're waiting for this to complete, it can take a while.

You should by now have received your username, password and tokens
for https://go.vumi.org/ which you are going to federate with. This will
allow you to receive USSD traffic right on your machine.

Complete the config file below with the details provided and save it
in a file called `ussd_transport.yaml`:

.. literalinclude:: ./sample-ussd-transport.yaml

Next save the following bit in a file called `supervisord.conf` in the
`etc` folder:

.. literalinclude:: ./sample-supervisord-hangman.conf

When that's done we can start things up::

    $ supervisord -c etc/supervisord.conf
    $ supervisorctl -c etc/supervisord.conf tail -f ussd_transport

Now dial the number that's associated with your conversation and you
will be prompted with a game of hangman running from your local machine.

Writing Applications
====================

In the :doc:`next section </intro/scaleconf03>` we'll be getting into
application development, starting with Python and later moving on to
Javascript.

.. _ScaleConf: http://www.scaleconf.org/
.. _VirtualBox: http://www.virtualbox.org/
.. _Vagrant: http://www.vagrantup.com/
.. _Vumi: https://github.com/praekelt/vumi/
.. _repository: https://github.com/praekelt/vumi/
