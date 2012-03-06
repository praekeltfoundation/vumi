===================================
Writing your first Vumi app, part 1
===================================

This is the first part in a series of tutorials demonstrating how to develop Vumi apps.

In this tutorial we'll be developing a simple chat bot with which you can communicate via XMPP (in particular Google Talk).

We'll assume you have a working knowledge of Python_ and VirtualEnv_.

.. admonition:: Where to get help:

    If you're having trouble at any point feel free to drop by #vumi_ on irc.freenode.net to chat with other Vumi users who might be able to help.

In this first part of the tutorial we'll be creating a working environment and a project skeleton. 

Environment Setup
=================

Before we proceed let's create an isolated working environment using VirtualEnv_.

From the command line ``cd`` into a directory were you'd like to store your code then run the following command::

    $ virtualenv --no-site-packages ve

This will create a ``ve`` directory where any libraries you install will go, thus isolating your environment.
Once the virtual environment has been created activate it by running ``. ve/bin/activate``.
   
.. note::

    For this to work VirtualEnv_ needs to be installed. You can tell it's installed by executing ``virtualenv`` from the command line. If that command runs successfully, with no errors, VirtualEnv_ is installed. If not you can install it by executing ``pip install virtualenv`` from the command line.

.. _#vumi: irc://irc.freenode.net/vumi
.. _Python: https://python.org/
.. _VirtualEnv: https://pypi.python.org/pypi/virtualenv

Now that you created and activated the virtual environment install Vumi with the following command::
    
    $ pip install -e git+git://github.com/praekelt/vumi.git@develop#egg=vumi

.. note::

    This will install the latest development version of Vumi containing the latest-and-greatest features. The development branch is kept stable but is not recommended for production environments.
