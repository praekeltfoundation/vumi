===================================
Writing your first Vumi app, part 1
===================================

This is the first part in a series of tutorials demonstrating how to develop Vumi apps.

In this tutorial we'll be developing a simple chat bot with which you can communicate via XMPP (in particular Google Talk).

We'll assume you have a working knowledge of `Python <https://python.org/>`_ and `VirtualEnv`_.

.. admonition:: Where to get help:

    If you're having trouble at any point feel free to drop by `#vumi`_ on irc.freenode.net to chat with other Vumi users who might be able to help.

In this first part of the tutorial we'll be creating a working environment and a project skeleton. 

Environment Setup
=================

Before we proceed let's create an isolated working environment using `VirutalEnv`.

From the command line ``cd`` into a directory were you'd like to store your code then run the following command::

    $ virtualenv --no-site-packages ve

This will create a ``ve`` directory where any libraries you install will go, thus isolating your environment.
   
.. note::

    For this to work `VirtualEnv`_ needs to be installed. You can tell its installed by executing ``virtualenv`` from the command line. If that command runs successfully, with no errors `VirtualEnv`_ installed. If not you can install it by executing ``$ pip install virtualenv`` from the command line.

.. _`#vumi`: irc://irc.freenode.net/vumi
.. _`Python`: https://python.org/
.. _`VirtualEnv`: https://pypi.python.org/pypi/virtualenv

