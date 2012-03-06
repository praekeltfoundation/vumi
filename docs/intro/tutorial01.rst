===================================
Writing your first Vumi app, part 1
===================================

This is the first part in a series of tutorials demonstrating how to develop Vumi apps.

In this tutorial we'll be developing a simple chat bot with which you can communicate via XMPP (in particular Google Talk).

We'll assume you have a working knowledge of `Python <https://python.org/>`_ and `VirtualEnv`_.

.. admonition:: Where to get help:

    If you're having trouble at any point feel free to drop by 
    `#vumi on irc.freenode.net`__ to chat with other Vumi users 
    who might be able to help.

__ irc://irc.freenode.net/vumi
__

In this first part of the tutorial we'll be creating a working environment and a project skeleton. 

Environment Setup
=================

Before we proceed let's create an isolated working environment using `VirutalEnv`.

From the command line ``cd`` into a directory were you'd like to store your code then run the following command:

.. code-block::bash

    $ virtualenv --no-site-packages ve

This will create a ``ve`` directory where any libraries you install will go, thus isolating your environment.
    

.. _`Python`: https://python.org/
.. _`VirtualEnv`: https://pypi.python.org/pypi/virtualenv

