Accounting
==========

.. note:: Accounting at this stage is the responsibility of the campaign specific logic, this however will change over time. 

Initially Vumi takes a deliberately simple approach to accounting.

What Vumi should do now:

1. An account can be active or inactive.
2. Messaging only takes place for active accounts, messages submitted for inactive accounts are discarded and unrecoverable.
3. Every message sent or received is linked to an account.
4. Every message sent or received is timestamped.
5. All messages sent or received can be queried and exported by date per account. 

What Vumi will do in the future:

1. Send and receive messages against a limited amount of message credits.
2. Payment mechanisms in order to purchase more credits.