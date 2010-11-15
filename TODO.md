Vumi
====

Lots obviously still needs to be done.

Some glaring issues:

1. We have two approaches to the workers, one around Django & Celery, the other around raw AMQP and USSD/SMPP workers. I would prefer these to be unified.

2. Direct SMPP connection is still being developed at http://github.com/dmaclay/python-smpp and http://github.com/dmaclay/twisted-smpp. This is currently undergoing compliancy testing at [Clickatell][clickatell].

3. The DSL for describing menus; [Alexandria][alexandria] is good in theory but highly dodgy in practice. It is one to throw away. I value the ideas but not the implementation. Needs to be reimplemented for Twisted specifically, it reinvents way too much w.r.t. it being asynchronous.

4. The database connection is now tied into [Alexandria][alexandria], bad idea.

5. Cross transport switching of messages, send an SMS during a USSD session for example.

6. Define clear request & response objects.

7. Figure out how we want to implement session storage.

8. Standardize the format and structure of messages sent over AMQP between transports & workers.

9. Set up Jira.