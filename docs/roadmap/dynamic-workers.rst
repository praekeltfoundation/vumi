Dynamic Workers
===============

This has been completely rethought since the last version of this
document. (This is still very much a work in progress, so please
correct, update or argue as necessary.)

In the old system, we have a separate ``twistd`` process for each
worker, managed by supervisord. In the Brave New Dyanmic Workers
World, we will be able to start and stop arbitrary workers in a
``twistd`` process by sending a :doc:`blinkenlights` message to a
supervisor worker in that process.

Advantages:
 * We can manage Vumi workers separately from OS processes, which
   gives us more flexibility.
 * We can segregate workers for different projects/campaigns into
   different processes, which can make accounting easier.

Disadvantages:
 * We have to manage Vumi workers separately from OS processes, which
   requires more work and higher system complexity. (This is the basic
   cost of the feature, though, and it's worth it for the
   flexibility.)
 * A badly-behaved worker can take down a bunch of other workers if it
   manages to kill/block the process.


Supervisor workers
******************

.. note::
   I have assumed that the supervisor will be a worker rather than a
   static component of the process. I don't have any really compelling
   reasons either way, but making it a worker lets us coexist easily
   with the current one-worker-one-process model.

A supervisor worker is nothing more than a standard worker that
manages other workers within its process. Its responsibilites have not
yet been completely defined, but will likely the following:

 * Monitoring and reportng process-level metrics.
 * Starting and stopping workers as required.

Monitoring will use the usual :doc:`blinkenlights` mechanisms, and
will work the same way as any other worker's monitoring. The
supervisor will also provide a queryable status API to allow
interrogation via Blinkenlights. (Format to be decided.)

Starting and stopping workers will be done via Blinkenlights messages
with a payload format similar to the following::

    {
        "operation": "vumi_worker",
        "worker_name": "SMPP Transport for account1",
        "worker_class": "vumi.workers.smpp.transport.SMPPTransport",
        "worker_config": {
            "host": "smpp.host.com",
            "port": "2773",
            "username": "account1",
            "password": "password",
            "system_id": "abc",
        },
    }

We could potentially even have a hierarchy of supervisors, workers and
hybrid workers::

    process
     +- supervisor
         +- worker
         +- worker
         +- hybrid supervisor/worker
         |   +- worker
         |   +- worker
         +- worker
