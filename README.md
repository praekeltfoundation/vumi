Vumi
====

PubSub platform for connecting online messaging services such as SMS and USSD to a horizontally scalable backend of workers.

Getting started
---------------

Make sure you have your AMQP broker running. I've only tested it with RabbitMQ, in theory, it should work with any other AMQP 0.8 spec based broker.

    $ rabbitmq-server

RabbitMQ will automatically assign a node name for you. For my network that doesn't work too well because the rest of the clients are unable to connect. If you run into the same problem, try the following:

    $ RABBITMQ_NODENAME=rabbit@localhost rabbitmq-server

Make sure you have configured your login credentials & virtual host stuff in RabbitMQ. This is the minimal stuff for this to work 'out of the box':

    $ rabbitmqctl -n rabbit@localhost add_user vumi vumi
    Creating user "vumi" ...
    ...done.
    $ rabbitmqctl -n rabbit@localhost add_vhost /vumi
    Creating vhost "vumi" ...
    ...done.
    $ rabbitmqctl -n rabbit@localhost set_permissions -p /vumi vumi \
        '.*' '.*' '.*'
    Setting permissions for user "vumi" in vhost "vumi" ...
    ...done.
 
That last line gives the user 'vumi' on virtual host 'vumi' configure, read & write access to all resources that match those three regular expressions. Which, in this case, matches all resources in the vhost.

This project uses [virtualenv][virtualenv] and [pip][pip] to to create a sandbox and manage the required libraries at the required versions. Make sure you have both installed.

Setup a virtual python environment in the directory `ve`. The `--no-site-packages` makes sure that all required dependencies are installed your the virtual environments `site-packages` directory even if they exist in Python's global `site-packages` directory.

    $ virtualenv --no-site-packages ./ve/ 

Start the environment by sourcing `activate`. This'll prepend the name of the virtual environment to your shell prompt, informing you that the virtual environment is active.

        $ source ve/bin/activate

When you're done run `deactivate` to exit the virtual environment.

Install the required libraries with pip into the virtual environment. They're pulled in from both [pypi][pypi] and [GitHub][github]. Make sure you have the development package for python (python-dev or python-devel or something of that sort) installed, Twisted needs it when it's being built.

    $ pip -E ./ve/ install -r config/requirements.pip
 
Running Vumi
------------

Vumi is implemented using a [Pub/Sub][pubsub] design using the [Competing Consumer pattern][competing consumers]. 

Vumi transports and workers are both started with the `start_worker` Twisted plugin.

Vumi currently has a TruTeq transport that allows for receiving and sending of USSD messages over TruTeq's SSMI protocol. The TruTeq service connects to the SSMI service and connects to RabbitMQ. It publishes all incoming messages over SSMI as JSON to the receive queue in RabbitMQ and it publishes all incoming messages over the send queue back to TruTeq over SSMI.

The worker reads all incoming JSON objects on the receive queue and publishes a response back to the send queue for the TruTeq service to publish over SSMI.

Make sure you update the configuration file in `config/truteq.cfg` and start the broker:

    $ source ve/bin/activate
    (ve)$ twistd -n \
            --pidfile=./tmp/pids/ussd_transport.pid \
            --logfile=./logs/ussd_transport.log \
            start_worker \ 
            --worker_class=richmond.workers.truteq.transport.USSDTransport \
            --config=environments/truteq.yaml
    ...
 
    $ source ve/bin/activate
    (ve)$ twistd -n \
            --pidfile=./tmp/pids/default_demo_worker.pid \
            --logfile=./logs/default_demo_worker.log \
            start_worker \
            --worker_class=richmond.campaigns.default_demo.USSDWorker
    ...

The worker's `--worker_class` option allows you to specify a class that subclasses `vumi.service.Worker`.

Remove the `-n` option to have `twistd` run in the background. The `--pidfile` option isn't necessary, `twistd` will use 'twistd.pid' by default. However, since we could have multiple brokers and workers running at the same time on the same machine it is good to be explicit since `twistd` will assume an instance is already running if 'twistd.pid' already exists.

Running the Webapp / API
------------------------

The webapp is a regular Django application. Before you start make sure the `DATABASE` settings in `src/vumi/webapp/settings.py` are up to date. `Vumi` is being developed with `PostgreSQL` as the default backend for the Django ORM but this isn't a requirement.

For development start it within the virtual environment:

    $ source ve/bin/activate
    (ve)$ python setup.py develop
    (ve)$ ./manage.py syncdb
    (ve)$ ./manage.py runserver
    ...

For development it sometimes is handy to have Celery run in eager mode. In eager mode, tasks avoids the queue entirely and are processed immediately in the main process. Do this by settings the environment variable 'VUMI_SKIP_QUEUE'

    (ve)$ VUMI_SKIP_QUEUE=True ./manage.py runserver

This is specified in the `settings.py` file, if so desired, you can also default it to `DEBUG` so that when `DEBUG=True` the queue will always be skipped.

When running in production start it with the `twistd` plugin `vumi_webapp`
 
    $ source ve/bin/activate
    (ve)$ twistd --pidfile=tmp/pids/vumi.webapp.pid -n vumi_webapp

Run the tests for the webapp API with `./manage.py` as well:

    $ source ve/bin/activate
    (ve)$ ./manage.py test api

Scheduling SMS for delivery via the API
---------------------------------------

The API is HTTP with concepts borrowed from REST. All URLs have rate limiting and require HTTP Basic Authentication.

There are currently a number of SMS transports available. [Clickatell][clickatell], [Opera][opera], [E-Scape][e-scape] and [Techsys][Techsys]. The API for both is exactly the same, just replace '/clickatell/' with '/opera/' or any of the others in the URL.

Sending via Clickatell:

    http://localhost:8000/api/v1/sms/clickatell/send.json

Sending via Opera:

    http://localhost:8000/api/v1/sms/opera/send.json


**Sending SMSs**

    $ curl -u 'username:password' -X POST \
    >   http://localhost:8000/api/v1/sms/clickatell/send.json \
    >   -d 'to_msisdn=27123456789' \
    >   -d 'from_msisdn=27123456789' \
    >   -d 'message=hello world'
    [
        {
            "delivered_at": "2010-05-13 11:34:34", 
            "id": 5, 
            "from_msisdn": "27123456789", 
            "to_msisdn": "27123456789", 
            "transport_status": 0, 
            "message": "hello world"
        }
    ]

**Sending Batched SMSs**

Sending multiple SMSs is as simple as sending a simple SMS. Just specify multiple values for `to_msisdn`.

    $ curl -u 'username:password' -X POST \
    >   http://localhost:8000/api/v1/sms/clickatell/send.json \
    >   -d 'to_msisdn=27123456780' \
    >   -d 'to_msisdn=27123456781' \
    >   -d 'to_msisdn=27123456782' \
    >   -d 'from_msisdn=27123456789' \
    >   -d 'message=hello world'
    [
        {
            "delivered_at": "2010-05-13 11:32:22", 
            "id": 2, 
            "from_msisdn": "27123456789", 
            "to_msisdn": "27123456780", 
            "transport_status": 0, 
            "message": "hello world"
        }, 
        {
            "delivered_at": "2010-05-13 11:32:22", 
            "id": 3, 
            "from_msisdn": "27123456789", 
            "to_msisdn": "27123456781", 
            "transport_status": 0, 
            "message": "hello world"
        }, 
        {
            "delivered_at": "2010-05-13 11:32:22", 
            "id": 4, 
            "from_msisdn": "27123456789", 
            "to_msisdn": "27123456782", 
            "transport_status": 0, 
            "message": "hello world"
        }
    ]

**Sending Personalized SMSs**

Personalized SMSs can be sent by specifying a template and the accompanying variables.

All template variables should be prefixed with 'template_'. In the template you can refer to the values without their prefix.

    $ curl -u 'username:password' -X POST \
    > http://localhost:8000/api/v1/sms/clickatell/template_send.json \
    > -d 'to_msisdn=27123456789' \
    > -d 'to_msisdn=27123456789' \
    > -d 'to_msisdn=27123456789' \
    > -d 'from_msisdn=27123456789' \
    > -d 'template_name=Simon' \
    > -d 'template_surname=de Haan' \
    > -d 'template_name=Jack' \
    > -d 'template_surname=Jill' \
    > -d 'template_name=Foo' \
    > -d 'template_surname=Bar' \
    > -d 'template=Hello {{name}} {{surname}}'
    [
        {
            "delivered_at": "2010-05-14 04:42:09", 
            "id": 6, 
            "from_msisdn": "27123456789", 
            "to_msisdn": "27123456789", 
            "transport_status": 0, 
            "message": "Hello Foo Bar"
        }, 
        {
            "delivered_at": "2010-05-14 04:42:09", 
            "id": 7, 
            "from_msisdn": "27123456789", 
            "to_msisdn": "27123456789", 
            "transport_status": 0, 
            "message": "Hello Jack Jill"
        }, 
        {
            "delivered_at": "2010-05-14 04:42:09", 
            "id": 8, 
            "from_msisdn": "27123456789", 
            "to_msisdn": "27123456789", 
            "transport_status": 0, 
            "message": "Hello Simon de Haan"
        }
    ]

Checking the status of sent SMSs
--------------------------------

Once an SMS has been scheduled for sending you can check it's status via the API. There are 3 options of retrieving previously sent SMSs.

**Retrieving one specific SMS**

    $ curl -u 'username:password' -X GET \
    > http://localhost:8000/api/v1/sms/clickatell/status/1.json \
    {
        "delivered_at": null, 
        "created_at": "2010-05-14 16:31:01", 
        "updated_at": "2010-05-14 16:31:01", 
        "transport_status_display": "", 
        "from_msisdn": "27123456789", 
        "id": 1, 
        "to_msisdn": "27123456789", 
        "message": "testing api", 
        "transport_status": 0
    }

**Retrieving SMSs sent since a specific date**

    $ curl -u 'username:password' -X GET \
    > http://localhost:8000/api/v1/sms/clickatell/status.json?since=2009-01-01
    [
        {
            "delivered_at": null, 
            "created_at": "2010-05-14 16:31:01", 
            "updated_at": "2010-05-14 16:31:01", 
            "transport_status_display": "", 
            "from_msisdn": "27123456789", 
            "id": 51, 
            "to_msisdn": "27123456789", 
            "message": "testing api", 
            "transport_status": 0
        }, 
        ...
        ...
        ...
    ]

**Retrieving SMSs by specifying their IDs**

    $ curl -u 'username:password' -X GET \
    > "http://localhost:8000/api/v1/sms/clickatell/status.json?id=3&id=4"
    [
        {
            "delivered_at": null, 
            "created_at": "2010-05-14 16:31:01", 
            "updated_at": "2010-05-14 16:31:01", 
            "transport_status_display": "", 
            "from_msisdn": "27123456789", 
            "id": 4, 
            "to_msisdn": "27123456789", 
            "message": "testing api", 
            "transport_status": 0
        }, 
        {
            "delivered_at": null, 
            "created_at": "2010-05-14 16:31:01", 
            "updated_at": "2010-05-14 16:31:01", 
            "transport_status_display": "", 
            "from_msisdn": "27123456789", 
            "id": 3, 
            "to_msisdn": "27123456789", 
            "message": "testing api", 
            "transport_status": 0
        }
    ]
    
Specifying Callbacks
--------------------

There are two types of callbacks defined. These are `sms_received` and `sms_receipt`. Each trigger an HTTP POST to the given URLs.


    $ curl -u 'username:password' -X POST \
    > http://localhost:8000/api/v1/account/callbacks.json \
    > -d 'name=sms_received' \
    > -d 'url=http://localhost/sms/clickatell/received/callback'
    {
        "name": "sms_received", 
        "url": "http://localhost/sms/clickatell/received/callback", 
        "created_at": "2010-07-22 21:27:24", 
        "updated_at": "2010-07-22 21:27:24", 
        "id": 3
    }
    
    $ curl -u 'username:password' -X POST \
    > http://localhost:8000/api/v1/account/callbacks.json \
    > -d 'name=sms_receipt' \
    > -d 'url=http://localhost/sms/clickatell/receipt/callback'
    {
        "name": "sms_receipt", 
        "url": "http://localhost/sms/clickatell/receipt/callback", 
        "created_at": "2010-07-22 21:32:33", 
        "updated_at": "2010-07-22 21:32:33", 
        "id": 4
    }
    
The next time an SMS is received or a SMS receipt is delivered, Vumi will post the data to the URLs specified.

Accepting delivery receipts from the transports
-----------------------------------------------

Both [Clickatell][clickatell] and [Opera][opera] support notification of an SMS being delivered. In the general configuration areas of both sites there is an option where a URL callback can be specified. Clickatell or Opera will then post the delivery report to that URL.

Vumi will accept delivery reports from both:

For [Clickatell][clickatell]:

    http://localhost:8000/api/v1/sms/clickatell/receipt.json

For [Opera][opera]:

    http://localhost:8000/api/v1/sms/opera/receipt.json

Accepting inbound SMS from the transports
-----------------------------------------

Like the SMS delivery reports, both [Opera][opera] and [Clickatell][clickatell] will forward incoming SMSs to Vumi. 

For Clickatell the URL is:

    http://localhost:8000/api/v1/sms/clickatell/receive.json

For Opera the URL is:

    http://localhost:8000/api/v1/sms/opera/receive.xml

Note the XML suffix on the URL. The resource returns XML whereas Clickatell returns JSON. This is important! Opera can forward our response to further callbacks in their application and it needs to be formatted as XML for upstream callbacks to make sense of it.

Webapp Workers
--------------

Vumi uses [Celery][celery], the distributed task queue. The main Django process only registers when an SMS is received,sent or when a delivery report is received. The real work is done by the Celery workers.

Start the Celery worker via `manage.py`:

    (ve)$ ./manage.py celeryd
    
For a complete listing of the command line options available, use the help command:

    (ve)$ ./manage.py help celeryd


[virtualenv]: http://pypi.python.org/pypi/virtualenv
[pip]: http://pypi.python.org/pypi/pip
[pypi]: http://pypi.python.org/pypi/
[GitHub]: http://www.github.com/
[pubsub]: http://en.wikipedia.org/wiki/Publish/subscribe
[competing consumers]: http://www.eaipatterns.com/CompetingConsumers.html
[celery]: http://ask.github.com/celery
[clickatell]: http://clickatell.com
[opera]: http://operainteractive.co.za/
[Techsys]: http://www.techsys.co.za/
[E-Scape]: http://www.escapetech.net/