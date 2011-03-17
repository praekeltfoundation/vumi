The HTTP API
============

.. note::
    This documentation is in need of a update. While the features are still
    correct the underlying implementation of the transports has changed
    significantly recently. Work is underway to get the docs and the actual
    code in sync.
    
    Specifically all the SMPP and Supervisord stuff is missing from these docs.

Running the Webapp / API
------------------------

The webapp is a regular Django application. Before you start make sure the `DATABASE` settings in `src/vumi/webapp/settings.py` are up to date. `Vumi` is being developed with `PostgreSQL` as the default backend for the Django ORM but this isn't a requirement.

To setup PostgreSQL::

    $ sudo -u postgres createuser --superuser --pwprompt vumi
    ... // snip, default password is `vumi` // ...
    $ createdb -W -U vumi -h localhost -E UNICODE vumi
    $ virtualenv --no-site-packages ve
    $ pip -E ve install psycopg2

For development start it within the virtual environment::

    $ source ve/bin/activate
    (ve)$ python setup.py develop
    (ve)$ ./manage.py syncdb
    (ve)$ ./manage.py runserver
    ...

Run the tests for the webapp API with `./manage.py` as well::

    $ source ve/bin/activate
    (ve)$ ./manage.py test api

Scheduling SMS for delivery via the API
---------------------------------------

The API is HTTP with concepts borrowed from REST. All URLs have rate limiting and require HTTP Basic Authentication.

There are currently a number of SMS transports available. Clickatell_, Opera_, `E-Scape`_ and Techsys_. The API for both is exactly the same, just replace '/clickatell/' with '/opera/' or any of the others in the URL.

Sending via Clickatell::

    http://localhost:8000/api/v1/sms/clickatell/send.json

Sending via Opera::

    http://localhost:8000/api/v1/sms/opera/send.json


Sending SMSs
~~~~~~~~~~~~

::

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

Sending Batched SMSs
~~~~~~~~~~~~~~~~~~~~

Sending multiple SMSs is as simple as sending a simple SMS. Just specify multiple values for `to_msisdn`.

::

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

Sending Personalized SMSs
~~~~~~~~~~~~~~~~~~~~~~~~~

Personalized SMSs can be sent by specifying a template and the accompanying variables.

All template variables should be prefixed with 'template\_'. In the template you can refer to the values without their prefix.

::

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

Retrieving one specific SMS
~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

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

Retrieving SMSs sent since a specific date
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

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

Retrieving SMSs by specifying their IDs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

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

::

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

Both Clickatell_ and Opera_ support notification of an SMS being delivered. In the general configuration areas of both sites there is an option where a URL callback can be specified. Clickatell or Opera will then post the delivery report to that URL.

Vumi will accept delivery reports from both:

For Clickatell_:

    http://localhost:8000/api/v1/sms/clickatell/receipt.json

For Opera_:

    http://localhost:8000/api/v1/sms/opera/receipt.json

Accepting inbound SMS from the transports
-----------------------------------------

Like the SMS delivery reports, both Opera_ and Clickatell_ will forward incoming SMSs to Vumi. 

For Clickatell the URL is:

    http://localhost:8000/api/v1/sms/clickatell/receive.json

For Opera the URL is:

    http://localhost:8000/api/v1/sms/opera/receive.xml

Note the XML suffix on the URL. The resource returns XML whereas Clickatell returns JSON. This is important! Opera can forward our response to further callbacks in their application and it needs to be formatted as XML for upstream callbacks to make sense of it.

.. _Clickatell: http://clickatell.com
.. _Opera: http://operainteractive.co.za/
.. _Techsys: http://www.techsys.co.za/
.. _E-Scape: http://www.escapetech.net/
