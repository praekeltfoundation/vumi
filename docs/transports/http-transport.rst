HTTP Transport
==============

A simple API for submitting Vumi messages into Vumi.

Notes
~~~~~

Default allowed keys:

	* content
	* to_addr
	* from_addr

Others can be allowed by specifying the `allowed_fields` in the configuration file.

There is no limit on the length of the content so if you are publishing to a length constrained transport such as SMS then you are responsible for limiting the length appropriately.

If you expect a reply from the `Application` that is dealing with these requests then set the `reply_expected` boolean to `true` in the config file. That will keep the HTTP connection open until a response is returned. The `content` of the reply message is used as the HTTP response body.

Configuration parameters
~~~~~~~~~~~~~~~~~~~~~~~~

::

    transport_name: http_transport
    web_path: /a/path/
    web_port: 8123
    reply_expected: false
    allowed_fields:
    	- content
    	- to_addr
    	- from_addr
    	- provider
    field_defaults:
    	transport_type: http


The API implementation can be found here https://github.com/praekelt/vumi/blob/develop/vumi/transports/api/api.py