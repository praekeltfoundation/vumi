Dynamic Workers
===============

Currently a transport is automatically bound to a single account, this is undesirable. We are going to split this up into:

1. A transport type factory
2. A transport type instance

Transport Type Factory
**********************

A transport type factory listens for messages over AMQP that carry the configuration encoded as a JSON dictionary. Once a message is received and it has all the necessary required details it will start a new instance of its defined type using the given credentials. 

This is typically implemented as a twistd plugin. Examples of this are an SMPP transport, a Twitter transport and an XMPP transport.

There should be a maximum number of instances that a single factory can start to prevent a single instance becoming 

Transport Type Instance
***********************

This is a python object inside the twistd plugin process. Once a Factory receives a JSON configuration dictionary it'll start a new instance of the transport type using that configuration.

In pseudocode, this is what it looks like::
    
    class SMPPTransport(Transport):
        
        def __init__(self, configuration):
            self.connect(config.get('host'), config.get('port'))
            self.authenticate(config.get('username'), config.get('password'),
                                config.get('system_id'))
        
        def connect(self, host, port):
            ...
        
        def authenticate(self, username, password, system_id):
            ...
        
    
    class SMPPTransportFactory(Factory):
        def start(self, configuration):
            return SMPPTransport(configuration)
        
    
If given the following configuration JSON dictionaries it would start two connections to two SMPP gateways:

example SMPP configuration message::

    {
        "host": "smpp.host.com",
        "port": "2773",
        "username": "account1",
        "password": "password",
        "system_id": "abc"
    }

The Factory would receive these messages from Blinkenlights over AMQP. Each instance would report back to Blinkenlights on its general health.