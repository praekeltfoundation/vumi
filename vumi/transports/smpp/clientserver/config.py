# -*- test-case-name: vumi.transports.smpp.clientserver.test.test_client -*-

class ClientConfig(object):

    required = [
            'host',
            'port',
            'system_id',  # in SMPP system_id is the username
            'password',
            ]

    smpp_options = {
            'system_type': "",
            'interface_version': "34",
            'dest_addr_ton': 0,
            'dest_addr_npi': 0,
            'registered_delivery': 0,
            }

    client_options = {
            'smpp_bind_timeout': 30,
            }

    def __init__(self, **kwargs):
        self.dictionary = {}
        #print ""
        for i in self.required:
            self.dictionary[i] = kwargs[i]
            #print "%s: %s" % (i, self.dictionary[i])
        for k, v in self.smpp_options.items():
            self.dictionary[k] = kwargs.get(k, self.smpp_options[k])
            #print "%s: %s" % (k, self.dictionary[k])
        for k, v in self.client_options.items():
            self.dictionary[k] = kwargs.get(k, self.client_options[k])
            #print "%s: %s" % (k, self.dictionary[k])

    # a get method that performs like a dictionary's get method
    def get(self, key, default=None):
        try:
            return self.dictionary[key]
        except:
            return default

    def set(self, key, value):
        self.dictionary[key] = value
