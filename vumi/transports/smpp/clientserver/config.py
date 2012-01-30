
class ClientConfig(object):

    def __init__(self, **kwargs):
        self.host = kwargs['host']
        self.port = kwargs['port']
        self.system_id = kwargs['system_id']  # in SMPP system_id is the username
        self.password = kwargs['password']
        self.system_type = kwargs.get('system_type', '')
        self.interface_version = kwargs.get('interface_version', "34")
        self.dest_addr_ton = kwargs.get('dest_addr_ton', 0)
        self.dest_addr_npi = kwargs.get('dest_addr_npi', 0)
        self.registered_delivery = kwargs.get('registered_delivery', 0)

    # a get method that performs like a dictionary's get method
    def get(self, attr, default=None):
        try:
            return getattr(self, attr)
        except:
            return default
