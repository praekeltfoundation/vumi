class Client(object):
    
    def __init__(self, connection):
        self.connection = connection
    
    def send_sms(self, to_msisdn, from_msisdn, message):
        return {}