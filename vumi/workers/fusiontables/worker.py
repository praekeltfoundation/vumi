from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from vumi.service import Worker
from fusiontables.authorization.clientlogin import ClientLogin
from fusiontables.sql.sqlbuilder import SQL
from fusiontables import ftclient
from urllib2 import HTTPError
from urllib import quote


def get_or_create_table(client, table_name, structure):
    table = get_table(client, table_name)
    if table:
        return table
    else:
        return create_table(client, table_name, structure)
    
def get_table(client, table_name):
    try:
        tables = client.query(SQL().showTables())
        for line in tables.split('\n'):
            table_id, table_name = line.split(',')
            if table_id.isdigit() and table_name == table_name:
                return table_id
    except ValueError, e:
        log.err()
    
def create_table(client, table_name, structure):
    return client.query(SQL().createTable({
        table_name: structure
    })).split("\n")[1]
    

def prep(value):
    v = value or ''
    return v.replace("'","\\'")

def prep_geo(dictionary):
    if isinstance(dictionary, dict):
        return ' '.join(map(str, dictionary['coordinates']))
    return ''

class FusionTableWorker(Worker):
    
    @inlineCallbacks
    def startWorker(self):
        """start consuming json off the queue"""
        table_name = self.config['table_name']
        table_structure = self.config['table_structure']
        
        token = ClientLogin().authorize(self.config['username'], 
                                                self.config['password'])
        self.client = ftclient.ClientLoginFTClient(token)
        self.table_id = get_or_create_table(self.client, table_name, table_structure)
        consumer = yield self.consume('twitter.inbound.vumiapp.mandela', 
                        self.consume_message)
        
    
    def consume_message(self, message):
        try:
            data = message.payload['data']
            user = data.get('user', {})
            post_data = {
                "geo_location": prep_geo(data.get('geo')),
                "tweet": prep(data.get('text')),
                "created_at": prep(data.get('created_at')),
                "user_location": prep(user.get('location')),
                "user_name": prep(user.get('name')),
                "user_screen_name": prep(user.get('screen_name')),
                "user_url": prep(user.get('url')),
                "user_time_zone": prep(user.get('time_zone')),
            }
            row = self.client.query(SQL().insert(self.table_id, post_data))
            log.msg('write row: %s' % row.split("\n")[1])
            return True
        except HTTPError, e:
            log.msg(post_data)
            log.err()
            return False
    
    def stopWorker(self):
        """stop consuming json off the queue"""
        pass
    