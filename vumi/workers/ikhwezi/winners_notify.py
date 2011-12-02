import psycopg2
import sys
from datetime import datetime

from vumi.webapp.api import utils


# This script sends winner notifications for those winners allocated today
# The actuall send is commented out 

password = sys.argv[1]
print "password = %s" % password

NOW = datetime.utcnow()


def rowset(conn, sql="SELECT 0", presql=[], commit=False):
    cursor = conn.cursor()
    for s in presql:
        #print s
        cursor.execute(s)
    if commit:
        conn.commit()
    #print sql
    cursor.execute(sql)
    result = cursor.fetchall()
    hashlist = []
    names = []
    for column in cursor.description:
        names.append(column[0])
    for row in result:
        hash = {}
        index = 0
        while index < len(names):
            hash[names[index]] = row[index]
            index+=1
        hashlist.append(hash)
    return hashlist

# This assumes a ssh tunnel like> ssh -L 5555:localhost:5432  -i /home/dmaclay/foundation/aws/praekelt_foundation_eu.pem ubuntu@vumi.praekeltfoundation.org
def conn():
    return psycopg2.connect(
            host="localhost",
            #port=5555,  # UNCOMMENT THIS FOR REMOTE DB
            user="vumi",
            password="vumi",
            database="ikhwezi")

the_conn = conn()

url = "http://ikhwezi:%s@vumi.praekeltfoundation.org/api/v1/sms/send.json" % password

params = [
    ("from_msisdn", "27000000000"),
]

rs = rowset(the_conn, """
        SELECT
            msisdn,
            message
        FROM ikhwezi_winner
        WHERE allocated_at > '%s'
        """ % (NOW.date()))
for r in rs:
    params.append(("to_msisdn", r['msisdn']))
    params.append(("message", r['message']))

for i in params:
    print i
print "Winners to notify: %s" % (len(rs))

#url, resp = utils.callback(url, params)

#print url
#print repr(resp)

