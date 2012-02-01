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
            port=5555,  # UNCOMMENT THIS FOR REMOTE DB
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
            provider,
            provider_voucher_number,
            msisdn,
            message
        FROM ikhwezi_winner
        WHERE message IS NOT NULL
        ORDER BY provider_voucher_number
        """)
#rs = [
        #{
            #"msisdn": "27763805186",
            #"message": "hi dmaclay",
            #},
        #{
            #"msisdn": "27735939536",
            #"message": "hi hodgestar",
            #}
        #]

print "Winners to notify: %s" % (len(rs))

for r in rs:
    params = [
        ("from_msisdn", "27000000000"),
        ("to_msisdn", r['msisdn']),
        ("message", r['message']),
    ]

    print r['provider'], r['provider_voucher_number'],
    print params

    url, resp = utils.callback(url, params)
    print url
    print repr(resp)
    print ""

print "Winners to notify: %s" % (len(rs))

