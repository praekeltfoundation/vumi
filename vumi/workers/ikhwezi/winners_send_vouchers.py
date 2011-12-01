import psycopg2
from datetime import datetime

from vumi.webapp.api import utils


NOW = datetime.utcnow()

# This script sends actual vouchers for those winners allocated today
# The actuall send is commented out 


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

url = "http://ikhwezi:ikkystuff@vumi.praekeltfoundation.org/api/v1/sms/send.json"

params = [
    ("from_msisdn", "27000000000"),
]

recharge_prefix = {
        "Vodacom": "*100*01*",
        "MTN": "*141*",
        "CellC": "*102*",
        "Virgin": "*102*",
        "Telkom_8ta": "*188*",
        }


rs = rowset(the_conn, """
        SELECT
            provider_voucher_number,
            msisdn,
            voucher,
            provider
        FROM ikhwezi_winner
        WHERE allocated_at > '%s'
        """ % (NOW.date()))
for r in rs:
    params.append(("to_msisdn", r['msisdn']))
    params.append(("message",
        "U Won R10 airtime on the HIV/AIDS Quiz. Dial %s%s# to recharge" % (recharge_prefix[r['provider']], r['voucher'])))

for i in params:
    print i
print "Winners to send vouchers: %s" % (len(rs))

#url, resp = utils.callback(url, params)

#print url
#print repr(resp)

