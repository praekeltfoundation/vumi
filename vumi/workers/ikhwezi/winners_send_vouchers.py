import psycopg2
import json
import sys

from vumi.webapp.api import utils


# This script sends actual vouchers and sets the send id
# This will send vouchers where the MSISDN is set,
# the voucher is loaded, and a SMS id from VUMI is not yet set
# USSD prefixes for recharge are determined based on provider

password = sys.argv[1]
print "password = %s" % password


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
        WHERE voucher IS NOT NULL
        AND msisdn IS NOT NULL
        AND voucher_send_id IS NULL
        AND provider = 'Vodacom'
        """)

for r in rs:
    #print r
    params = [
        ("from_msisdn", "27000000000"),
    ]

    params.append(("to_msisdn", r['msisdn']))
    params.append(("message",
        "U Won R10 airtime on the HIV/AIDS Quiz. Dial %s%s# to recharge" % (recharge_prefix[r['provider']], r['voucher'])))

    for i in params:
        print i
    print ""

    url, resp = utils.callback(url, params)
    send_json = json.loads(resp)
    voucher_send_id = send_json[0]['id']
    print "Voucher sent with id: %s" % (voucher_send_id)
    rs2 = rowset(the_conn, presql=[
            """
            UPDATE ikhwezi_winner
            SET voucher_send_id = '%s'
            WHERE msisdn = '%s'
            """ % (voucher_send_id, r['msisdn'])],
            commit = True
            )

    #print url
    #print repr(resp)
    #print json.loads(resp)
print "Winners to send vouchers: %s" % (len(rs))

