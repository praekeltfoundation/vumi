import psycopg2
import sys

from vumi.webapp.api import utils


# This script loads a voucher list for a single provider
# It assumes a file of voucher strings, one per line, with no other content
# It updates the ikhwezi_winner table, starting at a given provider_voucher_number

provider = sys.argv[1]
count_from = int(sys.argv[2])
filename = sys.argv[3]
print "provider = %s" % provider
print "count_from = %s" % count_from
print "file = %s" % filename

f = open(filename)
fstring = f.read()
f.close
flist = fstring.split('\n')
flist.pop(-1)

params = [{"provider_voucher_number": count_from+i, "voucher": v} for i,v in enumerate(flist)]
for i in params:
    print i


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

for p in params:
    rs = rowset(the_conn, presql=[
            """
            UPDATE ikhwezi_winner
            SET voucher = '%s'
            WHERE provider_voucher_number = %s
            AND provider = '%s'
            """ % (
                p['voucher'],
                p['provider_voucher_number'],
                provider)],
            commit = True
            )
