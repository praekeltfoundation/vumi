import psycopg2


# This script loads the ikhwezi_winner table with empty voucher slots,
# after which ikhwezi_winner winner can be used to select & notify winners
# but the voucher column must be updated before prizes can go out

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
            port=5555,
            user="vumi",
            password="vumi",
            database="ikhwezi")

the_conn = conn()


# The number of available prize vouchers per provider
provider_prizes = {
        "Vodacom": {
            "total_winners": 2700  # was 2250
            },
        "MTN": {
            "total_winners": 1800  # was 1500
            },
        "CellC": {
            "total_winners": 900  # was 1000
            },
        "Telkom_8ta": {
            "total_winners": 300  # was 250
            },
        "Virgin": {
            "total_winners": 300  # was 0
            },
        }


def load_empty_vouchers():
    for k, v in provider_prizes.items():
        for i in range(v["total_winners"]):
            rowset(the_conn,
                    presql=["""
                        INSERT INTO ikhwezi_winner (
                            provider_voucher_number,
                            provider
                        ) VALUES (
                            %s,
                            '%s'
                        )
                        """ % (i+1, k)],
                    commit=True)

load_empty_vouchers()

