import psycopg2
import random
import json
from datetime import datetime


NOW = datetime.utcnow()
TOTAL_SESSIONS = 200000.0
SESSIONS_TO_WIN = 0  # THE LAST TIME THIS SCRIPT IS RUN (i.e. USSD sessions all used up), SET THIS TO 0


# This script inspects the database,
# Selects winners for the various providers
# Updates the database to reflect who has won / not won
# Writes the winner details (msisdn, language, winning_message) to ikhwezi_winner

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



# Get the current unique msisdns and total sessions used
def uniques_and_sessions():
    rs = rowset(the_conn, """
            SELECT
                count(*) as uniques,
                sum(sessions) as sessions
            FROM ikhwezi_quiz
            """)
    return rs[0]['uniques'], rs[0]['sessions']

current_uniques, current_sessions = uniques_and_sessions()
fraction_complete = current_sessions/TOTAL_SESSIONS

print "\n"
print "Current Unique MSISDNs: %s" % (current_uniques)
print "Current Total Sessions: %s" % (current_sessions)
print "Expected Total Sessions: %s" % (TOTAL_SESSIONS)
print "Fraction of campaign complete: %s" % (fraction_complete)
print "Sessions requied to win: %s" % (SESSIONS_TO_WIN)


# Vodacom Messaging determines provider from their network bind
# so we need to re-label CellC msisdns starting with 27741 as Virgin
def update_virgin_users_provider():
    rowset(the_conn,
            presql=["""
                UPDATE ikhwezi_quiz
                SET Provider = 'Virgin'
                WHERE provider = 'CellC'
                AND msisdn LIKE '27741%'
                """],
            commit=True)

update_virgin_users_provider()

# Find the count of current winners on a per provider basis
# Find the count of current losers (decided non-winners)
# Find the count of candidates (people to pick winners from)
# Find the count of others (people who are still busy playing)
def winners_by_providor():
    winners = rowset(the_conn, """
            SELECT
                provider,
                count(*) as msisdns
            FROM ikhwezi_quiz
            WHERE winner = 'true'
            GROUP BY provider
            """)
    losers = rowset(the_conn, """
            SELECT
                provider,
                count(*) as msisdns
            FROM ikhwezi_quiz
            WHERE winner = 'false'
            GROUP BY provider
            """)
    candidates = rowset(the_conn, """
            SELECT
                provider,
                count(*) as msisdns
            FROM ikhwezi_quiz
            WHERE winner IS NULL
            AND sessions >= %s
            GROUP BY provider
            """ % (SESSIONS_TO_WIN))
    others = rowset(the_conn, """
            SELECT
                provider,
                count(*) as msisdns
            FROM ikhwezi_quiz
            WHERE winner IS NULL
            AND sessions < %s
            GROUP BY provider
            """ % (SESSIONS_TO_WIN))
    dct = {}
    for i in provider_prizes.keys():
        dct[i] = {
                "winners": 0,
                "losers": 0,
                "candidates": 0,
                "others": 0,
                }
    for i in winners:
        p = dct.get(i['provider'])
        if p is not None:
            dct[i['provider']]['winners'] = i['msisdns']
    for i in losers:
        p = dct.get(i['provider'])
        if p is not None:
            dct[i['provider']]['losers'] = i['msisdns']
    for i in candidates:
        p = dct.get(i['provider'])
        if p is not None:
            dct[i['provider']]['candidates'] = i['msisdns']
    for i in others:
        p = dct.get(i['provider'])
        if p is not None:
            dct[i['provider']]['others'] = i['msisdns']
    return dct

winners_dict = winners_by_providor()

# Determine how many new winners need to be allocated per provider
# = (provider_vouchers * sessions/max_sessions) - already allocated
def new_winner_counts_by_provider():
    dct = {}
    print "\n"
    for k,v in provider_prizes.items():
        print "Provider: %s" % (k)
        total = v['total_winners']
        allocated = winners_dict[k]['winners']
        remaining_wins = v['total_winners'] - allocated
        print "\tTotal Prizes: %s" % (total)
        print "\tAllocated: %s" % (allocated)
        print "\tRemaining: %s" % (remaining_wins)
        target = fraction_complete*total
        # Cap target to dispense at total - in case of too many sessions
        if target > total:
            target = total
        print "\tTarget to allocate: %s" % (target)
        dispense = int(target - allocated)
        # We can't dispense negative vouchers
        if dispense < 0:
            dispense = 0
        print "\tNew vouchers to dispense: %s" % (dispense)
        dct[k] = dispense
    print "\n"
    return dct

new_winner_counts = new_winner_counts_by_provider()

print "Required new winners by proivider:"
for k, v in new_winner_counts.items():
    print "\t%s: %s" % (k, v)

language_map = {
         "1": "English",
         "2": "Zulu",
         "3": "Afrikaans",
         "4": "Sotho"
        }

def get_language(num):
    return language_map.get(str(num), "English")


# Get the lists of candidates to win, on a per provider basis
def candidate_lists_by_provider():
    dct = {}
    for i in provider_prizes.keys():
        dct[i] = []
    for k in provider_prizes.keys():
        candidates = rowset(the_conn, """
                SELECT
                    msisdn,
                    demographic1,
                    provider
                FROM ikhwezi_quiz
                WHERE sessions >= %s
                AND winner IS NULL
                AND provider = '%s'
                """ % (SESSIONS_TO_WIN, k))
        for i in candidates:
            dct[k].append({"msisdn": i['msisdn'], "language": get_language(i['demographic1'])})
            print get_language(i['demographic1'])
    return dct

candidate_lists = candidate_lists_by_provider()


print "New candidates by provider:"
for k, v in candidate_lists.items():
    print "\t%s: %s" % (k, len(v))


# Shuffle the candidate lists
random.seed()
for k in candidate_lists.keys():
    random.shuffle(candidate_lists[k])

new_winners = {}
for k in provider_prizes.keys():
    new_winners[k] = []

# Pop the requied number of new winners off each candidate list
# and add them to the winner lists
for k in provider_prizes.keys():
    for i in range(new_winner_counts[k]):
        try:
            new_winners[k].append(candidate_lists[k].pop(0))
        except:
            pass

print "New winners by provider:"
for k, v in new_winners.items():
    print "\t%s: %s" % (k, len(v))

winner_messages = {
        "English": "Thnx 4 taking the Quiz. U have won R10 airtime! We will send U your airtime voucher. For more info about HIV/AIDS pls phone Aids Helpline 0800012322",
        "Zulu": "Siyabonga ngokuphendula ngeHIV. Uwinile! Uzothola i-SMS ne-airtime voucher. Ukuthola okwengeziwe ngeHIV/AIDS shayela i-Aids Helpline 0800012322",
        "Afrikaans": "Dankie vir jou deelname aan die vasvra! Jy het R10 lugtyd gewen! Jou lugtyd koepon is oppad! Vir meer inligting oor MIV/Vigs, bel die Vigs-hulplyn 0800012322",
        "Sotho": "Rea o leboha ka ho nka karolo ho HIV Quiz. O mohlodi! SMS e tla romelwa le voutjhara ya moya. Lesedi le leng ka HIV/AIDS, letsetsa Aids Helpline 0800012322"
        }

def add_messages_to_winners():
    for k, v in new_winners.items():
        for i in v:
            i["message"] = winner_messages[i["language"]]

add_messages_to_winners()


# Update ikhwezi_quiz setting the new winners and losers
def set_winners_and_losers():
    presql = []

    lose = []
    for k, v in candidate_lists.items():
        for i in v:
            lose.append("'%s'" % i['msisdn'])
    loser_string = ','.join(lose)
    loser_sql = """
                UPDATE ikhwezi_quiz
                SET winner = 'false'
                WHERE msisdn IN (%s)
                """ % (loser_string)
    if len(lose) > 0:
        presql.append(loser_sql)

    win = []
    for k, v in new_winners.items():
        for i in v:
            win.append("'%s'" % i['msisdn'])
    winner_string = ','.join(win)
    winner_sql = """
                UPDATE ikhwezi_quiz
                SET winner = 'true'
                WHERE msisdn IN (%s)
                """ % (winner_string)
    if len(win) > 0:
        presql.append(winner_sql)

    rowset(the_conn,
            presql=presql,
            commit=True)

set_winners_and_losers()


# Update ikhwezi_winner setting the new winners and their messages
def update_voucher_allocation():
    for k, v in new_winners.items():
        i = 0
        old_winner_count = winners_dict[k]['winners']
        for this_winner in v:
            presql = []
            i += 1
            sql = """
            UPDATE ikhwezi_winner
            SET msisdn = '%s',
                message = '%s',
                allocated_at = '%s'
            WHERE provider = '%s'
            AND provider_voucher_number = %s
            AND msisdn IS NULL
            """ % (
                    this_winner['msisdn'],
                    this_winner['message'],
                    str(NOW),
                    k,
                    i + old_winner_count
                    )
            print sql
            presql.append(sql)
            rowset(the_conn,
                    presql=presql,
                    commit=True)

update_voucher_allocation()


