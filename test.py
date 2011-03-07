
import sys

from vumi.webapp.api import utils

try:
    username = sys.argv[1]
    password = sys.argv[2]
    __msisdn = sys.argv[3]
except:
    print "You must supply a valid username, password and msisdn"
    quit()

try:
    count = int(sys.argv[4])
except:
    count = 1

print username, password, __msisdn, count


url = "http://"+username+":"+password+"@qa.vumi.praekeltfoundation.org/api/v1/sms/smpp/send.json"

params = [
        ("from_msisdn", "27123456789"),
        ("message", "Reply with 'vumi validate'"),
        ]

for to in range(count):
    params.append(("to_msisdn", __msisdn))

print url
print params

url, resp = utils.callback(url, [(p[0],str(p[1])) for p in params])

print url
print resp

