#!/usr/bin/python

# A simple script to load some test messages into RabbitMQ for vumi smpp


from amqplib import client_0_8 as amqp
import json
import re
import time


def handler(msg):
    try:
        gpjm = json.loads(msg.body)
        basic_handler(gpjm)
    except:
        pass


def basic_handler(gpjm):
    print '\nReceived: ' + json.dumps(gpjm)


THIS_ROUTE = "smpp"


conn = amqp.Connection(host="localhost", userid="vumi", password="vumi", virtual_host="/vumi")
chan = conn.channel()
#chan.queue_declare(queue=THIS_ROUTE, durable=False, exclusive=False, auto_delete=True)
#tag = chan.basic_consume(queue=THIS_ROUTE, no_ack=True, callback=handler)
print "connected"

i = 1
while i > 0:
    msg = amqp.Message('{"msisdn":"27999900001", "message":"Sample message, testing, testing, 123..."}')
    chan.basic_publish(msg, routing_key="smpp")
    i -=1
print "published"

#time.sleep(5)

#while True:
    #chan.wait()

#chan.basic_cancel(tag)
chan.close()
conn.close()

