curl -u 'vumi:vumi' \
    -X POST http://localhost:7998/api/v1/sms/send.json \
    -d 'to_msisdn=27763805186' \
    -d 'to_msisdn=27000000000' \
    -d 'from_msisdn=270000000000' \
    -d 'message=Hello world'

