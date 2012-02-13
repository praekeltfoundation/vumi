curl -u 'vumi:vumi' \
    -X POST http://localhost:7997/api/v1/account/http \
    -d 'to_addr=27763805186' \
    -d 'from_addr=270000000000' \
    -d 'content=Hello world'

