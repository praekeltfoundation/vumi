#!/bin/bash

curl -XPUT \
    -d 'data1' \
    -H "x-riak-index-field1_bin: val1" \
    'http://127.0.0.1:8098/riak/mybucket/foo%3Abar'

curl -v http://localhost:8098/buckets/mybucket/index/field1_bin/val1; echo ""
