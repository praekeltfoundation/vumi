#!/bin/bash

# Exit early if the config is already correct.
grep '{http_url_encoding, on}' /etc/riak/app.config && exit 0

# Our grep failed, so fix the config and restart riak.
sudo service riak stop
sudo sed -i.bak 's/{vnode_vclocks, true}/{http_url_encoding, on}, {vnode_vclocks, true}/' /etc/riak/app.config
sudo service riak start
