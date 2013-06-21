#!/bin/bash

# Exit early if the config isn't wrong in the way we expect.
grep '{http_url_encoding, "on"}' /etc/riak/app.config || exit 0

# We have a damaged config, so fix it and restart riak.
sudo service riak stop
sudo sed -i.bak 's/{http_url_encoding, "on"}/{http_url_encoding, on}/' /etc/riak/app.config
sudo service riak start
