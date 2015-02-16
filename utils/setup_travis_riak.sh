#!/bin/bash

# NOTE: This needs to be run as root.

RIAK_VERSION="${1?Please provide Riak version.}"

# Stop Riak (if it's running) so we can change stuff underneath it.
service riak stop


function install_riak() {
    # We're installing our own Riak version, so we add Basho's repo.
    curl http://apt.basho.com/gpg/basho.apt.key | sudo apt-key add -
    bash -c "echo deb http://apt.basho.com $(lsb_release -sc) main > /etc/apt/sources.list.d/basho.list"
    apt-get -qq update
    apt-get install -qq -y --force-yes riak="${RIAK_VERSION}"
}


case "${RIAK_VERSION}" in
    "current")
        # We want to use the existing Riak (2.0.something at time of writing).
        # We need to copy over our own advanced config to enable legacy search.
        cp utils/advanced.config /etc/riak/advanced.config
        ;;
    "1.4.*")
        # We want Riak 1.4.x, so install it and copy over a suitable config.
        install_riak
        cp utils/app.config /etc/riak/app.config
        ;;
    "*")
        # Unexpected version.
        echo "I don't know how to set up Riak ${RIAK_VERSION}, sorry."
        exit 1
        ;;
esac


# Get rid of all the existing Riak data so we start fresh and then start fresh.
rm -rf /var/lib/riak/*
sudo service riak start
