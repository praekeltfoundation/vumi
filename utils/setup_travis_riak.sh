#!/bin/bash -e

RIAK_VERSION="${1?Please provide Riak version.}"
RIAK_FILENAME=riak-${RIAK_VERSION}-precise.tgz
RIAK_DOWNLOAD=$HOME/download/$RIAK_FILENAME

echo "Setting up Riak with RIAK_VERSION='${RIAK_VERSION}'"

if [ ! -f $RIAK_DOWNLOAD ]; then
    mkdir -p $HOME/download
    wget -O $RIAK_DOWNLOAD http://jerith.za.net/files/riak/$RIAK_FILENAME
fi

tar xzf $RIAK_DOWNLOAD -C $HOME/


case "${RIAK_VERSION}" in
    2.1.*)
        echo "Using Riak ${RIAK_VERSION} with v2.1 config..."
        cp utils/advanced.config $HOME/riak/etc/
        ;;
    1.4.*)
        echo "Using Riak ${RIAK_VERSION} with v1.4 config..."
        cp utils/app.config $HOME/riak/etc/
        ;;
    *)
        # Unexpected version.
        echo "I don't know how to set up Riak ${RIAK_VERSION}, sorry."
        exit 1
        ;;
esac

$HOME/riak/bin/riak start
