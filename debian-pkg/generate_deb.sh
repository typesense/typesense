#!/bin/sh

set -ex
CURR_DIR=`dirname $0 | while read a; do cd $a && pwd && break; done`

rm -rf /tmp/typesense-deb-build && mkdir /tmp/typesense-deb-build
cp -r $CURR_DIR/typesense-server /tmp/typesense-deb-build

# generate a random API key to be used
API_KEY=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 48 | head -n 1)

# TS_VERSION is passed as an environment variable to the script
sed -i "s/\$VERSION/$TS_VERSION/g" `find /tmp/typesense-deb-build -maxdepth 10 -type f`
sed -i "s/\$API_KEY/$API_KEY/g" `find /tmp/typesense-deb-build -maxdepth 10 -type f`

dpkg -b /tmp/typesense-deb-build/typesense-server "/tmp/typesense-deb-build/typesense-server_${TS_VERSION}_amd64.deb"
