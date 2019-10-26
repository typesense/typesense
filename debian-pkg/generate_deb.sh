#!/bin/sh

# TS_VERSION is passed as an environment variable to the script

set -ex
CURR_DIR=`dirname $0 | while read a; do cd $a && pwd && break; done`

rm -rf /tmp/typesense-deb-build && mkdir /tmp/typesense-deb-build
cp -r $CURR_DIR/typesense-server /tmp/typesense-deb-build

# Download Typesense, extract and make it executable

curl -o /tmp/typesense-server-$TS_VERSION.tar.gz https://dl.typesense.org/releases/typesense-server-$TS_VERSION-linux-amd64.tar.gz
rm -rf /tmp/typesense-server-$TS_VERSION && mkdir /tmp/typesense-server-$TS_VERSION
tar -xzf /tmp/typesense-server-$TS_VERSION.tar.gz -C /tmp/typesense-server-$TS_VERSION

downloaded_hash=`md5sum /tmp/typesense-server-$TS_VERSION/typesense-server | cut -d' ' -f1`
original_hash=`cat /tmp/typesense-server-$TS_VERSION/typesense-server.md5.txt`

if [ "$downloaded_hash" == "$original_hash" ]; then
    mkdir -p /tmp/typesense-deb-build/typesense-server/usr/bin
    cp /tmp/typesense-server-$TS_VERSION/typesense-server /tmp/typesense-deb-build/typesense-server/usr/bin
else
    >&2 echo "Typesense server binary is corrupted. Quitting."
    exit 1
fi

rm -rf /tmp/typesense-server-$TS_VERSION /tmp/typesense-server-$TS_VERSION.tar.gz

# generate a random API key to be used
API_KEY=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 48 | head -n 1)

sed -i "s/\$VERSION/$TS_VERSION/g" `find /tmp/typesense-deb-build -maxdepth 10 -type f`
sed -i "s/\$API_KEY/$API_KEY/g" `find /tmp/typesense-deb-build -maxdepth 10 -type f`

dpkg -b /tmp/typesense-deb-build/typesense-server "/tmp/typesense-deb-build/typesense-server-${TS_VERSION}-amd64.deb"
