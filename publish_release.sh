#!/bin/bash

if [ -z "$TYPESENSE_VERSION" ]
then
  echo "\$TYPESENSE_VERSION is not provided. Quitting."
  exit 1
fi

set -ex
CURR_DIR=`dirname $0 | while read a; do cd $a && pwd && break; done`

aws s3 cp $CURR_DIR/build-Linux/typesense-server-${TYPESENSE_VERSION}-linux-amd64.tar.gz s3://dl.typesense.org/releases/typesense-server-${TYPESENSE_VERSION}-linux-amd64.tar.gz --profile typesense
aws s3 cp $CURR_DIR/typesense-server-${TYPESENSE_VERSION}-amd64.deb s3://dl.typesense.org/releases/ --profile typesense
aws s3 cp $CURR_DIR/build-Darwin/typesense-server-${TYPESENSE_VERSION}-darwin-amd64.tar.gz s3://dl.typesense.org/releases/typesense-server-${TYPESENSE_VERSION}-darwin-amd64.tar.gz --profile typesense
