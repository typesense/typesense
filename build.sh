#!/bin/sh

set -ex
PROJECT_DIR=`dirname $0 | while read a; do cd $a && pwd && break; done`
SYSTEM_NAME=$(uname -s)

if [[ "$@" == *"--clean"* ]]; then
  echo "Cleaning..."
  rm -rf $PROJECT_DIR/build
  mkdir $PROJECT_DIR/build
fi

if [[ "$@" == *"--depclean"* ]]; then
  echo "Cleaning dependencies..."
  rm -rf $PROJECT_DIR/external-$SYSTEM_NAME
  mkdir $PROJECT_DIR/external-$SYSTEM_NAME
fi

cmake -H$PROJECT_DIR -B$PROJECT_DIR/build
make -C $PROJECT_DIR/build