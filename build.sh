#!/bin/sh
PROJECT_DIR=`dirname $0`

if [ "$1" == "--clean" ]; then
  rm -rf $PROJECT_DIR/build
  mkdir $PROJECT_DIR/build
fi

cmake -H$PROJECT_DIR -B$PROJECT_DIR/build
make -C $PROJECT_DIR/build