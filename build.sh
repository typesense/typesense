#!/bin/sh
PROJECT_DIR=`dirname $0`
rm -rf $PROJECT_DIR/build
mkdir $PROJECT_DIR/build
cmake -H$PROJECT_DIR -B$PROJECT_DIR/build
make -C $PROJECT_DIR