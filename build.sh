#!/bin/sh
set -o xtrace

PROJECT_DIR=`dirname $0`

if [ "$1" = "--clean" ]; then
  rm -rf $PROJECT_DIR/build
  mkdir $PROJECT_DIR/build

  # rm -rf $PROJECT_DIR/external
  # DOCKER_NO_CACHE=" --no-cache"
fi

docker build --file $PROJECT_DIR/docker/development.Dockerfile --tag wreally/typesense-development $DOCKER_NO_CACHE $PROJECT_DIR/docker

./dockcross --image wreally/typesense-development cmake -H$PROJECT_DIR -B$PROJECT_DIR/build
./dockcross --image wreally/typesense-development make -C $PROJECT_DIR/build