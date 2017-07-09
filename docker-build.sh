#!/bin/sh

set -ex
PROJECT_DIR=`dirname $0 | while read a; do cd $a && pwd && break; done`

if [[ "$@" == *"--clean"* ]]; then
  echo "Cleaning..."
  rm -rf $PROJECT_DIR/build
  mkdir $PROJECT_DIR/build
fi

if [[ "$@" == *"--depclean"* ]]; then
  echo "Cleaning dependencies..."
  rm -rf $PROJECT_DIR/external
fi

echo "Creating development image..."
docker build --file $PROJECT_DIR/docker/development.Dockerfile --tag wreally/typesense-development:latest $PROJECT_DIR/docker

echo "Building Typesense..."
docker run -v $PROJECT_DIR:/typesense wreally/typesense-development cmake -H/typesense -B/typesense/build
docker run -v $PROJECT_DIR:/typesense wreally/typesense-development make -C/typesense/build

if [[ "$@" == *"--build-deploy-image"* ]]; then
    echo "Creating deployment image for Typesense server..."

    if [ -z "$TYPESENSE_IMG_VERSION" ]; then
      echo "Need to set TYPESENSE_IMG_VERSION environment variable."
      exit 1
    fi

    cp $PROJECT_DIR/docker/deployment.Dockerfile $PROJECT_DIR/build
    docker build --file $PROJECT_DIR/build/deployment.Dockerfile --tag wreally/typesense-server:$TYPESENSE_IMG_VERSION \
                        $PROJECT_DIR/build
fi

echo "Done... quitting."