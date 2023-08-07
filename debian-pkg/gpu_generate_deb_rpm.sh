#!/bin/bash

# TSV is passed as an environment variable to the script

if [ -z "$TSV" ]
then
  echo '$TSV is not provided. Quitting.'
  exit 1
fi

if [ -z "$ARCH" ]
then
  echo '$ARCH is not provided. Quitting.'
  exit 1
fi

RPM_ARCH=$ARCH
if [ "$ARCH" == "amd64" ]; then
  RPM_ARCH="x86_64"
fi

set -ex
CURR_DIR=`dirname $0 | while read a; do cd $a && pwd && break; done`

rm -rf /tmp/typesense-gpu-deb-build && mkdir /tmp/typesense-gpu-deb-build
cp -r $CURR_DIR/typesense-gpu-deps /tmp/typesense-gpu-deb-build

rm -rf /tmp/typesense-gpu-deps-$TSV && mkdir /tmp/typesense-gpu-deps-$TSV
tar -xzf $CURR_DIR/../bazel-bin/typesense-gpu-deps-$TSV-linux-${ARCH}.tar.gz -C /tmp/typesense-gpu-deps-$TSV
mkdir -p /tmp/typesense-gpu-deb-build/typesense-gpu-deps/usr/lib/
cp /tmp/typesense-gpu-deps-$TSV/*.so /tmp/typesense-gpu-deb-build/typesense-gpu-deps/usr/lib/

rm -rf /tmp/typesense-gpu-deps-$TSV /tmp/typesense-gpu-deps-$TSV.tar.gz

sed -i "s/\$VERSION/$TSV/g" `find /tmp/typesense-gpu-deb-build -maxdepth 10 -type f`
sed -i "s/\$ARCH/$ARCH/g" `find /tmp/typesense-gpu-deb-build -maxdepth 10 -type f`

dpkg-deb -Zgzip -z6 \
         -b /tmp/typesense-gpu-deb-build/typesense-gpu-deps "/tmp/typesense-gpu-deb-build/typesense-gpu-deps-${TSV}-${ARCH}.deb"

# Generate RPM

rm -rf /tmp/typesense-gpu-rpm-build && mkdir /tmp/typesense-gpu-rpm-build
cp "/tmp/typesense-gpu-deb-build/typesense-gpu-deps-${TSV}-${ARCH}.deb" /tmp/typesense-gpu-rpm-build
cd /tmp/typesense-gpu-rpm-build && alien --scripts -k -r -g -v /tmp/typesense-gpu-rpm-build/typesense-gpu-deps-${TSV}-${ARCH}.deb

sed -i 's#%dir "/"##' `find /tmp/typesense-gpu-rpm-build/*/*.spec -maxdepth 10 -type f`
sed -i 's#%dir "/usr/bin/"##' `find /tmp/typesense-gpu-rpm-build/*/*.spec -maxdepth 10 -type f`
sed -i 's/%config/%config(noreplace)/g' `find /tmp/typesense-gpu-rpm-build/*/*.spec -maxdepth 10 -type f`

SPEC_FILE="/tmp/typesense-gpu-rpm-build/typesense-gpu-deps-${TSV}/typesense-gpu-deps-${TSV}-1.spec"
cd /tmp/typesense-gpu-rpm-build/typesense-gpu-deps-${TSV} && \
  rpmbuild --target=${RPM_ARCH} --buildroot /tmp/typesense-gpu-rpm-build/typesense-gpu-deps-${TSV} -bb \
  $SPEC_FILE

cp "/tmp/typesense-gpu-rpm-build/typesense-gpu-deps-${TSV}-${ARCH}.deb" $CURR_DIR/../bazel-bin
cp "/tmp/typesense-gpu-rpm-build/typesense-gpu-deps-${TSV}-1.${RPM_ARCH}.rpm" $CURR_DIR/../bazel-bin
