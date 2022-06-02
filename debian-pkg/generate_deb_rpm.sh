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

rm -rf /tmp/typesense-deb-build && mkdir /tmp/typesense-deb-build
cp -r $CURR_DIR/typesense-server /tmp/typesense-deb-build

# Download Typesense, extract and make it executable

#curl -o /tmp/typesense-server-$TSV.tar.gz https://dl.typesense.org/releases/$TSV/typesense-server-$TSV-linux-${ARCH}.tar.gz
rm -rf /tmp/typesense-server-$TSV && mkdir /tmp/typesense-server-$TSV
tar -xzf /typesense-core/build-Linux/typesense-server-$TSV-linux-${ARCH}.tar.gz -C /tmp/typesense-server-$TSV

downloaded_hash=`md5sum /tmp/typesense-server-$TSV/typesense-server | cut -d' ' -f1`
original_hash=`cat /tmp/typesense-server-$TSV/typesense-server.md5.txt`

if [ "$downloaded_hash" == "$original_hash" ]; then
    mkdir -p /tmp/typesense-deb-build/typesense-server/usr/bin
    cp /tmp/typesense-server-$TSV/typesense-server /tmp/typesense-deb-build/typesense-server/usr/bin
else
    >&2 echo "Typesense server binary is corrupted. Quitting."
    exit 1
fi

rm -rf /tmp/typesense-server-$TSV /tmp/typesense-server-$TSV.tar.gz

sed -i "s/\$VERSION/$TSV/g" `find /tmp/typesense-deb-build -maxdepth 10 -type f`
sed -i "s/\$ARCH/$ARCH/g" `find /tmp/typesense-deb-build -maxdepth 10 -type f`

dpkg-deb -Zgzip -z6 \
         -b /tmp/typesense-deb-build/typesense-server "/tmp/typesense-deb-build/typesense-server-${TSV}-${ARCH}.deb"

# Generate RPM

rm -rf /tmp/typesense-rpm-build && mkdir /tmp/typesense-rpm-build
cp "/tmp/typesense-deb-build/typesense-server-${TSV}-${ARCH}.deb" /tmp/typesense-rpm-build
cd /tmp/typesense-rpm-build && alien --scripts -k -r -g -v /tmp/typesense-rpm-build/typesense-server-${TSV}-${ARCH}.deb

sed -i 's#%dir "/"##' `find /tmp/typesense-rpm-build/*/*.spec -maxdepth 10 -type f`
sed -i 's#%dir "/usr/bin/"##' `find /tmp/typesense-rpm-build/*/*.spec -maxdepth 10 -type f`
sed -i 's/%config/%config(noreplace)/g' `find /tmp/typesense-rpm-build/*/*.spec -maxdepth 10 -type f`

SPEC_FILE="/tmp/typesense-rpm-build/typesense-server-${TSV}/typesense-server-${TSV}-1.spec"
SPEC_FILE_COPY="/tmp/typesense-rpm-build/typesense-server-${TSV}/typesense-server-${TSV}-copy.spec"

cp $SPEC_FILE $SPEC_FILE_COPY

PRE_LINE=`grep -n "%pre" $SPEC_FILE_COPY | cut -f1 -d:`
START_LINE=`expr $PRE_LINE - 1`

head -$START_LINE $SPEC_FILE_COPY > $SPEC_FILE

echo "%prep" >> $SPEC_FILE
echo "cat >/tmp/find_requires.sh <<EOF
#!/bin/sh
%{__find_requires} | grep -v GLIBC_PRIVATE
exit 0
EOF" >> $SPEC_FILE

echo "chmod +x /tmp/find_requires.sh" >> $SPEC_FILE
echo "%define _use_internal_dependency_generator 0" >> $SPEC_FILE
echo "%define __find_requires /tmp/find_requires.sh" >> $SPEC_FILE

tail -n+$START_LINE $SPEC_FILE_COPY >> $SPEC_FILE

rm $SPEC_FILE_COPY

cd /tmp/typesense-rpm-build/typesense-server-${TSV} && \
  rpmbuild --target=${RPM_ARCH} --buildroot /tmp/typesense-rpm-build/typesense-server-${TSV} -bb \
  $SPEC_FILE

cp "/tmp/typesense-rpm-build/typesense-server-${TSV}-${ARCH}.deb" /typesense-core
cp "/tmp/typesense-rpm-build/typesense-server-${TSV}-1.${RPM_ARCH}.rpm" /typesense-core
