FROM typesense/ubuntu-10.04-gcc:4.9.2

ENV PATH /usr/local/gcc-4.9.2/bin/:$PATH
ENV LD_LIBRARY_PATH /usr/local/gcc-4.9.2/lib64

RUN apt-get update

RUN apt-get install -y zlib1g-dev
RUN apt-get install -y liblist-compare-perl

RUN curl -o /opt/cmake-3.3.2-Linux-x86_64.tar.gz -L https://cmake.org/files/v3.3/cmake-3.3.2-Linux-x86_64.tar.gz
RUN tar -C /opt -xvzf /opt/cmake-3.3.2-Linux-x86_64.tar.gz
RUN cp -r /opt/cmake-3.3.2-Linux-x86_64/* /usr

RUN apt-get install libidn11
RUN curl -o /opt/snappy_1.1.3.orig.tar.gz -L https://launchpad.net/ubuntu/+archive/primary/+files/snappy_1.1.3.orig.tar.gz
RUN tar -C /opt -xf /opt/snappy_1.1.3.orig.tar.gz
RUN mkdir /opt/snappy-1.1.3/build && cd /opt/snappy-1.1.3/build && ../configure && make && make install

RUN curl -o /opt/openssl-1.0.2k.tar.gz -L https://openssl.org/source/openssl-1.0.2k.tar.gz
RUN tar -C /opt -xvzf /opt/openssl-1.0.2k.tar.gz
RUN cd /opt/openssl-1.0.2k && sh ./config --prefix=/usr --openssldir=/usr zlib-dynamic
RUN make -C /opt/openssl-1.0.2k depend
RUN make -C /opt/openssl-1.0.2k -j4
RUN make -C /opt/openssl-1.0.2k install

RUN curl -L -o /opt/curl-7.55.1.tar.bz2 https://github.com/curl/curl/releases/download/curl-7_55_1/curl-7.55.1.tar.bz2
RUN tar -C /opt -xf /opt/curl-7.55.1.tar.bz2
RUN cd /opt/curl-7.55.1 && ./configure && make && make install

ENV CC /usr/local/gcc-4.9.2/bin/gcc
ENV CXX /usr/local/gcc-4.9.2/bin/g++
