# docker build --file $PROJECT_DIR/docker/development.Dockerfile --tag typesense/typesense-development:latest $PROJECT_DIR/docker
# 
# $ docker push typesense/typesense-development:latest

FROM typesense/ubuntu-10-04-gcc:6.4.0

ENV PATH /usr/local/gcc-6.4.0/bin/:$PATH
ENV LD_LIBRARY_PATH /usr/local/gcc-6.4.0/lib64

RUN apt-get install -y python-software-properties \
	&& add-apt-repository ppa:git-core/ppa \
	&& apt-get update \
	&& apt-get install -y \
	zlib1g-dev \
	liblist-compare-perl \
	libidn11 \
	git

ADD https://github.com/Kitware/CMake/releases/download/v3.15.2/cmake-3.15.2-Linux-x86_64.tar.gz /opt/cmake-3.15.2-Linux-x86_64.tar.gz
RUN tar -C /opt -xvzf /opt/cmake-3.15.2-Linux-x86_64.tar.gz
RUN cp -r /opt/cmake-3.15.2-Linux-x86_64/* /usr

ADD https://launchpad.net/ubuntu/+archive/primary/+files/snappy_1.1.3.orig.tar.gz /opt/snappy_1.1.3.orig.tar.gz
RUN tar -C /opt -xf /opt/snappy_1.1.3.orig.tar.gz
RUN mkdir -p /opt/snappy-1.1.3/build && cd /opt/snappy-1.1.3/build && ../configure && make -j8 && make install

ADD https://github.com/unicode-org/icu/releases/download/release-61-1/icu4c-61_1-src.tgz /opt/icu4c-61_1-src.tgz
RUN tar -C /opt -xf /opt/icu4c-61_1-src.tgz
RUN cd /opt/icu/source && echo "#define U_DISABLE_RENAMING 1" >> common/unicode/uconfig.h && \
    echo "#define U_STATIC_IMPLEMENTATION 1" >> common/unicode/uconfig.h && \
    echo "#define U_USING_ICU_NAMESPACE 0" >> common/unicode/uconfig.h
RUN cd /opt/icu/source && ./runConfigureICU Linux --disable-samples --disable-tests --enable-static \
    --disable-shared --disable-renaming && make -j8 && make install

ADD https://openssl.org/source/openssl-1.1.1d.tar.gz /opt/openssl-1.1.1d.tar.gz
RUN tar -C /opt -xvzf /opt/openssl-1.1.1d.tar.gz
RUN cd /opt/openssl-1.1.1d && sh ./config --prefix=/usr/local --openssldir=/usr/local zlib
RUN make -C /opt/openssl-1.1.1d depend
RUN make -C /opt/openssl-1.1.1d -j8
RUN make -C /opt/openssl-1.1.1d install

ADD https://github.com/curl/curl/releases/download/curl-7_65_3/curl-7.65.3.tar.gz /opt/curl-7.65.3.tar.gz
RUN tar -C /opt -xf /opt/curl-7.65.3.tar.gz
RUN cd /opt/curl-7.65.3 && LIBS="-ldl -lpthread" ./configure --disable-shared --with-ssl=/usr/local \
--without-ca-bundle --without-ca-path && make -j8 && make install && rm -rf /usr/local/lib/*.so*

ADD https://github.com/gflags/gflags/archive/v2.2.2.tar.gz /opt/gflags-2.2.2.tar.gz
RUN tar -C /opt -xf /opt/gflags-2.2.2.tar.gz
RUN cd /opt/gflags-2.2.2 && cmake . -DBUILD_SHARED_LIBS=OFF && make -j8 && make install && rm -rf /usr/local/lib/*.so*

RUN apt-get install -y autoconf automake libtool unzip
RUN rm -rf /usr/local/lib/*.so*

ADD https://github.com/protocolbuffers/protobuf/releases/download/v3.11.4/protobuf-cpp-3.11.4.tar.gz /opt/protobuf-cpp-3.11.4.tar.gz
RUN tar -C /opt -xf /opt/protobuf-cpp-3.11.4.tar.gz
RUN cd /opt/protobuf-3.11.4 && ./configure --disable-shared && make -j8 && make check && make install && rm -rf /usr/local/lib/*.so*

ADD https://github.com/google/leveldb/archive/1.22.tar.gz /opt/leveldb-1.22.tar.gz.tar.gz
RUN tar -C /opt -xf /opt/leveldb-1.22.tar.gz.tar.gz
RUN mkdir -p /opt/leveldb-1.22/build && cd /opt/leveldb-1.22/build && cmake -DCMAKE_BUILD_TYPE=Release .. && \
    cmake --build . && make install && rm -rf /usr/local/lib/*.so*

ADD https://github.com/apache/incubator-brpc/archive/0.9.7-rc03.tar.gz /opt/brpc-0.9.7-rc03.tar.gz
RUN tar -C /opt -xf /opt/brpc-0.9.7-rc03.tar.gz
COPY patches/brpc_cmakelists.txt /opt/incubator-brpc-0.9.7-rc03/src/CMakeLists.txt
RUN mkdir -p /opt/incubator-brpc-0.9.7-rc03/build && cd /opt/incubator-brpc-0.9.7-rc03/build && \
    cmake -DWITH_DEBUG_SYMBOLS=OFF .. && make -j8 && make install && rm -rf /usr/local/lib/*.so*

ENV CC /usr/local/gcc-6.4.0/bin/gcc
ENV CXX /usr/local/gcc-6.4.0/bin/g++
