# $ docker build --file $PROJECT_DIR/docker/ubuntu-10-04-gcc.Dockerfile --tag typesense/ubuntu-10.04-gcc:latest --tag typesense/ubuntu-10.04-gcc:6.4 $PROJECT_DIR/docker
# 
# NOTE: Default build image is bloated. Before publishing, export from a container to squash the image:
# $ docker run -it typesense/ubuntu-10.04-gcc:latest bash -c "exit"
# $ docker ps -a | grep typesense
# $ docker export <container_id> | docker import - typesense/ubuntu-10.04-gcc:latest
# $ docker push typesense/ubuntu-10.04-gcc

FROM ubuntu:10.04

RUN sed -i -re 's/([a-z]{2}\.)?archive.ubuntu.com|security.ubuntu.com/old-releases.ubuntu.com/g' /etc/apt/sources.list
RUN apt-get update && apt-get install -y \
	build-essential \
	tar \
	libmpfr-dev \
	libgmp3-dev \
	libmpc-dev \
	flex \
	bison \
	zlib1g-dev

RUN mkdir -p /etc/ssl/certs
ADD https://curl.haxx.se/ca/cacert.pem /etc/ssl/certs/cacert.pem

ADD https://openssl.org/source/openssl-1.0.2k.tar.gz /opt/openssl-1.0.2k.tar.gz
RUN tar -C /opt -xvzf /opt/openssl-1.0.2k.tar.gz
RUN cd /opt/openssl-1.0.2k && sh ./config --prefix=/usr/local --openssldir=/usr/local zlib-dynamic
RUN make -C /opt/openssl-1.0.2k depend
RUN make -C /opt/openssl-1.0.2k -j4
RUN make -C /opt/openssl-1.0.2k install

ADD https://github.com/curl/curl/releases/download/curl-7_55_1/curl-7.55.1.tar.bz2 /opt/curl-7.55.1.tar.bz2
RUN tar -C /opt -xf /opt/curl-7.55.1.tar.bz2
RUN cd /opt/curl-7.55.1 && LIBS="-ldl -lpthread" ./configure --disable-shared --with-ssl=/usr/local \
--with-ca-bundle=/etc/ssl/certs/cacert.pem && make && make install

RUN curl -L -o /opt/gcc-6.4.0.tar.gz https://ftp.gnu.org/gnu/gcc/gcc-6.4.0/gcc-6.4.0.tar.gz
RUN tar -C /opt -xf /opt/gcc-6.4.0.tar.gz

RUN mkdir /opt/gcc-6.4.0/build && cd /opt/gcc-6.4.0/build && ../configure --disable-checking --enable-languages=c,c++ \
      --enable-multiarch --disable-multilib --enable-shared --enable-threads=posix \
      --with-gmp=/usr/local/lib --with-mpc=/usr/lib --with-mpfr=/usr/lib \
      --build=x86_64-linux-gnu --host=x86_64-linux-gnu --target=x86_64-linux-gnu \
      --without-included-gettext --with-tune=generic --prefix=/usr/local/gcc-6.4.0
RUN cd /opt/gcc-6.4.0/build && make -j8
RUN cd /opt/gcc-6.4.0/build && make install

RUN apt-get remove -y gcc
RUN rm -rf /opt/gcc-6.4.0 /opt/gcc-6.4.0.tar.gz /opt/openssl-1.0.2k /opt/openssl-1.0.2k.tar.gz \
/opt/curl-7.55.1 /opt/curl-7.55.1.tar.bz2

ENV PATH /usr/local/gcc-6.4.0/bin/:$PATH
ENV LD_LIBRARY_PATH /usr/local/gcc-6.4.0/lib64
