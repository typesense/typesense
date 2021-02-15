FROM ubuntu:18.04

RUN apt-get update && apt-get -y install build-essential libc6-dbg

ADD https://sourceware.org/pub/valgrind/valgrind-3.16.1.tar.bz2 /opt/valgrind-3.16.1.tar.bz2
RUN tar -C /opt -xf /opt/valgrind-3.16.1.tar.bz2
RUN cd /opt/valgrind-3.16.1 && ./configure --prefix=/usr && make -j8 && make install
