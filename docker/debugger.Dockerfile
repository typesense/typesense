FROM ubuntu:20.04

RUN apt-get update && apt-get -y install build-essential libc6-dbg

ADD https://sourceware.org/pub/valgrind/valgrind-3.17.0.tar.bz2 /opt/valgrind-3.17.0.tar.bz2
RUN tar -C /opt -xf /opt/valgrind-3.17.0.tar.bz2
RUN cd /opt/valgrind-3.17.0 && ./configure --prefix=/usr && make -j8 && make install

RUN DEBIAN_FRONTEND="noninteractive" apt-get -y install gdb
