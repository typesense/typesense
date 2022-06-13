FROM typesense/typesense-development:29-DEC-2021-1

RUN apt-get install -y texinfo libc6-dbg

ADD http://ftp.gnu.org/gnu/gdb/gdb-7.11.tar.gz /opt/gdb-7.11.tar.gz
RUN tar -C /opt -xf /opt/gdb-7.11.tar.gz
RUN cd /opt/gdb-7.11 && ./configure && make -j8 && make install

ADD https://sourceware.org/pub/valgrind/valgrind-3.17.0.tar.bz2 /opt/valgrind-3.17.0.tar.bz2
RUN tar -C /opt -xf /opt/valgrind-3.17.0.tar.bz2
RUN cd /opt/valgrind-3.17.0 && ./configure --prefix=/usr && make -j8 && make install
