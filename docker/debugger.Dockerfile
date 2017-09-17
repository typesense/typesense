FROM ubuntu:16.04

RUN mkdir -p /opt
COPY typesense-server /opt
RUN chmod +x /opt/typesense-server
EXPOSE 8108
ENTRYPOINT ["/opt/typesense-server"]