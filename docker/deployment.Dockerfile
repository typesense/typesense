FROM ubuntu:22.04

RUN apt-get -y update && apt-get -y install ca-certificates

RUN mkdir -p /opt
COPY typesense-server /opt
RUN chmod +x /opt/typesense-server
EXPOSE 8108
STOPSIGNAL SIGINT
ENTRYPOINT ["/opt/typesense-server"]
