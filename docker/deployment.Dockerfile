FROM ubuntu:20.04

RUN apt-get -y update && apt-get -y install ca-certificates curl

RUN mkdir -p /opt
COPY typesense-server /opt
RUN chmod +x /opt/typesense-server

EXPOSE 8108
HEALTHCHECK curl -f http://localhost:8108/health

ENTRYPOINT ["/opt/typesense-server"]
