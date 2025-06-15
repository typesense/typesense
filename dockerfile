FROM typesense/typesense:latest
COPY ./config /etc/typesense
EXPOSE 8108
CMD ["--data-dir", "/data", "--api-key", "$TYPESENSE_API_KEY", "--enable-cors"]
