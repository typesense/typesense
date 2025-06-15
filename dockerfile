FROM typesense/typesense:latest
EXPOSE 8108
CMD ["--data-dir", "/data", "--api-key=$TYPESENSE_API_KEY", "--enable-cors"]
