FROM typesense/typesense:latest

# Create data directory and set permissions
RUN mkdir -p /data && \
    addgroup -g 10001 choreo && \
    adduser -D -s /bin/bash -u 10001 -G choreo choreouser && \
    chown -R choreouser:choreo /data


# Switch to non-root user (required by Choreo)
USER 10001

# Expose the Typesense port
EXPOSE 8108

# Start Typesense server with required parameters
CMD ["typesense-server", "--data-dir", "/data", "--api-key", "$TYPESENSE_API_KEY", "--listen-address", "0.0.0.0", "--listen-port", "8108", "--enable-cors"]
