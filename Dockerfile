# syntax=docker/dockerfile:1.4
# DSF Container image

# TARGET arg used for building per-platform
ARG TARGET

# Using debian base image
FROM debian:bullseye-slim

# Setup default database and socket locations
ENV DSF_DB_FILE=/var/dsfd/dsf.db
ENV DSF_SOCK=/var/run/dsfd/dsf.sock

# Refresh TAREGET argument in FROM context
ARG TARGET

# Copy binaries into container
COPY "./target/$TARGET/release/dsfd" /usr/local/bin
COPY "./target/$TARGET/release/dsfc" /usr/local/bin

# Start Launch DSFD on start
CMD ["/usr/local/bin/dsfd"]
