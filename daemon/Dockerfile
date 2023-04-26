# Dockerfile to generate DSF daemon container images
# Used with `buildx` in CI

# Specify ARCH (docker) and TARGET (rust)
ARG ARCH=
FROM ${ARCH}rust:latest

# Build binary
# (note there isn't exactly a useful way to cache at this point :-( )
WORKDIR /work
COPY ./ /work
RUN cargo build --release

FROM ${ARCH}debian:latest

# Copy binary into container
COPY --from=0 /work/target/release/dsfd /usr/local/bin/dsfd

# Start Launch DSFD on start
CMD ["/usr/local/bin/dsfd"]

