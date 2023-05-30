# build sqld
FROM rust:slim-bullseye AS builder
RUN apt update \
    && apt install -y libclang-dev clang \
        build-essential tcl protobuf-compiler file \
        libssl-dev pkg-config git\
    && apt clean
COPY . .
RUN cargo build -p sqld --release

# runtime
FROM debian:bullseye-slim
COPY --from=builder /target/release/sqld /bin/sqld
RUN adduser --system --home /var/lib/sqld --uid 666 sqld 
RUN apt-get update && apt-get install -y ca-certificates
COPY docker-entrypoint.sh /usr/local/bin
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
VOLUME [ "/var/lib/sqld" ]
WORKDIR /var/lib/sqld
USER sqld
EXPOSE 5001 8080
CMD ["/bin/sqld"]
