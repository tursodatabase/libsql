# build storage server
FROM rust:slim-bullseye AS chef
RUN apt update \
    && apt install -y libclang-dev clang \
        build-essential tcl protobuf-compiler file \
        libssl-dev pkg-config git tcl cmake curl \
    && apt clean \
    && rm -rf /var/lib/apt/lists/*

# We need to install and set as default the toolchain specified in rust-toolchain.toml
# Otherwise cargo-chef will build dependencies using wrong toolchain
# This also prevents planner and builder steps from installing the toolchain over and over again
COPY rust-toolchain.toml rust-toolchain.toml
RUN cat rust-toolchain.toml | grep "channel" | awk '{print $3}' | sed 's/\"//g' > toolchain.txt \
    && rustup update $(cat toolchain.txt) \
    && rustup default $(cat toolchain.txt) \
    && rm toolchain.txt rust-toolchain.toml \
    && cargo install cargo-chef
FROM chef AS planner
ARG BUILD_DEBUG=false
ENV CARGO_PROFILE_RELEASE_DEBUG=$BUILD_DEBUG
RUN echo $CARGO_PROFILE_RELEASE_DEBUG
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG BUILD_DEBUG=false
ENV CARGO_PROFILE_RELEASE_DEBUG=$BUILD_DEBUG
COPY --from=planner /recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .

RUN curl -L -O https://github.com/apple/foundationdb/releases/download/7.3.27/foundationdb-clients_7.3.27-1_amd64.deb
RUN dpkg -i foundationdb-clients_7.3.27-1_amd64.deb
RUN cargo build -p libsql-storage-server --features foundation-db --release

# runtime
FROM debian:bullseye-slim
RUN apt update

EXPOSE 5002

COPY --from=builder /foundationdb-clients_7.3.27-1_amd64.deb /foundationdb-clients_7.3.27-1_amd64.deb
RUN dpkg -i /foundationdb-clients_7.3.27-1_amd64.deb

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /target/release/libsql-storage-server /bin/libsql-storage-server

CMD ["/bin/libsql-storage-server", "--storage-type", "foundation-db"]
