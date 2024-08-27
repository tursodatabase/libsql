# build sqld
FROM rust:slim-bullseye AS chef
RUN apt update \
    && apt install -y libclang-dev clang \
        build-essential tcl protobuf-compiler file \
        libssl-dev pkg-config git cmake \
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
ARG ENABLE_FEATURES=""
RUN if [ "$ENABLE_FEATURES" == "" ]; then \
        cargo build -p libsql-server --release ; \
    else \
        cargo build -p libsql-server --features "$ENABLE_FEATURES" --release ; \
    fi
RUN cargo build -p bottomless-cli --release

# official gosu install instruction (https://github.com/tianon/gosu/blob/master/INSTALL.md)
FROM debian:bullseye-slim as gosu
ENV GOSU_VERSION 1.17
RUN set -eux; \
# save list of currently installed packages for later so we can clean up
	savedAptMark="$(apt-mark showmanual)"; \
	apt-get update; \
	apt-get install -y --no-install-recommends ca-certificates gnupg wget; \
	rm -rf /var/lib/apt/lists/*; \
	\
	dpkgArch="$(dpkg --print-architecture | awk -F- '{ print $NF }')"; \
	wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch"; \
	wget -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch.asc"; \
	\
# verify the signature
	export GNUPGHOME="$(mktemp -d)"; \
	gpg --batch --keyserver hkps://keys.openpgp.org --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4; \
	gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu; \
	gpgconf --kill all; \
	rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc; \
	\
# clean up fetch dependencies
	apt-mark auto '.*' > /dev/null; \
	[ -z "$savedAptMark" ] || apt-mark manual $savedAptMark; \
	apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false; \
	\
	chmod +x /usr/local/bin/gosu; \
# verify that the binary works
	gosu --version; \
	gosu nobody true

# runtime
FROM debian:bullseye-slim
RUN apt update

EXPOSE 5001 8080
VOLUME [ "/var/lib/sqld" ]

RUN groupadd --system --gid 666 sqld
RUN adduser --system --home /var/lib/sqld --uid 666 --gid 666 sqld
WORKDIR /var/lib/sqld
USER sqld

COPY docker-entrypoint.sh /usr/local/bin
COPY docker-wrapper.sh /usr/local/bin

COPY --from=gosu /usr/local/bin/gosu /usr/local/bin/gosu
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /target/release/sqld /bin/sqld
COPY --from=builder /target/release/bottomless-cli /bin/bottomless-cli

USER root

ENTRYPOINT ["/usr/local/bin/docker-wrapper.sh"]
CMD ["/bin/sqld"]
