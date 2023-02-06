FROM rust:slim-bookworm
RUN cargo install cargo-insta
RUN apt update && apt install -y libssl-dev rsync build-essential podman pkg-config aardvark-dns
COPY sqld.tar .
COPY docker-entrypoint.sh .
CMD /docker-entrypoint.sh
