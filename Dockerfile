# install dependencies
FROM rust:slim-bullseye AS compiler
RUN apt update && apt install -y libclang-dev clang wget unzip libsqlite3-dev build-essential tcl protobuf-compiler file
RUN cargo install cargo-chef
WORKDIR /iku-turso

# prepare recipe
FROM compiler AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# build iku-turso
FROM compiler AS builder
COPY --from=planner iku-turso/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release

# runtime
FROM debian:bullseye-slim
COPY --from=builder /iku-turso/target/release/server /bin/server
ENTRYPOINT ["/bin/server", "serve"]
