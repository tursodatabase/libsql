FROM rust:latest as builder

WORKDIR /usr/src/app
COPY . .
# Will build and cache the binary and dependent crates in release mode
RUN --mount=type=cache,target=/usr/local/cargo,from=rust:latest,source=/usr/local/cargo \
    --mount=type=cache,target=target \
    cargo build --bin libsql-storage-server && mv ./target/debug/libsql-storage-server ./libsql-storage-server && chmod +x ./libsql-storage-server


# Runtime image
FROM debian:bookworm-slim
EXPOSE 5002

COPY --from=builder /usr/src/app/libsql-storage-server /

CMD ["/libsql-storage-server"]