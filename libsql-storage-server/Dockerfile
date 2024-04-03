# run this from the root
# docker build -f libsql-storage-server/Dockerfile -t lss .
# docker run -p lss

FROM rust:latest as builder

WORKDIR /app
COPY . .

RUN --mount=type=cache,target=/usr/local/cargo,from=rust:latest,source=/usr/local/cargo \
    --mount=type=cache,target=target \
    cargo build --bin libsql-storage-server && mv ./target/debug/libsql-storage-server ./libsql-storage-server && chmod +x ./libsql-storage-server


FROM debian:bookworm-slim
EXPOSE 5002

COPY --from=builder /app/libsql-storage-server /

CMD ["/libsql-storage-server"]