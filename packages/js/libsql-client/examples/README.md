# libSQL TypeScript/JavaScript client example

## Getting Started

Install dependencies and build the example:

```console
npm i && npm run build
```

First, run the example with local SQLite:

```console
DB_URL="file:example.db" npm run start
```

Then, start up a `sqld` server:

```console
cargo run -- --http-listen-addr 127.0.0.1:8080
```

and now run the example against the server:

```console
DB_URL="http://127.0.0.1:8080" npm run start
```
