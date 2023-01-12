# libSQL serverless driver for TypeScript and JavaScript

## Getting Started

```typescript
import { Database } from "@libsql/serverless"

const db = new Database();
await db.execute("CREATE TABLE users (email TEXT)");
await db.execute("INSERT INTO users (email) VALUES ('alice@example.com')");
await db.execute("INSERT INTO users (email) VALUES ('bob@example.com')");
```

## Features

* SQLite JavaScript API
* SQLite-backed local-only backend
* SQL over HTTP with `fetch()`

## Roadmap

* Read replica mode
* Cloudflare D1 API compatibility?
