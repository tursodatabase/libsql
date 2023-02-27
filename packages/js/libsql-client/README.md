# libSQL driver for TypeScript and JavaScript

## Getting Started

To get started, you need `sqld` running somewhere. Then:

```typescript
import { createClient } from "@libsql/client"

const config = {
  url: "http://localhost:8080"
};
const db = createClient(config);
const rs = await db.execute("SELECT * FROM users");
console.log(rs);
```

You can also just run against local SQLite with:

```typescript
import { createClient } from "@libsql/client"

const config = {
  url: "file:example.db" // Use "file::memory:" for in-memory mode.
};
const db = createClient(config);
const rs = await db.execute("SELECT * FROM users");
console.log(rs);
```

## Features

* SQLite JavaScript API
* SQLite-backed local-only backend
* SQL over HTTP with `fetch()`

## Roadmap

* Read replica mode
* Cloudflare D1 API compatibility?
