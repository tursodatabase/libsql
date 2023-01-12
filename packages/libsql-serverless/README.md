# libSQL serverless driver for TypeScript and JavaScript

## Getting Started

To get started, you need `sqld` running somewhere. Then:

```typescript
import { connect } from "@libsql/serverless"

const config = {
  url: "http://localhost:8080"
};
const db = connect(config);
const rs = await db.execute("SELECT * FROM users");
console.log(rs);
```

You can also just run against local SQLite by dropping the `url` option from configuration:

```typescript
import { connect } from "@libsql/serverless"

const config = { };
const db = connect(config);
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
