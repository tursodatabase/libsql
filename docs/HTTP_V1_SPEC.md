# The sqld HTTP API v1 specification ("Hrana over HTTP")

Version 1 of the HTTP API ("Hrana over HTTP") is designed to complement the
WebSocket-based Hrana protocol for use cases that don't require stateful
database connections and for which the additional network rountrip required by
WebSockets relative to HTTP is not necessary.

This API aims to be of production quality and it is primarily intended to be
consumed by client libraries. It does not deprecate or replace the "version 0"
of the HTTP API, which is designed to be quick and easy for users who send HTTP
requests manually (for example using `curl` or by directly using an HTTP
library).

## Overview

This HTTP API uses data structures and semantics from the Hrana protocol;
versions of the HTTP API are intended to correspond to versions of the Hrana
protocol, so HTTP API v1 corresponds to the `hrana1` version of Hrana.

Endpoints in the HTTP API correspond to requests in Hrana. Each request is
executed as if a fresh Hrana stream was opened for the request.

All request and response bodies are encoded in JSON, with content type
`application/json`.

## Execute a statement

```
POST /v1/execute

-> {
    "stmt": Stmt,
}

<- {
    "result": StmtResult,
}
```

The `execute` endpoint receives a statement and returns the result of executing
the statement. The `Stmt` and `StmtResult` structures are from the Hrana
protocol. The semantics of this endpoint is the same as the `execute` request in
Hrana.

## Execute a batch

```
POST /v1/batch

-> {
    "batch": Batch,
}

<- {
    "result": BatchResult,
}
```

The `batch` endpoint receives a batch and returns the result of executing the
statement. The `Batch` and `BatchResult` structures are from the Hrana protocol.
The semantics of this endpoint is the same as the `batch` request in Hrana.

## Errors

Successful responses are indicated by a HTTP status code in range [200, 300).
Errors are indicated with HTTP status codes in range [400, 600), and the error
responses should have the format of `Error` from the Hrana protocol. However,
the clients should be able to handle error responses that don't correspond to
this format; in particular, the server may produce some error responses with the
error message as plain text.
