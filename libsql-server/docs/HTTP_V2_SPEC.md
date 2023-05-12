# The sqld HTTP API v2 specification ("Hrana over HTTP")

Version 2 of the HTTP API ("Hrana over HTTP") extends the HTTP API with features
introduced in Hrana 2.

## Overview

This HTTP API uses data structures and semantics from the Hrana 2 protocol.
Endpoints in the HTTP API correspond to requests in Hrana. Each request is
executed as if a fresh Hrana stream was opened for the request.

All request and response bodies are encoded in JSON, with content type
`application/json`.

## Version check

```
GET /v2
```

A server that supports HTTP API v2 must return a success response for requests
on path `/v2`. This can be used as a crude and unreliable mechanism for clients
to fall back to version 1 of the protocol if the server does not support version
2.

## Execute a statement

```
POST /v2/execute

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

Note that specifying the SQL text using `sql_id` in `Stmt` is never valid,
because there is no way to store SQL texts on the server using the HTTP API.

## Execute a batch

```
POST /v2/batch

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

## Execute a sequence

```
POST /v2/sequence

-> {
    "sql": string,
}

<- {
}
```

The `sequence` endpoint receives a SQL text with statements separated by
semicolons and executes it in the same way as the `sequence` request in Hrana.

## Describe a statement

```
POST /v2/describe

-> {
    "sql": string,
}

<- {
    "result": DescribeResult,
}
```

The `describe` endpoint receives a SQL statement and returns information about
this statement, with the same semantics as the `describe` request in Hrana.

## Errors

Successful responses are indicated by a HTTP status code in range [200, 300).
Errors are indicated with HTTP status codes in range [400, 600), and the error
responses should have the format of `Error` from the Hrana protocol. However,
the clients should be able to handle error responses that don't correspond to
this format; in particular, the server may produce some error responses with the
error message as plain text.
