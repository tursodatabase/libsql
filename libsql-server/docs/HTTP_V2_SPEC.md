# The sqld HTTP API v2 specification ("Hrana over HTTP")

Version 2 of the HTTP API ("Hrana over HTTP") exposes stateful streams from
Hrana over HTTP. It provides functionality equivalent to Hrana and it is useful
for environments with missing or incomplete support for WebSockets.

This version deprecates version 1 of the HTTP API. Both clients and servers
should move to version 2 as soon as possible.

## Overview

The HTTP API uses data structures and semantics from the Hrana 2 protocol.

Individual requests on the same stream are tied together by the use of a baton.
The server returns a baton in every response to a request on the stream, and the
client then needs to include the baton in the subsequent request. The client
must serialize the requests: it must wait for a response to the previous request
before sending next request.

The server can also optionally specify a different URL that the client should
use for the requests on the stream. This can be used to ensure that stream
requests are "sticky" and reach the same server.

The server will close streams after a short period of inactivity, to make sure
that abandoned streams don't accumulate on the server.

## Check support for version 2

```typescript
GET /v2
```

If the server supports this version of the HTTP API, it should return a 2xx
response for a GET request on `/v2`. This can be used as a crude version
negotiation mechanism by the client.

## Execute requests on a stream

```typescript
POST /v2/pipeline

-> {
    "baton": string | null,
    "requests": Array<StreamRequest>,
}

<- {
    "baton": string | null,
    "base_url": string | null,
    "results": Array<StreamResult>
}

type StreamResult =
    | StreamResultOk
    | StreamResultError

type StreamResultOk = {
    "type": "ok",
    "response": StreamResponse,
}

type StreamResultError = {
    "type": "error",
    "error": Error,
}
```

The `pipeline` endpoint is used to execute a pipeline of requests on a stream.
`baton` in the request specifies the stream. If the client sets `baton` to
`null`, the server should create a new stream.

Server responds with another `baton` value in the response. If the `baton` value
in the response is `null`, it means that the server has closed the stream. The
client must use this value to refer to this stream in the next request (the
`baton` in the response should be different from the `baton` in the request).
This forces the client to issue the requests serially: it must wait for the
response from a previous `pipeline` request before issuing another request on
the same stream.

The server should ensure that the `baton` values are unpredictable and
unforgeable, for example by cryptographically signing them.

If the `base_url` in the response is not `null`, the client should use this URL
when sending further requests on this stream. If it is `null`, the client should
use the same URL that it has used for the previous request. The `base_url`
must be an absolute URL with "http" or "https" scheme.

The `requests` array in the request specifies a sequence of stream requests that
should be executed on the stream. The server executes them in order and returns
the results in the `results` array in the response. Result is either a success
(`type` set to `"ok"`) or an error (`type` set to `"error"`). The server always
executes all requests, even if some of them return errors.

If the client receives an HTTP error (4xx or 5xx response) in response to the
`pipeline` endpoint, it means that the server encountered an internal error and
the stream is no longer valid.

## Requests

Requests in the HTTP API closely mirror stream requests in Hrana:

```typescript
type StreamRequest =
    | CloseStreamReq
    | ExecuteStreamReq
    | BatchStreamReq
    | SequenceStreamReq
    | DescribeStreamReq
    | StoreSqlStreamReq
    | CloseSqlStreamReq

type StreamResponse =
    | CloseStreamResp
    | ExecuteStreamResp
    | BatchStreamResp
    | SequenceStreamResp
    | DescribeStreamResp
    | StoreSqlStreamResp
    | CloseSqlStreamResp
```

### Close stream

```typescript
type CloseStreamReq = {
    "type": "close",
}

type CloseStreamResp = {
    "type": "close",
}
```

The `close` request closes the stream. It is an error if the client tries to
execute more requests on the same stream.

### Execute a statement

```typescript
type ExecuteStreamReq = {
    "type": "execute",
    "stmt": Stmt,
}

type ExecuteStreamResp = {
    "type": "execute",
    "result": StmtResult,
}
```

The `execute` request has the same semantics as the `execute` request in Hrana. 

### Execute a batch

```typescript
type BatchStreamReq = {
    "type": "batch",
    "batch": Batch,
}

type BatchStreamResp = {
    "type": "batch",
    "result": BatchResult,
}
```

The `batch` request has the same semantics as the `batch` request in Hrana.

### Execute a sequence of SQL statements

```typescript
type SequenceStreamReq = {
    "type": "sequence",
    "sql"?: string | null,
    "sql_id"?: int32 | null,
}

type SequenceStreamResp = {
    "type": "sequence",
}
```

The `sequence` request has the same semantics as the `sequence` request in
Hrana.

### Describe a statement

```typescript
type DescribeStreamReq = {
    "type": "describe",
    "sql"?: string | null,
    "sql_id"?: int32 | null,
}

type DescribeStreamResp = {
    "type": "describe",
    "result": DescribeResult,
}
```

The `describe` request has the same semantics as the `describe` request in
Hrana.

### Store an SQL text on the server

```typescript
type StoreSqlStreamReq = {
    "type": "store_sql",
    "sql_id": int32,
    "sql": string,
}

type StoreSqlStreamResp = {
    "type": "store_sql",
}
```

The `store_sql` request has the same semantics as the `store_sql` request in
Hrana, except that the scope of the SQL texts is just a single stream (in Hrana,
it is the whole connection).

### Close a stored SQL text

```typescript
type CloseSqlStreamReq = {
    "type": "close_sql",
    "sql_id": int32,
}

type CloseSqlStreamResp = {
    "type": "close_sql",
}
```

The `close_sql` request has the same semantics as the `close_sql` request in
Hrana, except that the scope of the SQL texts is just a single stream.
