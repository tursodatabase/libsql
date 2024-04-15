# The Hrana protocol specification (version 3)

Hrana (from Czech "hrana", which means "edge") is a protocol for connecting to a
SQLite database over the network. It is designed to be used from edge functions
and other environments where low latency and small overhead is important.

This is a specification for version 3 of the Hrana protocol (Hrana 3).

## Overview

The Hrana protocol provides SQL _streams_. Each stream corresponds to a SQLite
connection and executes a sequence of SQL statements.

### Variants (WebSocket / HTTP)

The protocol has two variants:

- Hrana over WebSocket, which uses WebSocket as the underlying protocol.
  Multiple streams can be multiplexed over a single WebSocket.
- Hrana over HTTP, which communicates with the server using HTTP requests. This
  is less efficient than WebSocket, but HTTP is the only reliable protocol in
  some environments.

Each of these variants is described later.

### Encoding

The protocol has two encodings:

- [JSON][rfc8259] is the canonical encoding, backward compatible with Hrana 1
  and 2.
- Protobuf ([Protocol Buffers][protobuf]) is a more compact binary encoding,
  introduced in Hrana 3.

[rfc8259]: https://datatracker.ietf.org/doc/html/rfc8259
[protobuf]: https://protobuf.dev/

This document defines protocol structures in JSON and specifies the schema using
TypeScript type notation. The Protobuf schema is described in proto3 syntax in
an appendix.

The encoding is negotiated between the server and client. This process depends
on the variant (WebSocket or HTTP) and is described later. All Hrana 3 servers
must support both JSON and Protobuf; clients can choose which encodings to
support and use.

Both encodings support forward compatibility: when a peer (client or server)
receives a protocol structure that includes an unrecognized field (object
property in JSON or a message field in Protobuf), it must ignore this field.



## Hrana over WebSocket

Hrana over WebSocket runs on top of the [WebSocket protocol][rfc6455].

### Version and encoding negotiation

The version of the protocol and the encoding is negotiated as a WebSocket
subprotocol: the client includes a list of supported subprotocols in the
`Sec-WebSocket-Protocol` request header in the opening handshake, and the server
replies with the selected subprotocol in the same response header.

The negotiation mechanism provides backward compatibility with older versions of
the Hrana protocol and forward compatibility with newer versions.

[rfc6455]: https://www.rfc-editor.org/rfc/rfc6455

The WebSocket subprotocols defined in all Hrana versions are as follows:

| Subprotocol | Version | Encoding | 
|-------------|---------|----------|
| `hrana1`    |       1 |     JSON |
| `hrana2`    |       2 |     JSON |
| `hrana3`    |       3 |     JSON |
| `hrana3-protobuf` | 3 | Protobuf |

This document describes version 3 of the Hrana protocol. Versions 1 and 2 are
described in their own specifications.

Version 3 of Hrana over WebSocket is designed to be a strict superset of
versions 1 and 2: every server that implements Hrana 3 over WebSocket also
implements versions 1 and 2 and should accept clients that indicate subprotocol
`hrana1` or `hrana2`.

### Overview

The client starts the connection by sending a _hello_ message, which
authenticates the client to the server. The server responds with either a
confirmation or with an error message, closing the connection. The client can
choose not to wait for the confirmation and immediately send further messages to
reduce latency.

A single connection can host an arbitrary number of streams. In effect, one
Hrana connection works as a "connection pool" in traditional SQL servers.

After a stream is opened, the client can execute SQL statements on it. For the
purposes of this protocol, the statements are arbitrary strings with optional
parameters.

To reduce the number of roundtrips, the protocol supports batches of statements
that are executed conditionally, based on success or failure of previous
statements. Clients can use this mechanism to implement non-interactive
transactions in a single roundtrip.

### Messages

If the negotiated encoding is JSON, all messages exchanged between the client
and server are sent as text frames (opcode 0x1) on the WebSocket. If the
negotiated encoding is Protobuf, messages are sent as binary frames (opcode
0x2).

```typescript
type ClientMsg =
    | HelloMsg
    | RequestMsg

type ServerMsg =
    | HelloOkMsg
    | HelloErrorMsg
    | ResponseOkMsg
    | ResponseErrorMsg
```

The client sends messages of type `ClientMsg`, and the server sends messages of
type `ServerMsg`. The type of the message is determined by its `type` field.

#### Hello

```typescript
type HelloMsg = {
    "type": "hello",
    "jwt": string | null,
}
```

The `hello` message is sent as the first message by the client. It authenticates
the client to the server using the [Json Web Token (JWT)][rfc7519] passed in the
`jwt` field. If no authentication is required (which might be useful for
development and debugging, or when authentication is performed by other means,
such as with mutual TLS), the `jwt` field might be set to `null`.

[rfc7519]: https://www.rfc-editor.org/rfc/rfc7519

The client can also send the `hello` message again anytime during the lifetime
of the connection to reauthenticate, by providing a new JWT. If the provided JWT
expires and the client does not provide a new one in a `hello` message, the
server may terminate the connection.

```typescript
type HelloOkMsg = {
    "type": "hello_ok",
}

type HelloErrorMsg = {
    "type": "hello_error",
    "error": Error,
}
```

The server waits for the `hello` message from the client and responds with a
`hello_ok` message if the client can proceed, or with a `hello_error` message
describing the failure.

The client may choose not to wait for a response to its `hello` message before
sending more messages to save a network roundtrip. If the server responds with
`hello_error`, it must ignore all further messages sent by the client and it
should close the WebSocket immediately.

#### Request/response

```typescript
type RequestMsg = {
    "type": "request",
    "request_id": int32,
    "request": Request,
}
```

After sending the `hello` message, the client can start sending `request`
messages. The client uses requests to open SQL streams and execute statements on
them. The client assigns an identifier to every request, which is then used to
match a response to the request.

The `Request` structure represents the payload of the request and is defined
later.

```typescript
type ResponseOkMsg = {
    "type": "response_ok",
    "request_id": int32,
    "response": Response,
}

type ResponseErrorMsg = {
    "type": "response_error",
    "request_id": int32,
    "error": Error,
}
```

When the server receives a `request` message, it must eventually send either a
`response_ok` with the response or a `response_error` that describes a failure.
The response from the server includes the same `request_id` that was provided by
the client in the request. The server can send the responses in arbitrary order.

The request ids are arbitrary 32-bit signed integers, the server does not
interpret them in any way.

The server should limit the number of outstanding requests to a reasonable
value, and stop receiving messages when this limit is reached. This will cause
the TCP flow control to kick in and apply back-pressure to the client. On the
other hand, the client should always receive messages, to avoid deadlock.

### Requests

Most of the work in the protocol happens in request/response interactions.

```typescript
type Request =
    | OpenStreamReq
    | CloseStreamReq
    | ExecuteReq
    | BatchReq
    | OpenCursorReq
    | CloseCursorReq
    | FetchCursorReq
    | SequenceReq
    | DescribeReq
    | StoreSqlReq
    | CloseSqlReq
    | GetAutocommitReq

type Response =
    | OpenStreamResp
    | CloseStreamResp
    | ExecuteResp
    | BatchResp
    | OpenCursorResp
    | CloseCursorResp
    | FetchCursorResp
    | SequenceResp
    | DescribeResp
    | StoreSqlReq
    | CloseSqlReq
    | GetAutocommitResp
```

The type of the request and response is determined by its `type` field. The
`type` of the response must always match the `type` of the request. The
individual requests and responses are defined in the rest of this section.

#### Open stream

```typescript
type OpenStreamReq = {
    "type": "open_stream",
    "stream_id": int32,
}

type OpenStreamResp = {
    "type": "open_stream",
}
```

The client uses the `open_stream` request to open an SQL stream, which is then
used to execute SQL statements. The streams are identified by arbitrary 32-bit
signed integers assigned by the client.

The client can optimistically send follow-up requests on a stream before it
receives the response to its `open_stream` request. If the server receives a
request that refers to a stream that failed to open, it should respond with an
error, but it should not close the connection.

Even if the `open_stream` request returns an error, the stream id is still
considered as used, and the client cannot reuse it until it sends a
`close_stream` request.

The server can impose a reasonable limit to the number of streams opened at the
same time.

> This request was introduced in Hrana 1.

#### Close stream

```typescript
type CloseStreamReq = {
    "type": "close_stream",
    "stream_id": int32,
}

type CloseStreamResp = {
    "type": "close_stream",
}
```

When the client is done with a stream, it should close it using the
`close_stream` request. The client can safely reuse the stream id after it
receives the response.

The client should close even streams for which the `open_stream` request
returned an error.

If there is an open cursor for the stream, the cursor is closed together with
the stream.

> This request was introduced in Hrana 1.

#### Execute a statement

```typescript
type ExecuteReq = {
    "type": "execute",
    "stream_id": int32,
    "stmt": Stmt,
}

type ExecuteResp = {
    "type": "execute",
    "result": StmtResult,
}
```

The client sends an `execute` request to execute an SQL statement on a stream.
The server responds with the result of the statement. The `Stmt` and
`StmtResult` structures are defined later.

If the statement fails, the server responds with an error response (message of
type `"response_error"`).

> This request was introduced in Hrana 1.

#### Execute a batch

```typescript
type BatchReq = {
    "type": "batch",
    "stream_id": int32,
    "batch": Batch,
}

type BatchResp = {
    "type": "batch",
    "result": BatchResult,
}
```

The `batch` request runs a batch of statements on a stream. The server responds
with the result of the batch execution.

If a statement in the batch fails, the error is returned inside the
`BatchResult` structure in a normal response (message of type `"response_ok"`).
However, if the server encounters a serious error that prevents it from
executing the batch, it responds with an error response (message of type
`"response_error"`).

> This request was introduced in Hrana 1.

#### Open a cursor executing a batch

```typescript
type OpenCursorReq = {
    "type": "open_cursor",
    "stream_id": int32,
    "cursor_id": int32,
    "batch": Batch,
}

type OpenCursorResp = {
    "type": "open_cursor",
}
```

The `open_cursor` request runs a batch of statements like the `batch` request,
but instead of returning all statement results in the request response, it opens
a _cursor_ which the client can then use to read the results incrementally.

The `cursor_id` is an arbitrary 32-bit integer id assigned by the client. This
id must be unique for the given connection and must not be used by another
cursor that was not yet closed using the `close_cursor` request.

Even if the `open_cursor` request returns an error, the cursor id is still
considered as used, and the client cannot reuse it until it sends a
`close_cursor` request.

After the `open_cursor` request, the client must not send more requests on the
stream until the cursor is closed using the `close_cursor` request.

> This request was introduced in Hrana 3.

#### Close a cursor

```typescript
type CloseCursorReq = {
    "type": "close_cursor",
    "cursor_id": int32,
}

type CloseCursorResp = {
    "type": "close_cursor",
}
```

The `close_cursor` request closes a cursor opened by an `open_cursor` request
and allows the server to release resources and continue processing other
requests for the given stream.

> This request was introduced in Hrana 3.

#### Fetch entries from a cursor

```typescript
type FetchCursorReq = {
    "type": "fetch_cursor",
    "cursor_id": int32,
    "max_count": uint32,
}

type FetchCursorResp = {
    "type": "fetch_cursor",
    "entries": Array<CursorEntry>,
    "done": boolean,
}
```

The `fetch_cursor` request reads data from a cursor previously opened with the
`open_cursor` request. The cursor data is encoded as a sequence of entries
(`CursorEntry` structure). `max_count` in the request specifies the maximum
number of entries that the client wants to receive in the response; however, the
server may decide to send fewer entries.

If the `done` field in the response is set to true, then the cursor is finished
and all subsequent calls to `fetch_cursor` are guaranteed to return zero
entries. The client should then close the cursor by sending the `close_cursor`
request.

If the `cursor_id` refers to a cursor for which the `open_cursor` request
returned an error, and the cursor hasn't yet been closed with `close_cursor`,
then the server should return an error, but it must not close the connection
(i.e., this is not a protocol error).

> This request was introduced in Hrana 3.

#### Store an SQL text on the server

```typescript
type StoreSqlReq = {
    "type": "store_sql",
    "sql_id": int32,
    "sql": string,
}

type StoreSqlResp = {
    "type": "store_sql",
}
```

The `store_sql` request stores an SQL text on the server. The client can then
refer to this SQL text in other requests by its id, instead of repeatedly
sending the same string over the network.

SQL text ids are arbitrary 32-bit signed integers assigned by the client. It is
a protocol error if the client tries to store an SQL text with an id which is
already in use.

> This request was introduced in Hrana 2.

#### Close a stored SQL text

```typescript
type CloseSqlReq = {
    "type": "close_sql",
    "sql_id": int32,
}

type CloseSqlResp = {
    "type": "close_sql",
}
```

The `close_sql` request can be used to delete an SQL text stored on the server
with `store_sql`. The client can safely reuse the SQL text id after it receives
the response.

It is not an error if the client attempts to close a SQL text id that is not
used.

> This request was introduced in Hrana 2.

#### Execute a sequence of SQL statements

```typescript
type SequenceReq = {
    "type": "sequence",
    "stream_id": int32,
    "sql"?: string | null,
    "sql_id"?: int32 | null,
}

type SequenceResp = {
    "type": "sequence",
}
```

The `sequence` request executes a sequence of SQL statements separated by
semicolons on the stream given by `stream_id`. `sql` or `sql_id` specify the SQL
text; exactly one of these fields must be specified.

Any rows returned by the statements are ignored. If any statement fails, the
subsequent statements are not executed and the request returns an error
response.

> This request was introduced in Hrana 2.

#### Describe a statement

```typescript
type DescribeReq = {
    "type": "describe",
    "stream_id": int32,
    "sql"?: string | null,
    "sql_id"?: int32 | null,
}

type DescribeResp = {
    "type": "describe",
    "result": DescribeResult,
}
```

The `describe` request is used to parse and analyze a SQL statement. `stream_id`
specifies the stream on which the statement is parsed. `sql` or `sql_id` specify
the SQL text: exactly one of these two fields must be specified, `sql` passes
the SQL directly as a string, while `sql_id` refers to a SQL text previously
stored with `store_sql`. In the response, `result` contains the result of
describing a statement.

> This request was introduced in Hrana 2.

#### Get the autocommit state

```typescript
type GetAutocommitReq = {
    "type": "get_autocommit",
    "stream_id": int32,
}

type GetAutocommitResp = {
    "type": "get_autocommit",
    "is_autocommit": bool,
}
```

The `get_autocommit` request can be used to check whether the stream is in
autocommit state (not inside an explicit transaction).

> This request was introduced in Hrana 3.

### Errors

If either peer detects that the protocol has been violated, it should close the
WebSocket with an appropriate WebSocket close code and reason. Some examples of
protocol violations include:

- Text message payload that is not a valid JSON.
- Data frame type that does not match the negotiated encoding (i.e., binary frame when
  the encoding is JSON or a text frame when the encoding is Protobuf).
- Unrecognized `ClientMsg` or `ServerMsg` (the field `type` is unknown or
  missing)
- Client receives a `ResponseOkMsg` or `ResponseErrorMsg` with a `request_id`
  that has not been sent in a `RequestMsg` or that has already received a
  response.

### Ordering

The protocol allows the server to reorder the responses: it is not necessary to
send the responses in the same order as the requests. However, the server must
process requests related to a single stream id in order.

For example, this means that a client can send an `open_stream` request
immediately followed by a batch of `execute` requests on that stream and the
server will always process them in correct order.



## Hrana over HTTP

Hrana over HTTP runs on top of HTTP. Any version of the HTTP protocol can be
used.

### Overview

HTTP is a stateless protocol, so there is no concept of a connection like in the
WebSocket protocol. However, Hrana needs to expose stateful streams, so it needs
to ensure that requests on the same stream are tied together.

This is accomplished by the use of a baton, which is similar to a session cookie.
The server returns a baton in every response to a request on the stream, and the
client then needs to include the baton in the subsequent request. The client
must serialize the requests on a stream: it must wait for a response to the
previous request before sending next request on the same stream.

The server can also optionally specify a different URL that the client should
use for the requests on the stream. This can be used to ensure that stream
requests are "sticky" and reach the same server.

If the client terminates without closing a stream, the server has no way of
finding this out: with Hrana over WebSocket, the WebSocket connection is closed
and the server can close the streams that belong to this connection, but there
is no connection in Hrana over HTTP. Therefore, the server will close streams
after a short period of inactivity, to make sure that abandoned streams don't
accumulate on the server.

### Version and encoding negotiation

With Hrana over HTTP, the client indicates the Hrana version and encoding in the
URI path of the HTTP request. The client can check whether the server supports a
given Hrana version by sending an HTTP request (described later).

### Endpoints

The client communicates with the server by sending HTTP requests with a
specified method and URL.

#### Check support for version 3 (JSON)

```
GET v3
```

If the server supports version 3 of Hrana over HTTP with JSON encoding, it
should return a 2xx response to this request.

#### Check support for version 3 (Protobuf)

```
GET v3-protobuf
```

If the server supports version 3 of Hrana over HTTP with Protobuf encoding, it
should return a 2xx response to this request.

#### Execute a pipeline of requests (JSON)

```
POST v3/pipeline
-> JSON: PipelineReqBody
<- JSON: PipelineRespBody
```

```typescript
type PipelineReqBody = {
    "baton": string | null,
    "requests": Array<StreamRequest>,
}

type PipelineRespBody = {
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

The `v3/pipeline` endpoint is used to execute a pipeline of requests on a
stream. `baton` in the request specifies the stream. If the client sets `baton`
to `null`, the server should create a new stream.

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

#### Execute a pipeline of requests (Protobuf)

```
POST v3-protobuf/pipeline
-> Protobuf: PipelineReqBody
<- Protobuf: PipelineRespBody
```

The `v3-protobuf/pipeline` endpoint is the same as `v3/pipeline`, but it encodes
the request and response body using Protobuf.

#### Execute a batch using a cursor (JSON)

```
POST v3/cursor
-> JSON: CursorReqBody
<- line of JSON: CursorRespBody
   lines of JSON: CursorEntry
```

```typescript
type CursorReqBody = {
    "baton": string | null,
    "batch": Batch,
}

type CursorRespBody = {
    "baton": string | null,
    "base_url": string | null,
}
```

The `v3/cursor` endpoint executes a batch of statements on a stream using a
cursor, so the results can be streamed from the server to the client.

The HTTP response is composed of JSON structures separated with a newline. The
first line contains the `CursorRespBody` structure, and the following lines
contain `CursorEntry` structures, which encode the result of the batch.

The `baton` field in the request and the `baton` and `base_url` fields in the
response have the same meaning as in the `v3/pipeline` endpoint.

#### Execute a batch using a cursor (Protobuf)

```
POST v3-protobuf/cursor
-> Protobuf: CursorReqBody
<- length-delimited Protobuf: CursorRespBody
   length-delimited Protobufs: CursorEntry
```

The `v3-protobuf/cursor` endpoint is the same as `v3/cursor` endpoint, but the
request and response are encoded using Protobuf.

In the response body, the structures are prefixed with a length delimiter: a
Protobuf varint that encodes the length of the structure. The first structure is
`CursorRespBody`, followed by an arbitrary number of `CursorEntry` structures.

### Requests

Requests in Hrana over HTTP closely mirror stream requests in Hrana over
WebSocket:

```typescript
type StreamRequest =
    | CloseStreamReq
    | ExecuteStreamReq
    | BatchStreamReq
    | SequenceStreamReq
    | DescribeStreamReq
    | StoreSqlStreamReq
    | CloseSqlStreamReq
    | GetAutocommitStreamReq

type StreamResponse =
    | CloseStreamResp
    | ExecuteStreamResp
    | BatchStreamResp
    | SequenceStreamResp
    | DescribeStreamResp
    | StoreSqlStreamResp
    | CloseSqlStreamResp
    | GetAutocommitStreamReq
```

#### Close stream

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

> This request was introduced in Hrana 2.

#### Execute a statement

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

The `execute` request has the same semantics as the `execute` request in Hrana
over WebSocket. 

> This request was introduced in Hrana 2.

#### Execute a batch

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

The `batch` request has the same semantics as the `batch` request in Hrana over
WebSocket.

> This request was introduced in Hrana 2.

#### Execute a sequence of SQL statements

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
Hrana over WebSocket.

> This request was introduced in Hrana 2.

#### Describe a statement

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
Hrana over WebSocket.

> This request was introduced in Hrana 2.

#### Store an SQL text on the server

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
Hrana over WebSocket, except that the scope of the SQL texts is just a single
stream (with WebSocket, it is the whole connection).

> This request was introduced in Hrana 2.

#### Close a stored SQL text

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
Hrana over WebSocket, except that the scope of the SQL texts is just a single
stream.

> This request was introduced in Hrana 2.

#### Get the autocommit state

```typescript
type GetAutocommitStreamReq = {
    "type": "get_autocommit",
}

type GetAutocommitStreamResp = {
    "type": "get_autocommit",
    "is_autocommit": bool,
}
```

The `get_autocommit` request has the same semantics as the `get_autocommit`
request in Hrana over WebSocket.

> This request was introduced in Hrana 3.

### Errors

If the client receives an HTTP error (4xx or 5xx response), it means that the
server encountered an internal error and the stream is no longer valid. The
client should attempt to parse the response body as an `Error` structure (using
the encoding indicated by the `Content-Type` response header), but the client
must be able to handle responses with different bodies, such as plaintext or
HTML, which might be returned by various components in the HTTP stack.



## Shared structures

This section describes protocol structures that are common for both Hrana over
WebSocket and Hrana over HTTP.

### Errors

```typescript
type Error = {
    "message": string,
    "code"?: string | null,
}
```

Errors can be returned by the server in many places in the protocol, and they
are always represented with the `Error` structure. The `message` field contains
an English human-readable description of the error. The `code` contains a
machine-readable error code.

At this moment, the error codes are not yet stabilized and depend on the server
implementation.

> This structure was introduced in Hrana 1.

### Statements

```typescript
type Stmt = {
    "sql"?: string | null,
    "sql_id"?: int32 | null,
    "args"?: Array<Value>,
    "named_args"?: Array<NamedArg>,
    "want_rows"?: boolean,
}

type NamedArg = {
    "name": string,
    "value": Value,
}
```

A SQL statement is represented by the `Stmt` structure. The text of the SQL
statement is specified either by passing a string directly in the `sql` field,
or by passing SQL text id that has previously been stored with the `store_sql`
request. Exactly one of `sql` and `sql_id` must be passed.

The arguments in `args` are bound to parameters in the SQL statement by
position. The arguments in `named_args` are bound to parameters by name.

In SQLite, the names of arguments include the prefix sign (`:`, `@` or `$`). If
the name of the argument does not start with this prefix, the server will try to
guess the correct prefix. If an argument is specified both as a positional
argument and as a named argument, the named argument should take precedence.

It is an error if the request specifies an argument that is not expected by the
SQL statement, or if the request does not specify an argument that is expected
by the SQL statement. Some servers may not support specifying both positional
and named arguments.

The `want_rows` field specifies whether the client is interested in the rows
produced by the SQL statement. If it is set to `false`, the server should always
reply with no rows, even if the statement produced some. If the field is
omitted, the default value is `true`.

The SQL text should contain just a single statement. Issuing multiple statements
separated by a semicolon is not supported.

> This structure was introduced in Hrana 1. In Hrana 2, the `sql_id` field was
> added and the `sql` and `want_rows` fields were made optional.

### Statement results

```typescript
type StmtResult = {
    "cols": Array<Col>,
    "rows": Array<Array<Value>>,
    "affected_row_count": uint64,
    "last_insert_rowid": string | null,
    "rows_read": uint64,
    "rows_written": uint64,
    "query_duration_ms": double,
}

type Col = {
    "name": string | null,
    "decltype": string | null,
}
```

The result of executing an SQL statement is represented by the `StmtResult`
structure and it contains information about the returned columns in `cols` and
the returned rows in `rows` (the array is empty if the statement did not produce
any rows or if `want_rows` was `false` in the request).

`affected_row_count` counts the number of rows that were changed by the
statement. This is meaningful only if the statement was an INSERT, UPDATE or
DELETE, and the value is otherwise undefined.

`last_insert_rowid` is the ROWID of the last successful insert into a rowid
table. The rowid value is a 64-bit signed integer encoded as a string in JSON.
For other statements, the value is undefined.

> This structure was introduced in Hrana 1. The `decltype` field in the `Col`
> strucure was added in Hrana 2.

### Batches

```typescript
type Batch = {
    "steps": Array<BatchStep>,
}

type BatchStep = {
    "condition"?: BatchCond | null,
    "stmt": Stmt,
}
```

A batch is represented by the `Batch` structure. It is a list of steps
(statements) which are always executed sequentially. If the `condition` of a
step is present and evaluates to false, the statement is not executed.

> This structure was introduced in Hrana 1.

#### Conditions

```typescript
type BatchCond =
    | { "type": "ok", "step": uint32 }
    | { "type": "error", "step": uint32 }
    | { "type": "not", "cond": BatchCond }
    | { "type": "and", "conds": Array<BatchCond> }
    | { "type": "or", "conds": Array<BatchCond> }
    | { "type": "is_autocommit" }
```

Conditions are expressions that evaluate to true or false:

- `ok` evaluates to true if the `step` (referenced by its 0-based index) was
executed successfully. If the statement was skipped, this condition evaluates to
false.
- `error` evaluates to true if the `step` (referenced by its 0-based index) has
produced an error. If the statement was skipped, this condition evaluates to
false.
- `not` evaluates `cond` and returns the logical negative.
- `and` evaluates `conds` and returns the logical conjunction of them.
- `or` evaluates `conds` and returns the logical disjunction of them.
- `is_autocommit` evaluates to true if the stream is currently in the autocommit
  state (not inside an explicit transaction)

> This structure was introduced in Hrana 1. The `is_autocommit` type was added in Hrana 3.

### Batch results

```typescript
type BatchResult = {
    "step_results": Array<StmtResult | null>,
    "step_errors": Array<Error | null>,
}
```

The result of executing a batch is represented by `BatchResult`. The result
contains the results or errors of statements from each step. For the step in
`steps[i]`, `step_results[i]` contains the result of the statement if the
statement was executed and succeeded, and `step_errors[i]` contains the error if
the statement was executed and failed. If the statement was skipped because its
condition evaluated to false, both `step_results[i]` and `step_errors[i]` will
be `null`.

> This structure was introduced in Hrana 1.

### Cursor entries

```typescript
type CursorEntry =
    | StepBeginEntry
    | StepEndEntry
    | StepErrorEntry
    | RowEntry
    | ErrorEntry
```

Cursor entries are produced by cursors. A sequence of entries encodes the same
information as a `BatchResult`, but it is sent to the client incrementally, so
both peers don't need to keep the whole result in memory.

> These structures were introduced in Hrana 3.

#### Step results

```typescript
type StepBeginEntry = {
    "type": "step_begin",
    "step": uint32,
    "cols": Array<Col>,
}

type StepEndEntry = {
    "type": "step_end",
    "affected_row_count": uint32,
    "last_insert_rowid": string | null,
}

type RowEntry = {
    "type": "row",
    "row": Array<Value>,
}
```

At the beginning of every batch step that is executed, the server produces a
`step_begin` entry. This entry specifies the index of the step (which refers to
the `steps` array in the `Batch` structure). The server sends entries for steps
in the order in which they are executed. If a step is skipped (because its
condition evalated to false), the server does not send any entry for it.

After a `step_begin` entry, the server sends an arbitrary number of `row`
entries that encode the individual rows produced by the statement, terminated by
the `step_end` entry. Together, these entries encode the same information as the
`StmtResult` structure.

The server can send another `step_entry` only after the previous step was
terminated by `step_end` or by `step_error`, described below.

#### Errors

```typescript
type StepErrorEntry = {
    "type": "step_error",
    "step": uint32,
    "error": Error,
}

type ErrorEntry = {
    "type": "error",
    "error": Error,
}
```

The `step_error` entry indicates that the execution of a statement failed with
an error. There are two ways in which the server may produce this entry:

1. Before a `step_begin` entry was sent: this means that the statement failed
   very early, without producing any results. The `step` field indicates which
   step has failed (similar to the `step_begin` entry).
2. After a `step_begin` entry was sent: in this case, the server has started
   executing the statement and produced `step_begin` (and perhaps a number of
   `row` entries), but then encountered an error. The `step` field must in this
   case be equal to the `step` of the currently processed step.

The `error` entry means that the execution of the whole batch has failed. This
can be produced by the server at any time, and it is always the last entry in
the cursor.

### Result of describing a statement

```typescript
type DescribeResult = {
    "params": Array<DescribeParam>,
    "cols": Array<DescribeCol>,
    "is_explain": boolean,
    "is_readonly": boolean,
}
```

The `DescribeResult` structure is the result of describing a statement.
`is_explain` is true if the statement was an `EXPLAIN` statement, and
`is_readonly` is true if the statement does not modify the database.

> This structure was introduced in Hrana 2.

#### Parameters

```typescript
type DescribeParam = {
    "name": string | null,
}
```

Information about parameters of the statement is returned in `params`. SQLite
indexes parameters from 1, so the first object in the `params` array describes
parameter 1.

For each parameter, the `name` field specifies the name of the parameter. For
parameters of the form `?NNN`, `:AAA`, `@AAA` and `$AAA`, the name includes the
initial `?`, `:`, `@` or `$` character. Parameters of the form `?` are nameless,
their `name` is `null`.

It is also possible that some parameters are not referenced in the statement, in
which case the `name` is also `null`.

> This structure was introduced in Hrana 2.

#### Columns

```typescript
type DescribeCol = {
    "name": string,
    "decltype": string | null,
}
```

Information about columns of the statement is returned in `cols`.

For each column, `name` specifies the name assigned by the SQL `AS` clause. For
columns without `AS` clause, the name is not specified.

For result columns that directly originate from tables in the database,
`decltype` specifies the declared type of the column. For other columns (such as
results of expressions), `decltype` is `null`.

> This structure was introduced in Hrana 2.

### Values

```typescript
type Value =
    | { "type": "null" }
    | { "type": "integer", "value": string }
    | { "type": "float", "value": number }
    | { "type": "text", "value": string }
    | { "type": "blob", "base64": string }
```

SQLite values are represented by the `Value` structure. The type of the value
depends on the `type` field:

- `null`: the SQL NULL value.
- `integer`: a 64-bit signed integer. In JSON, the `value` is a string to avoid
  losing precision, because some JSON implementations treat all numbers as
  64-bit floats.
- `float`: a 64-bit float.
- `text`: a UTF-8 string.
- `blob`: a binary blob with. In JSON, the value is base64-encoded.

> This structure was introduced in Hrana 1.




## Protobuf schema

### Hrana over WebSocket

```proto
syntax = "proto3";
package hrana.ws;

message ClientMsg {
  oneof msg {
    HelloMsg hello = 1;
    RequestMsg request = 2;
  }
}

message ServerMsg {
  oneof msg {
    HelloOkMsg hello_ok = 1;
    HelloErrorMsg hello_error = 2;
    ResponseOkMsg response_ok = 3;
    ResponseErrorMsg response_error = 4;
  }
}

message HelloMsg {
  optional string jwt = 1;
}

message HelloOkMsg {
}

message HelloErrorMsg {
  Error error = 1;
}

message RequestMsg {
  int32 request_id = 1;
  oneof request {
    OpenStreamReq open_stream = 2;
    CloseStreamReq close_stream = 3;
    ExecuteReq execute = 4;
    BatchReq batch = 5;
    OpenCursorReq open_cursor = 6;
    CloseCursorReq close_cursor = 7;
    FetchCursorReq fetch_cursor = 8;
    SequenceReq sequence = 9;
    DescribeReq describe = 10;
    StoreSqlReq store_sql = 11;
    CloseSqlReq close_sql = 12;
    GetAutocommitReq get_autocommit = 13;
  }
}

message ResponseOkMsg {
  int32 request_id = 1;
  oneof response {
    OpenStreamResp open_stream = 2;
    CloseStreamResp close_stream = 3;
    ExecuteResp execute = 4;
    BatchResp batch = 5;
    OpenCursorResp open_cursor = 6;
    CloseCursorResp close_cursor = 7;
    FetchCursorResp fetch_cursor = 8;
    SequenceResp sequence = 9;
    DescribeResp describe = 10;
    StoreSqlResp store_sql = 11;
    CloseSqlResp close_sql = 12;
    GetAutocommitResp get_autocommit = 13;
  }
}

message ResponseErrorMsg {
  int32 request_id = 1;
  Error error = 2;
}

message OpenStreamReq {
  int32 stream_id = 1;
}

message OpenStreamResp {
}

message CloseStreamReq {
  int32 stream_id = 1;
}

message CloseStreamResp {
}

message ExecuteReq {
  int32 stream_id = 1;
  Stmt stmt = 2;
}

message ExecuteResp {
  StmtResult result = 1;
}

message BatchReq {
  int32 stream_id = 1;
  Batch batch = 2;
}

message BatchResp {
  BatchResult result = 1;
}

message OpenCursorReq {
  int32 stream_id = 1;
  int32 cursor_id = 2;
  Batch batch = 3;
}

message OpenCursorResp {
}

message CloseCursorReq {
  int32 cursor_id = 1;
}

message CloseCursorResp {
}

message FetchCursorReq {
  int32 cursor_id = 1;
  uint32 max_count = 2;
}

message FetchCursorResp {
  repeated CursorEntry entries = 1;
  bool done = 2;
}

message StoreSqlReq {
  int32 sql_id = 1;
  string sql = 2;
}

message StoreSqlResp {
}

message CloseSqlReq {
  int32 sql_id = 1;
}

message CloseSqlResp {
}

message SequenceReq {
  int32 stream_id = 1;
  optional string sql = 2;
  optional int32 sql_id = 3;
}

message SequenceResp {
}

message DescribeReq {
  int32 stream_id = 1;
  optional string sql = 2;
  optional int32 sql_id = 3;
}

message DescribeResp {
  DescribeResult result = 1;
}

message GetAutocommitReq {
  int32 stream_id = 1;
}

message GetAutocommitResp {
  bool is_autocommit = 1;
}
```

### Hrana over HTTP

```proto
syntax = "proto3";
package hrana.http;

message PipelineReqBody {
  optional string baton = 1;
  repeated StreamRequest requests = 2;
}

message PipelineRespBody {
  optional string baton = 1;
  optional string base_url = 2;
  repeated StreamResult results = 3;
}

message StreamResult {
  oneof result {
    StreamResponse ok = 1;
    Error error = 2;
  }
}

message CursorReqBody {
  optional string baton = 1;
  Batch batch = 2;
}

message CursorRespBody {
  optional string baton = 1;
  optional string base_url = 2;
}

message StreamRequest {
  oneof request {
    CloseStreamReq close = 1;
    ExecuteStreamReq execute = 2;
    BatchStreamReq batch = 3;
    SequenceStreamReq sequence = 4;
    DescribeStreamReq describe = 5;
    StoreSqlStreamReq store_sql = 6;
    CloseSqlStreamReq close_sql = 7;
    GetAutocommitStreamReq get_autocommit = 8;
  }
}

message StreamResponse {
  oneof response {
    CloseStreamResp close = 1;
    ExecuteStreamResp execute = 2;
    BatchStreamResp batch = 3;
    SequenceStreamResp sequence = 4;
    DescribeStreamResp describe = 5;
    StoreSqlStreamResp store_sql = 6;
    CloseSqlStreamResp close_sql = 7;
    GetAutocommitStreamResp get_autocommit = 8;
  }
}

message CloseStreamReq {
}

message CloseStreamResp {
}

message ExecuteStreamReq {
  Stmt stmt = 1;
}

message ExecuteStreamResp {
  StmtResult result = 1;
}

message BatchStreamReq {
  Batch batch = 1;
}

message BatchStreamResp {
  BatchResult result = 1;
}

message SequenceStreamReq {
  optional string sql = 1;
  optional int32 sql_id = 2;
}

message SequenceStreamResp {
}

message DescribeStreamReq {
  optional string sql = 1;
  optional int32 sql_id = 2;
}

message DescribeStreamResp {
  DescribeResult result = 1;
}

message StoreSqlStreamReq {
  int32 sql_id = 1;
  string sql = 2;
}

message StoreSqlStreamResp {
}

message CloseSqlStreamReq {
  int32 sql_id = 1;
}

message CloseSqlStreamResp {
}

message GetAutocommitStreamReq {
}

message GetAutocommitStreamResp {
  bool is_autocommit = 1;
}
```

### Shared structures

```proto
syntax = "proto3";
package hrana;

message Error {
  string message = 1;
  optional string code = 2;
}

message Stmt {
  optional string sql = 1;
  optional int32 sql_id = 2;
  repeated Value args = 3;
  repeated NamedArg named_args = 4;
  optional bool want_rows = 5;
}

message NamedArg {
  string name = 1;
  Value value = 2;
}

message StmtResult {
  repeated Col cols = 1;
  repeated Row rows = 2;
  uint64 affected_row_count = 3;
  optional sint64 last_insert_rowid = 4;
}

message Col {
  optional string name = 1;
  optional string decltype = 2;
}

message Row {
  repeated Value values = 1;
}

message Batch {
  repeated BatchStep steps = 1;
}

message BatchStep {
  optional BatchCond condition = 1;
  Stmt stmt = 2;
}

message BatchCond {
  oneof cond {
    uint32 step_ok = 1;
    uint32 step_error = 2;
    BatchCond not = 3;
    CondList and = 4;
    CondList or = 5;
    IsAutocommit is_autocommit = 6;
  }

  message CondList {
    repeated BatchCond conds = 1;
  }

  message IsAutocommit {
  }
}

message BatchResult {
  map<uint32, StmtResult> step_results = 1;
  map<uint32, Error> step_errors = 2;
}

message CursorEntry {
  oneof entry {
    StepBeginEntry step_begin = 1;
    StepEndEntry step_end = 2;
    StepErrorEntry step_error = 3;
    Row row = 4;
    Error error = 5;
  }
}

message StepBeginEntry {
  uint32 step = 1;
  repeated Col cols = 2;
}

message StepEndEntry {
  uint64 affected_row_count = 1;
  optional sint64 last_insert_rowid = 2;
}

message StepErrorEntry {
  uint32 step = 1;
  Error error = 2;
}

message DescribeResult {
  repeated DescribeParam params = 1;
  repeated DescribeCol cols = 2;
  bool is_explain = 3;
  bool is_readonly = 4;
}

message DescribeParam {
  optional string name = 1;
}

message DescribeCol {
  string name = 1;
  optional string decltype = 2;
}

message Value {
  oneof value {
    Null null = 1;
    sint64 integer = 2;
    double float = 3;
    string text = 4;
    bytes blob = 5;
  }

  message Null {}
}
```
