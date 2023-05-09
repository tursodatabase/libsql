# The Hrana protocol specification (version 1)

Hrana (from Czech "hrana", which means "edge") is a protocol for connecting to a
SQLite database over a WebSocket. It is designed to be used from edge functions,
where low latency and small overhead is important.

## Motivation

This protocol aims to provide several benefits over the Postgres wire protocol:

- Works in edge runtimes: WebSockets are available in all edge runtimes
(Cloudflare Workers, Deno Deploy, Lagon), but general TCP sockets are not
(notably, sockets are not supported by Cloudflare Workers).

- Fast cold start: the Postgres wire protocol requires [at least two
roundtrips][pgwire-flow] before the client can send queries, but Hrana needs
just a single roundtrip introduced by the WebSocket protocol. (In both cases,
additional roundtrips might be necessary due to TLS.)

- Multiplexing: a single Hrana connection can open multiple SQL streams, so an
application needs to open just a single connection even if it handles multiple
concurrent requests.

- Simplicity: Hrana is a simple protocol, so a client needs few lines of
code. This is important on edge runtimes that impose hard limits on code size
(usually just a few MB).

[pgwire-flow]: https://www.postgresql.org/docs/current/protocol-flow.html

## Usage

The Hrana protocol is intended to be used in one of two ways:

- Connecting to `sqld`: edge functions and other clients can connect directly
to `sqld` using Hrana, because it has native support for the protocol. This is
the approach with lowest latency, because no software in the middle is
necessary.

- Connecting to SQLite through a proxy: this allows edge functions
to efficiently connect to an existing SQLite databases.

## Overview

The protocol runs on top of the [WebSocket protocol][rfc6455] as a subprotocol
`hrana1`. The client includes `hrana1` in the `Sec-WebSocket-Protocol` request
header in the opening handshake, and the server replies with `hrana1` in the
same response header. Future versions of the Hrana protocol will be negotiated
as different WebSocket subprotocols.

[rfc6455]: https://www.rfc-editor.org/rfc/rfc6455

The client starts the connection by sending a _hello_ message, which
authenticates the client to the server. The server responds with either a
confirmation or with an error message, closing the connection. The client can
choose not to wait for the confirmation and immediately send further messages to
reduce latency.

A single connection can host an arbitrary number of _streams_. A stream
corresponds to a "session" in PostgreSQL or a "connection" in SQLite: SQL
statements in a stream are executed sequentially and can affect stream-specific
state such as transactions (with SQL `BEGIN` or `SAVEPOINT`). In effect, one
Hrana connection works as a "connection pool" in traditional SQL servers.

After a stream is opened, the client can execute SQL _statements_ on it. For the
purposes of this protocol, the statements are arbitrary strings with optional
parameters. The protocol can thus work with any SQL dialect.

To reduce the number of roundtrips, the protocol supports batches of statements
that are executed conditionally, based on success or failure of previous
statements. This mechanism is used to implement non-interactive transactions in
a single roundtrip.

## Messages

All messages exchanged between the client and server are text messages encoded
in JSON. Future versions of the protocol might additionally support binary
messages with a more compact binary encoding.

This specification describes the JSON messages using TypeScript syntax as
follows:

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

To maintain backwards compatibility, the recipient must ignore any unrecognized
fields in the JSON messages. However, if the recipient receives a message with
unrecognized `type`, it must abort the connection.

### Hello

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

### Request/response

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

### Errors

```typescript
type Error = {
    "message": string,
    "code"?: string | null,
}
```

When a server refuses to accept a client `hello` or fails to process a
`request`, it responds with a message that describes the error. The `message`
field contains an English human-readable description of the error. The `code`
contains a machine-readable error code.

If either peer detects that the protocol has been violated, it should close the
WebSocket with an appropriate WebSocket close code and reason. Some examples of
protocol violations include:

- Text message that is not a valid JSON.
- Unrecognized `ClientMsg` or `ServerMsg` (the field `type` is unknown or
missing)
- Client receives a `ResponseOkMsg` or `ResponseErrorMsg` with a `request_id`
that has not been sent in a `RequestMsg` or that has already received a
response.

## Requests

Most of the work in the protocol happens in request/response interactions.

```typescript
type Request =
    | OpenStreamReq
    | CloseStreamReq
    | ExecuteReq
    | BatchReq

type Response =
    | OpenStreamResp
    | CloseStreamResp
    | ExecuteResp
    | BatchResp
```

The type of the request and response is determined by its `type` field. The
`type` of the response must always match the `type` of the request.

### Open stream

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

### Close stream

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

### Execute a statement

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
The server responds with the result of the statement.

```typescript
type Stmt = {
    "sql": string,
    "args"?: Array<Value>,
    "named_args"?: Array<NamedArg>,
    "want_rows": boolean,
}

type NamedArg = {
    "name": string,
    "value": Value,
}
```

A statement contains the SQL text in `sql` and arguments.

The arguments in `args` are bound to parameters in the SQL statement by
position. The arguments in `named_args` are bound to parameters by name.

For SQLite, the names of arguments include the prefix sign (`:`, `@` or `$`). If
the name of the argument does not start with this prefix, the server will try to
guess the correct prefix. If an argument is specified both as a positional
argument and as a named argument, the named argument should take precedence.

It is an error if the request specifies an argument that is not expected by the
SQL statement, or if the request does not specify an argument that is expected
by the SQL statement. Some servers may not support specifying both positional
and named arguments.

The `want_rows` field specifies whether the client is interested in the rows
produced by the SQL statement. If it is set to `false`, the server should always
reply with no rows, even if the statement produced some.

The SQL text should contain just a single statement. Issuing multiple statements
separated by a semicolon is not supported.

```typescript
type StmtResult = {
    "cols": Array<Col>,
    "rows": Array<Array<Value>>,
    "affected_row_count": int32,
    "last_insert_rowid": string | null,
}

type Col = {
    "name": string | null,
}
```

The result of executing an SQL statement contains information about the returned
columns in `cols` and the returned rows in `rows` (the array is empty if the
statement did not produce any rows or if `want_rows` was `false` in the request).

`affected_row_count` counts the number of rows that were changed by the
statement. This is meaningful only if the statement was an INSERT, UPDATE or
DELETE, and the value is otherwise undefined.

`last_insert_rowid` is the ROWID of the last successful insert into a rowid
table. The rowid value is a 64-bit signed integer encoded as a string. For
other statements, the value is undefined.

### Execute a batch

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

```typescript
type Batch = {
    "steps": Array<BatchStep>,
}

type BatchStep = {
    "condition"?: BatchCond | null,
    "stmt": Stmt,
}

type BatchResult = {
    "step_results": Array<StmtResult | null>,
    "step_errors": Array<Error | null>,
}
```

A batch is a list of steps (statements) which are always executed sequentially.
If the `condition` of a step is present and evaluates to false, the statement is
skipped.

The batch result contains the results or errors of statements from each step.
For the step in `steps[i]`, `step_results[i]` contains the result of the
statement if the statement was executed and succeeded, and `step_errors[i]`
contains the error if the statement was executed and failed. If the statement
was skipped because its condition evaluated to false, both `step_results[i]` and
`step_errors[i]` will be `null`.

```typescript
type BatchCond =
    | { "type": "ok", "step": int32 }
    | { "type": "error", "step": int32 }
    | { "type": "not", "cond": BatchCond }
    | { "type": "and", "conds": Array<BatchCond> }
    | { "type": "or", "conds": Array<BatchCond> }
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

### Values

```typescript
type Value =
    | { "type": "null" }
    | { "type": "integer", "value": string }
    | { "type": "float", "value": number }
    | { "type": "text", "value": string }
    | { "type": "blob", "base64": string }
```

Values passed as arguments to SQL statements and returned in rows are one of
supported types:

- `null`: the SQL NULL value
- `integer`: a 64-bit signed integer, its `value` is a string to avoid losing
precision, because some JSON implementations treat all numbers as 64-bit floats
- `float`: a 64-bit float
- `text`: a UTF-8 text string
- `blob`: a binary blob with base64-encoded value

These types exactly correspond to SQLite types. In the future, the protocol
might be extended with more types for compatibility with Postgres.

### Ordering

The protocol allows the server to reorder the responses: it is not necessary to
send the responses in the same order as the requests. However, the server must
process requests related to a single stream id in order.

For example, this means that a client can send an `open_stream` request
immediately followed by a batch of `execute` requests on that stream and the
server will always process them in correct order.
