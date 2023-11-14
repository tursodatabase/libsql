# The Hrana protocol specification (version 2)

Hrana (from Czech "hrana", which means "edge") is a protocol for connecting to a
SQLite database over a WebSocket. It is designed to be used from edge functions,
where low latency and small overhead is important.

In this specification, version 2 of the protocol is described as a set of
extensions to version 1.

Version 2 is designed to be a strict superset of version 1: every server that
implements version 2 also implements version 1.

## Version negotiation

The Hrana protocol version 2 uses a WebSocket subprotocol `hrana2`. The
WebSocket subprotocol negotiation allows the client and server to use version 2
of the protocol if both peers support it, but fall back to version 1 if the
client or the server don't support version 2.

## Messages

### Hello

The `hello` message has the same format as in version 1. The client must send it
as the first message, but in version 2, the client can also send it again
anytime during the lifetime of the connection to reauthenticate, by providing a
new JWT.

This feature was introduced because, in long-living connections, the JWT used to
authenticate the client may expire and the server may terminate the connection.
Using this feature, the client can provide a fresh JWT, thus keeping the
connection properly authenticated.

## Requests

Version 2 introduces four new requests:

```typescript
type Request =
    | ...
    | SequenceReq
    | DescribeReq
    | StoreSqlReq
    | CloseSqlReq

type Response =
    | ...
    | SequenceResp
    | DescribeResp
    | StoreSqlReq
    | CloseSqlReq
```

### Store an SQL text on the server

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
an error if the client tries to store an SQL text with an id which is already in
use.

### Close a stored SQL text

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

### Execute a sequence of SQL statements

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

### Describe a statement

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

```typescript
type DescribeResult = {
    "params": Array<DescribeParam>,
    "cols": Array<DescribeCol>,
    "is_explain": boolean,
    "is_readonly": boolean,
}
```

In the result, `is_explain` is true if the statement was an `EXPLAIN` statement,
and `is_readonly` is true if the statement does not modify the database.

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

## Other changes

### Statement

```typescript
type Stmt = {
    "sql"?: string | undefined,
    "sql_id"?: int32 | undefined,
    "args"?: Array<Value>,
    "named_args"?: Array<NamedArg>,
    "want_rows"?: boolean,
}
```

In version 2 of the protocol, the SQL text of a statement can be specified
either by passing a string directly in the `sql` field, or by passing SQL text
id that has previously been stored with the `store_sql` request. Exactly one of
`sql` and `sql_id` must be passed.

Also, the `want_rows` field is now optional and defaults to `true`.

### Statement result

```typescript
type Col = {
    "name": string | null,
    "decltype": string | null,
}
```

In version 2 of the protocol, the column descriptor in the statement result also
includes the declared type of the column (if available).
