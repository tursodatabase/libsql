# SQLD HTTP API

This is the documentation for the sqld HTTP API.

## Usage

### The `Value` type

The `Value` type represents an SQLite value. It has 4 variants:

- Text: a UTF-8 encoded string
- Integer: a 64-bit signed integer
- Real: a 64-bits floating number
- Blob: some binary data, encoded in base64
- Null: the null value.

All these types map to JSON straightforwardly, except for blobs, that are represented as an object with { "base64": /* base64 encoded blob */}

### Response format

Responses to queries can either succeed or fail. When they succeed a payload specific to the endpoint being called is returned with a HTTP 200 (OK) status code.

In the case of a failure, a specific `Error` response is returned with the approriate HTTP status code. The `Error` response has the following structure:

```ts
type Error = {
    error: string
}
```

The general structure of a response is:

```ts
type Response<T> = T | Error;
```

Where `T` is the type of the payload in case of success.

### Routes

#### Queries

```
POST /
```

This endpoint supports sending batches of queries to the database. All of the statements in the batch are executed as part of a transaction. If any statement in the batch fails, an error is returned and the transaction is aborted, resulting in no change to the database.

The HTTP API is stateless, which means that interactive transactions are not possible. Since all batches are executed as part of transactions, any transaction statements (e.g `BEGIN`, `END`, `ROLLBACK`...) are forbidden and will yield an error.

##### Body

The body for the query request has the following format:

```ts
type QueryBody = {
    statements: Array<Query>
}

type Query = string | ParamQuery;
type ParamQuery = { q: string, params: undefined | Record<string, Value> | Array<Value> }
```

Queries are either simple strings or `ParamQuery` that accept parameter bindings. The `statements` arrays can contain a mix of the two types.

##### Response Format

On success, a request to `POST /` returns a response with an HTTP 200 code and a JSON body with the following structure:
```ts
type BatchResponse = Array<QueryResult>|Error

type QueryResult = {
    results: {
        columns: Array<string>,
        rows: Array<Array<Value>>,
        rows_read: uint64,
        rows_written: uint64,
        query_duration_ms: double
    }
}

```

Each `QueryResult` entry in the `BatchResponse` array corresponds to a query in the request.
The `BatchResponse` is either an `Error` or a set of `QueryResult`s.

The `Query` can either be a plain query string, such as `SELECT * FROM users` or `INSERT INTO users VALUES ("adhoc")`, or objects for queries with bound parameters.

##### Parameter binding

Queries with bound parameters come in two types:

1. Named bound parameters, where the parameter is referred to by a name and is prefixed with a `:`, a `@` or a `$`. If the query uses named parameters, then the `params` field of the query should be an object mapping parameters to their value.

- Example: a query with named bound parameters

```json
{
    "q": "SELECT * FROM users WHERE name = :name AND age = &age AND height > @height AND address = $address",
    "params": {
        ":name": "adhoc",
        "age" : "18",
        "@height" : "170",
        "$address" : "very nice place",
    }
}
```
The prefix of the parameter does not have to be specified in the `params` field (i.e, `name` instead of `:name`). If a
param `name` is given in `params` it will be binded to `:name`, `$name` and `@name` unless `params` contain a better
match. `:name` is a better match for `:name` than `name`.
One named parameter can occur in a query multiple times but does not have to be repeated in `params`.

2. Positional query parameters, bound by their position in the parameter list, and prefixed `?`. If the query uses positional parameters, the values should be provided as an array to the `params` field.

- Example: a query with positional bound parameters

```json
{
    "q": "SELECT * FROM users WHERE name = ?",
    "params": ["adhoc"]
}
```

#### Health

```
GET /health
```

The health route return an `HTTP 200 (OK)` if the server is up and running.

#### Version

```
GET /version
```

returns the server's version.
