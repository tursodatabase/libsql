# Libsql-server admin API documentation

This document describes the admin API endpoints.

The admin API is used to manage namespaces on a `sqld` instance. Namespaces are isolated database within a same sqld instance.

To enable the admin API, and manage namespaces, two extra flags need to be passed to `sqld`:

- `--admin-listen-addr <addr>:<port>`: the address and port on which the admin API should listen. It must be different from the user API listen address (which defaults to port 8080).
- `--enable-namespaces`: enable namespaces for the instance. By default namespaces are disabled.

**The admin API has no authentication unless you also set `--admin-auth-key <key>` (or the `LIBSQL_ADMIN_AUTH_KEY` environment variable).** Without it, anyone who can reach `--admin-listen-addr` can create, delete, and fork any namespace, and can rewrite a namespace's own config, including its `jwt_key` (which controls who can authenticate to that namespace's user-facing API) and `heartbeat_url`/`dump_url` (both of which cause the server to make an outbound request to a URL in the request body). Always set `--admin-auth-key` when `--admin-listen-addr` is reachable from anywhere other than a fully trusted local process, and prefer binding it to a loopback or private-network address rather than `0.0.0.0`.

## Routes

```HTTP
POST /v1/namespaces/:namespace/create
```

Create a namespace named `:namespace`.
body:

```json
{
    "dump_url"?: string,
}
```

```HTTP
DELETE /v1/namespaces/:namespace
```

Delete the namespace named `:namespace`.

```HTTP
POST /v1/namespaces/:namespace/fork/:to
```

Fork `:namespace` into new namespace `:to`
