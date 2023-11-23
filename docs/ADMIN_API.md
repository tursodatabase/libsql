# Libsql-server admin API documentation

This document describes the admin API endpoints.

The admin API is used to manage namespaces on a `sqld` instance. Namespaces are isolated database within a same sqld instance.

To enable the admin API, and manage namespaces, two extra flags need to be passed to `sqld`:
- `--admin-listen-addr <addr>:<port>`: the address and port on which the admin API should listen. It must be different from the user API listen address (whi defaults to port 8080).
- `--enable-namespaces`: enable namespaces for the instance. By default namespaces are disabled.

## Routes

```
POST /v1/namespaces/:namespace/create
```
Create a namespace named `:namespace`.
body:
```json
{
    "dump_url"?: string,
}
```

```
DELETE /v1/namespaces/:namespace
```

Delete the namespace named `:namespace`.

```
POST /v1/namespaces/:namespace/fork/:to
```
Fork `:namespace` into new namespace `:to`
