# Docker image quick reference

# How to use this image

## Launch a primary instance

```
docker run --name some-sqld -e SQLD_NODE=primary -d ghcr.io/libsql/sqld:main
```

## Launch a replica instance

```
docker run --name some-sqld -e SQLD_NODE=replica -D SQLD_PRIMARY_URL=https://<host>:<port> -d ghcr.io/libsql/sqld:main
```

# How to extend this image

## Environment variables

### `SQLD_NODE`

The `SQLD_NODE` environment variable configures the type of the launched instance. Possible values are: `primary` (default), `replica`, and `standalone`.
Please note that replica instances also need the `SQLD_PRIMARY_URL` environment variable to be defined.

### `SQLD_PRIMARY_URL`

The `SQLD_PRIMARY_URL` environment variable configures the gRPC URL of the primary instance for replica instances.

**See:** `SQLD_NODE` environment variable
