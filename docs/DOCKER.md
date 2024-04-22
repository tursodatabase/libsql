# Docker image quick reference

## Launch a primary instance

```
docker run --name some-sqld -p 8080:8080 -ti \ 
    -e SQLD_NODE=primary \
    ghcr.io/tursodatabase/libsql-server:latest
```

## Launch a replica instance

```
docker run --name some-sqld-replica -p 8081:8080 -ti 
    -e SQLD_NODE=replica \
    -e SQLD_PRIMARY_URL=https://<host>:<port> \
    ghcr.io/tursodatabase/libsql-server:lastest
````

## Running on Apple Silicon

```
docker run --name some-sqld  -p 8080:8080 -ti \ 
    -e SQLD_NODE=primary \
    --platform linux/amd64 \
    ghcr.io/tursodatabase/libsql-server:latest
```

_Note: the latest images for arm64 are available under the tag
`ghcr.io/tursodatabase/libsql-server:latest-arm`, however for tagged versions,
and stable releases please use the x86_64 versions via Rosetta._

## Docker Repository

[https://github.com/tursodatabase/libsql/pkgs/container/libsql-server](https://github.com/tursodatabase/libsql/pkgs/container/libsql-server)

# How to extend this image

## Data Persistance

Database files are stored in the `/var/lib/sqld` in the image. To persist the 
database across runs, mount this location to either a docker volume or a bind 
mount on your local disk.

```
docker run --name some-sqld -ti \
    -v ./.data/libsql \
    -e SQLD_NODE=primary \ 
    ghcr.io/tursodatabase/libsql-server:latest
```

## Environment variables

### `SQLD_NODE`

**default:** `primary`

The `SQLD_NODE` environment variable configures the type of the launched
instance. Possible values are: `primary` (default), `replica`, and `standalone`.
Please note that replica instances also need the `SQLD_PRIMARY_URL` environment
variable to be defined.

### `SQLD_PRIMARY_URL`

The `SQLD_PRIMARY_URL` environment variable configures the gRPC URL of the primary instance for replica instances.

**See:** `SQLD_NODE` environment variable

### `SQLD_DB_PATH`

**default:** `iku.db`

The location of the db file inside the container. Specifying only a filename
will place the file in the default directory inside the container at
`/var/lib/sqld`.

### `SQLD_HTTP_LISTEN_ADDR`

**default:** `0.0.0.0:8080`

Defines the HTTP listen address that sqld listens on and clients will connect
to. Recommended to leave this on the default port and remap ports at the
container networking level.

### `SQLD_GRPC_LISTEN_ADDR`

**default:** `0.0.0.0:5001`

Defines the GRPC listen address and port for sqld. Primarily used for
inter-node communication. Recommended to leave this on default.


## Docker Compose

Simple docker compose for local development:

```
version: "3"
services:
  db:
    image: ghcr.io/tursodatabase/libsql-server:latest
    platform: linux/amd64
    ports:
      - "8080:8080"
      - "5001:5001"
    # environment:
    #   - SQLD_NODE=primary
    volumes:
      - ./data/libsql:/var/lib/sqld
```
