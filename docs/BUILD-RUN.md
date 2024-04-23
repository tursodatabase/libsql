# Build and run sqld

There are four ways to build and run sqld:

- [Download a prebuilt binary](#download-a-prebuilt-binary)
- [Using Homebrew](#build-and-install-with-homebrew)
- [Using a prebuilt Docker image](#using-a-prebuilt-docker-image)
- [From source using Docker/Podman](#build-from-source-using-docker--podman)
- [From source using Rust](#build-from-source-using-rust)

## Running sqld

You can simply run launch the executable with no command line arguments to run
an instance of sqld. By default, sqld listens on 127.0.0.1 port 8080 and
persists database data in a directory `./data.sqld`.

Use the `--help` flag to discover how to change its runtime behavior.

## Query sqld

You can query sqld using one of the provided [client
libraries](../libsql-server#client-libraries).

You can also use the [turso cli](https://docs.turso.tech/reference/turso-cli) to connect to the sqld instance:
```
turso db shell http://127.0.0.1:8000    
```

## Download a prebuilt binary

The [libsql-server release page](https://github.com/tursodatabase/libsql/releases) for this repository lists released versions of sqld
along with downloads for macOS and Linux.

## Build and install with Homebrew

The sqld formulae for Homebrew works with macOS, Linux (including WSL).

### 1. Add the tap `libsql/sqld` to Homebrew

```bash
brew tap libsql/sqld
```

### 2. Install the formulae `sqld`

```bash
brew install sqld
```

This builds and installs the binary `sqld` into `$HOMEBREW_PREFIX/bin/sqld`,
which should be in your PATH.

### 3. Verify that `sqld` works

```bash
sqld --help
```

## Using a prebuilt Docker image

The sqld release process publishes a Docker image to the GitHub Container
Registry. The URL is https://ghcr.io/tursodatabase/libsql-server. You can run the latest image locally
on port 8080 with the following:

```bash
docker run -p 8080:8080 -d ghcr.io/tursodatabase/libsql-server:latest
```

Or you can run a specific version using one of the [sqld container release
tags] in the following form for version X.Y.Z:

```bash
docker run -p 8080:8080 -d ghcr.io/tursodatabase/libsql-server:vX.Y.Z
```

## Build from source using Docker / Podman

To build sqld with Docker, you must have a Docker [installed] and running on
your machine with its CLI in your shell PATH.

[installed]: https://docs.docker.com/get-docker/

### 1. Clone this repo

Clone this repo using your preferred mechanism. You may want to use one of the
[sqld release tags].

### 2. Build with Docker

Run the following to build a Docker image named "libsql/sqld" tagged with
version "latest".

```bash
docker build -t libsql/sqld:latest .
```

### 3. Verify the build

Check that sqld built successfully using its --help flag:

```bash
docker container run \
  --rm \
  -i \
  libsql/sqld \
  /bin/sqld --help
```

### 4. Create a data volume

The following will create a volume named `sqld-data` that sqld uses to persist
database files.

```bash
docker volume create sqld-data
```

### 5. Run sqld in a container

The following uses the built image to create and run a new container named
`sqld`, attaching the `sqld-data` volume to it, and exposing its port 8080
locally:

```bash
docker container run \
  -d \
  --name sqld \
  -v sqld-data:/var/lib/sqld \
  -p 127.0.0.1:8080:8080 \
  libsql/sqld:latest
```

8080 is the default port for the sqld HTTP service that handles client queries.
With this container running, you can use the URL `http://127.0.0.1:8080` or
`ws://127.0.0.1:8080` to configure one of the libSQL client SDKs for local
development.

### 6. Configure sqld with environment variables

In the sqld output using `--help` from step 3, you saw the names of command line
flags along with the names of environment variables (look for "env:") used to
configure the way sqld works.

## Build from source using Rust

To build from source, you must have a Rust development environment installed and
available in your PATH.

Currently we only support building sqld on macOS and Linux (including WSL). We
are working native Windows build instructions.

### 1. Clone this repo

Clone this repo using your preferred mechanism. You may want to use one of the
[sqld release tags].

Change to the `libsql-server` directory.

### 2. Build with cargo

```bash
cargo build
```

The sqld binary will be in `./target/debug/sqld`.

### 3. Verify the build

Check that sqld built successfully using its --help flag:

```bash
./target/debug/sqld --help
```

### 4. Run sqld with all defaults

The following starts sqld, taking the following defaults:

- Local files stored in the directory `./data.sqld`
- Client HTTP requests on 127.0.0.1:8080

```bash
./target/debug/sqld
```

8080 is the default port for the sqld HTTP service that handles client queries.
With this container running, you can use the URL `http://127.0.0.1:8080` or
`ws://127.0.0.1:8080` to configure one of the libSQL client SDKs for local
development.

### 5. Run tests (optional)

```console
cargo xtask test
```


[sqld releases page]: https://github.com/libsql/sqld/releases
[sqld container release tags]: https://github.com/libsql/sqld/pkgs/container/sqld
[sqld release tags]: https://github.com/libsql/sqld/releases
