# Build and run sqld

There are three ways to build and run sqld:

- [Using Homebrew](#build-and-install-with-homebrew)
- [From source using Docker/Podman](#build-from-source-using-docker--podman)
- [From source using Rust](#build-from-source-using-rust)

## Query sqld

After building, you can query sqld using one of the provided [client
libraries](../#client-libraries).

By default, sqld persists database data in a directory `./data.sqld`. The file
`data` is a normal SQLite 3 compatible database file. You can work with it
directly using the SQLite CLI:

```bash
sqlite3 ./data.sqld/data
```

Be sure to stop sqld before using `sqlite3` like this.

## Build and install with Homebrew

The sqld formulae for Homebrew works with macOS, Linux (including WSL).

### 1. Add the tap `libsql/sqld` to Homebrew

```bash
brew tap libsql/sqld
```

### 2. Install the formulae `sqld-beta`

```bash
brew install sqld-beta
```

This builds and installs the binary `sqld` into `$HOMEBREW_PREFIX/bin/sqld`,
which should be in your PATH.

### 3. Verify that `sqld` works

```bash
sqld --help
```

## Build from source using Docker / Podman

To build sqld with Docker, you must have a Docker [installed] and running on
your machine with its CLI in your shell PATH.

[installed]: https://docs.docker.com/get-docker/

### 1. Clone this repo

Clone this repo using your preferred mechanism. You may want to use one of the
[sqld release tags].

Change to the `sqld` directory.

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

### 1. Setup

Install dependencies:

```bash
./scripts/install-deps.sh
```

### 2. Clone this repo

Clone this repo using your preferred mechanism. You may want to use one of the
[sqld release tags].

Change to the `sqld` directory.

Install git submodules:

```bash
git submodule update --init --force --recursive --depth 1
```

### 3. Build with cargo

```bash
cargo build
```

The sqld binary will be in `./target/debug/sqld`.

### 4. Verify the build

Check that sqld built successfully using its --help flag:

```bash
./target/debug/sqld --help
```

### 5. Run sqld with all defaults

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

### 6. Run tests (optional)

```console
make test
```


[sqld release tags]: /libsql/sqld/releases