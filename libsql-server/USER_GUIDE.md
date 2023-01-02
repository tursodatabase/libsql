# Iku-Turso User Guide

## Deploying to Fly

Fly config file:

```toml
app = "iku-turso"
kill_signal = "SIGINT"
kill_timeout = 5
processes = []

[env]

[experimental]
  allowed_public_ports = []
  auto_rollback = true

[[services]]
  internal_port = 5000
  protocol = "tcp"

  [[services.ports]]
    port = 5000
```

Build image:

```console
podman build . -t iku-turso
```

Push to Fly registry:

```console
podman push --format v2s2 iku-turso:latest docker://registry.fly.io/iku-turso:latest
```

Deploy:

```console
flyctl deploy -i registry.fly.io/iku-turso:latest
```
