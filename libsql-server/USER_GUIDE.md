# `sqld` User Guide

## Deploying to Fly

First create a Fly config file (pick an application name):

```toml
app = "<app name>"
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

Then run (but say not for deploy):

```console
flyctl launch
```

Build the Docker image:

```console
podman build . -t sqld
```

Push to Fly registry:

```console
podman push --format v2s2 sqld:latest docker://registry.fly.io/<app name>:latest
```

Finally, deploy:

```console
flyctl deploy -i registry.fly.io/<app name>:latest
```

and allocate a IPv4 addres:

```
flyctl ips allocate-v4 -a <app name>
```

You now have `sqld` running on Fly listening to port `5000`.
