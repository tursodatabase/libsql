# `sqld` User Guide

## Deploying to Fly

You can use the existing `fly.toml` file from this repository.

Just run
```console
flyctl launch
```
... then pick a name and respond "Yes" when the prompt asks you to deploy.

Finaly, allocate a IPv4 addres:
```
flyctl ips allocate-v4 -a <your app name>
```

You now have `sqld` running on Fly listening to port `5000`.
