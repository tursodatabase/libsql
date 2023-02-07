# Ruby SQLite client acceptance tests

These are Ruby SQLite client acceptance tests for the `sqld` server.

## Getting Started

```console
bundle install
```

```console
bundle exec rspec sqlite_spec.rb
```

The default database URL can be configured using DB_URI env variable. It's especially
important if your local postgres requires authentication. In that case, you
can use

```console
DB_URI=postgres://asd:password@127.0.0.1:5432 bundle exec rspec sqlite_spec.rb
````
