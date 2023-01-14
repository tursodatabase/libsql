# Performance Testing

Setup database:

```
psql -h 127.0.0.1 -p 5432 < pg_bench_schema.sql
````

Run `pgbench`:

```console
pgbench -h 127.0.0.1 -p 5432 -f pg_bench_script.sql -c 10 -t 1000
```
