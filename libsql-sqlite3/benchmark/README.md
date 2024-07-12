## benchmarks tools

Simple benchmark tools intentionally written in C in order to have faster feedback loops (no need to wait for Rust builds)

You need to install `numpy` for some scripts to work. You can do it globally or using virtual env:
```py
$> python -m venv .env
$> source .env/bin/activate
$> pip install -r requirements.txt
```

### benchtest

Simple generic tool which takes SQL file, db file and run all queries against provded DB file. 
For SQL file generation you can use/extend `workload.py` script.

Take a look at the example:
```sh
$> LD_LIBRARY_PATH=../.libs/ ./benchtest queries.sql data.db
open queries file at queries.sql
open sqlite db at 'data.db'
executed simple statement: 'CREATE TABLE t ( id INTEGER PRIMARY KEY, emb FLOAT32(4) );'
executed simple statement: 'CREATE INDEX t_idx ON t ( libsql_vector_idx(emb) );'
prepared statement: 'INSERT INTO t VALUES ( ?, vector(?) );'
inserts (queries.sql):
  insert: 710.25 micros (avg.), 4 (count)
  size  : 0.2695 MB
  reads : 1.00 (avg.), 4 (total)
  writes: 1.00 (avg.), 4 (total)
prepared statement: 'SELECT * FROM vector_top_k('t_idx', vector(?), ?);'
search (queries.sql):
  select: 63.25 micros (avg.), 4 (count)
  size  : 0.2695 MB
  reads : 1.00 (avg.), 4 (total)
```

It is linked against liblibsql.so which resides in the `../libs/` directory and must be explicitly built from `libsql-sqlite3` sources:
```sh
$> basename $(pwd)
libsql-sqlite3
$> make # this command will generate libs in the .libs directory
$> cd benchmark
$> make bruteforce
open queries file at bruteforce.sql
open sqlite db at 'test.db'
executed simple statement: 'PRAGMA journal_mode=WAL;'
executed simple statement: 'CREATE TABLE x ( id INTEGER PRIMARY KEY, embedding FLOAT32(64) );'
prepared statement: 'INSERT INTO x VALUES (?, vector(?));'
inserts (bruteforce.sql):
  insert: 46.27 micros (avg.), 1000 (count)
  size  : 0.2695 MB
  reads : 1.00 (avg.), 1000 (total)
  writes: 1.00 (avg.), 1000 (total)
prepared statement: 'SELECT id FROM x ORDER BY vector_distance_cos(embedding, vector(?)) LIMIT ?;'
search (bruteforce.sql):
  select: 329.32 micros (avg.), 1000 (count)
  size  : 0.2695 MB
  reads : 2000.00 (avg.), 2000000 (total)
```

### anntest

Simple tool which takes DB file with 2 tables `data (id INTEGER PRIMARY KEY, emb FLOAT32(n))` and `queries (emb FLOAT32(n))` and execute vector search for all vectors in `queries` table abainst `data` table using provided SQL statements. 

In order to generate DB file you can use `benchtest` with `workload.py` tools. Take a look at the example:
```sh
$> python workload.py recall_uniform 64 1000 1000 > recall_uniform.sql
$> LD_LIBRARY_PATH=../.libs/ ./benchtest recall_uniform.sql recall_uniform.db
$> # ./anntext [db path] [test name (used only for printed stats)] [ann query] [exact query]
$> LD_LIBRARY_PATH=../.libs/ ./anntest recall_uniform.db 10-recall@10 "SELECT rowid FROM vector_top_k('data_idx', ?, 10)" "SELECT id FROM data ORDER BY vector_distance_cos(emb, ?) LIMIT 10"
open sqlite db at 'recall_uniform.db'
ready to perform 1000 queries with SELECT rowid FROM vector_top_k('data_idx', ?, 10) ann query and SELECT id FROM data ORDER BY vector_distance_cos(emb, ?) LIMIT 10 exact query
88.91% 10-recall@10 (avg.)
```

### blobtest

Simple tool which aims to prove that `sqlite3_blob_reopen` API can substantially increase performance of reads.

Take a look at the example:
```sh
$> LD_LIBRARY_PATH=../.libs/ ./blobtest blob-read-simple.db read simple 1000 1000
open sqlite db at 'blob-read-simple.db'
blob table: ready to prepare
blob table: prepared
time: 3.76 micros (avg.), 1000 (count)
$> LD_LIBRARY_PATH=../.libs/ ./blobtest blob-read-reopen.db read reopen 1000 1000
open sqlite db at 'blob-read-reopen.db'
blob table: ready to prepare
blob table: prepared
time: 0.31 micros (avg.), 1000 (count)
```
