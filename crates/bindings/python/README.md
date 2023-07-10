# libSQL API for Python

## Getting Started

#### Connecting to a database

```python
import libsql

con = libsql.connect("hello.db")
cur = con.cursor()
```

#### Creating a table

```python
cur.execute("CREATE TABLE users (id INTEGER, email TEXT);")
```

#### Inserting rows into a table

```python
cur.execute("INSERT INTO users VALUES (1, 'alice@example.org')")
```

#### Querying rows from a table

```python
print(cur.execute("SELECT * FROM users").fetchone())
```

## Developing

Setup the development environment:

```sh
python3 -m venv .env
source .env/bin/activate
pip3 install maturin pyperf pytest
```

Build the development version and use it:

```
maturin develop && python3 example.py
```

Run the tests:

```sh
pytest
```

Run the libSQL benchmarks:

```sh
python3 perf-libsql.py
```

Run the SQLite benchmarks for comparison:

```sh
python3 perf-sqlite3.py
```
