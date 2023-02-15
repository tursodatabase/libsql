# sqld client for Python

This is a Python client for [sqld][sqld], the server mode for [libSQL][libsql].

[sqld]: https://github.com/libsql/sqld
[libsql]: https://libsql.org/

## Getting started

To get started, you need [`sqld` running somewhere][sqld]. Then you can install this package with:

```
$ pip install libsql-client
```

and use it like this:

```python
import asyncio
import libsql_client

async def main():
    url = "http://localhost:8080"
    async with libsql_client.Client(url) as client:
        result_set = await client.execute("SELECT * from users")
        print(len(result_set.rows), "rows")
        for row in result_set.rows:
            print(row)

asyncio.run(main())
```

You can also connect to a local SQLite database simply by changing the URL:

```python
url = "file:example.db"
```

## Contributing to this package

First, please install Python and [Poetry][poetry]. To install all dependencies for local development to a
virtual environment, run:

[poetry]: https://python-poetry.org/

```
poetry install -G dev
```

To run the tests, use:

```
poetry run pytest
```

To check types with MyPy, use:

```
poetry run mypy
```
