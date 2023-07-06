#!/usr/bin/env python3

import sqlite3
import libsql
import pytest

@pytest.mark.parametrize("provider", ["libsql", "sqlite"])
def test_basic(provider):
    conn = connect(provider, ":memory:")
    cur = conn.cursor()
    cur.execute("CREATE TABLE users (id INTEGER, email TEXT)")
    cur.execute("INSERT INTO users VALUES (1, 'alice@example.com')")
    res = cur.execute("SELECT * FROM users")
    assert (1, 'alice@example.com') == res.fetchone()

@pytest.mark.parametrize("provider", ["libsql", "sqlite"])
def test_params(provider):
    conn = connect(provider, ":memory:")
    cur = conn.cursor()
    cur.execute("CREATE TABLE users (id INTEGER, email TEXT)")
    cur.execute("INSERT INTO users VALUES (?, ?)", (1, 'alice@example.com'))
    res = cur.execute("SELECT * FROM users")
    assert (1, 'alice@example.com') == res.fetchone()

def connect(provider, database):
    if provider == "libsql":
        return libsql.connect(database)
    if provider == "sqlite":
        return sqlite3.connect(database)
    raise Exception(f"Provider `{provider}` is not supported")
