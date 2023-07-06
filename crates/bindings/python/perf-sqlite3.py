#!/usr/bin/env python3
import pyperf
import sqlite3
import time

con = sqlite3.connect(":memory:")
cur = con.cursor()

def func():
    res = cur.execute("SELECT 1")
    res.fetchone()

runner = pyperf.Runner()
runner.bench_func('execute SELECT 1', func)
