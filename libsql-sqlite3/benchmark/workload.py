import sys
import numpy as np

def recall_uniform(dim, n, q):
    n = int(n)
    q = int(q)
    dim = int(dim)
    print(f'CREATE TABLE data ( id INTEGER PRIMARY KEY, emb FLOAT32({dim}) );')
    print(f'CREATE INDEX data_idx ON data( libsql_vector_idx(emb) );')
    print(f'CREATE TABLE queries ( emb FLOAT32({dim}) );')
    print(f'BEGIN TRANSACTION;')
    for i in range(n):
        vector = f"[{','.join(map(str, np.random.uniform(size=dim)))}]"
        print(f'INSERT INTO data VALUES ({i}, vector(\'{vector}\'));')
    for i in range(q):
        vector = f"[{','.join(map(str, np.random.uniform(size=dim)))}]"
        print(f'INSERT INTO queries VALUES (vector(\'{vector}\'));')
    print(f'COMMIT;')
    print('---insert everything')

def recall_normal(dim, n, q):
    n = int(n)
    q = int(q)
    dim = int(dim)
    print(f'CREATE TABLE data ( id INTEGER PRIMARY KEY, emb FLOAT32({dim}) );')
    print(f'CREATE TABLE queries ( emb FLOAT32({dim}) );')
    print(f'BEGIN TRANSACTION;')
    for i in range(n):
        vector = f"[{','.join(map(str, np.random.uniform(size=64)))}]"
        print(f'INSERT INTO data VALUES ({i}, \'{vector}\');')
    for i in range(q):
        vector = f"[{','.join(map(str, np.random.uniform(size=64)))}]"
        print(f'INSERT INTO queries VALUES (\'{vector}\');')
    print(f'COMMIT;')
    print('---insert everything')

def no_vectors(n, q):
    n = int(n)
    q = int(q)
    print('PRAGMA journal_mode=WAL;')
    print(f'CREATE TABLE x ( id INTEGER PRIMARY KEY, value TEXT );')
    for i in range(n):
        vector = f"[{','.join(map(str, np.random.uniform(size=64)))}]"
        print(f'INSERT INTO x VALUES ({i}, \'{vector}\');')
    print('---inserts')
    for i in range(q):
        print(f'SELECT id, value FROM x WHERE id = {np.random.randint(n)};')
    print('---search')

def bruteforce(dim, n, q):
    dim = int(dim)
    n = int(n)
    q = int(q)
    print('PRAGMA journal_mode=WAL;')
    print(f'CREATE TABLE x ( id INTEGER PRIMARY KEY, embedding FLOAT32({dim}) );')
    for i in range(n):
        vector = f"[{','.join(map(str, np.random.uniform(size=dim)))}]"
        print(f'INSERT INTO x VALUES ({i}, vector(\'{vector}\'));')
    print('---inserts')
    for i in range(q):
        vector = f"[{','.join(map(str, np.random.uniform(size=dim)))}]"
        print(f'SELECT id FROM x ORDER BY vector_distance_cos(embedding, vector(\'{vector}\')) LIMIT 1;')
    print('---search')

def diskann(dim, n, q):
    dim = int(dim)
    n = int(n)
    q = int(q)
    print('PRAGMA journal_mode=WAL;')
    print(f'CREATE TABLE x ( id INTEGER PRIMARY KEY, embedding FLOAT32({dim}) );')
    print(f'CREATE INDEX x_idx ON x( libsql_vector_idx(embedding) );')
    for i in range(n):
        vector = f"[{','.join(map(str, np.random.uniform(size=dim)))}]"
        print(f'INSERT INTO x VALUES ({i}, vector(\'{vector}\'));')
    print('---inserts')
    for i in range(q):
        vector = f"[{','.join(map(str, np.random.uniform(size=dim)))}]"
        print(f'SELECT id FROM vector_top_k(\'x_idx\', vector(\'{vector}\'), 1);')
    print('---search')

if __name__ == '__main__':
    globals()[sys.argv[1]](*sys.argv[2:])
