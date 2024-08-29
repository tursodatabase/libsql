#!/usr/bin/env python3

import re
import struct
import argparse
import graphviz
import libsql_client
from dataclasses import dataclass

def vector_size(v_type, v_dims):
    return v_dims * {"f": 4, "d": 8}[v_type]

def neighbour_metadata_offset(block_size, v_type, v_dims):
    vector_sz = vector_size(v_type, v_dims)
    neighbour_vector_sz = vector_size(v_type, v_dims)
    max_neighbours = int((block_size - 8 - 2 - vector_sz) / (neighbour_vector_sz + 16))
    return 8 + 2 + vector_sz + neighbour_vector_sz * max_neighbours

def unpack(buffer, offset, format):
    result = struct.unpack_from(format, buffer=buffer, offset=offset)
    return offset + struct.calcsize(format), result

@dataclass
class Block:
    id: int
    vector: tuple[float]

    n_count: int
    n_vectors: list[tuple[float]]
    n_ids: list[int]

def parse_block(block, v_type, v_dims):
    offset = 0
    offset, (id, n_count) = unpack(block, offset, "<qh")
    offset, (vector) = unpack(block, offset, "<" + v_type * v_dims)
    n_vectors, n_ids = [], []
    for i in range(n_count):
        offset, n_vector = unpack(block, offset, "<" + v_type * v_dims)
        n_vectors.append(n_vector)
    offset = neighbour_metadata_offset(65536, v_type, v_dims)
    for i in range(n_count):
        offset, (_, n_id) = unpack(block, offset, "<qq")
        n_ids.append(n_id)
    return Block(id=id, vector=vector, n_count=n_count, n_vectors=n_vectors, n_ids=n_ids)


def parse(filename, shadow_idx):
    suffix = '_shadow'
    if not shadow_idx.endswith(suffix):
        raise Exception(f'unexpected shadow table name: {shadow_idx}')
    with libsql_client.create_client_sync('file:' + filename) as client:
        table_name = client.execute(f'SELECT tbl_name FROM sqlite_master WHERE name = ?', [shadow_idx[:-len(suffix)]]).rows[0][0]
        table_ddl = client.execute(f'SELECT sql FROM sqlite_master WHERE name = ?', [table_name]).rows[0][0]
        vector_column = re.search('(FLOAT32|FLOAT64|F32_BLOB|F64_BLOB)\\((\\d+)\\)', str(table_ddl))
        if vector_column:
            v_type = vector_column.group(1)
            v_dims = vector_column.group(2)
            v_type = {"FLOAT32": "f", "F32_BLOB": "f", "FLOAT64": "d", "F64_BLOB": "d"}[v_type]
            v_dims = int(v_dims)
        else:
            raise Exception(f'unexpected vector column type name: {table_ddl}')

        result = client.execute(f'SELECT rowid, data FROM {shadow_idx}')
        dot = graphviz.Digraph(comment='Index Graph')
        for row in result:
            block = parse_block(row['data'], v_type, v_dims)
            if len(block.vector) == 2:
                dot.node(f'{block.id}', pos=f"{block.vector[0] * 20},{block.vector[1] * 20}!")
            for n_id in block.n_ids:
                dot.edge(f'{block.id}', f'{n_id}')
        print(dot.source)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('filename')
    parser.add_argument('shadow_idx')
    args = parser.parse_args()
    parse(args.filename, args.shadow_idx)

if __name__ == '__main__':
    main()
