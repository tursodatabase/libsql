#!/usr/bin/env python3

import argparse
import struct

def vector_size(vector_dims):
    return 4 + vector_dims * 4

def neighbour_metadata_offset(block_size, vector_dims):
    vector_sz = vector_size(vector_dims)
    neighbour_vector_sz = vector_size(vector_dims)
    max_neighbours = int((block_size - 8 - 2 - vector_sz) / (neighbour_vector_sz + 16))
    return 8 + 2 + vector_sz + neighbour_vector_sz * max_neighbours

def parse_vector(file, blocksize):
    off = 0
    raw = file.read(blocksize)
    if not raw:
        return False
    id = struct.unpack("<q", raw[off:off+8])[0]
    off += 8
    print(f"ID: {id}")
    num_neighbours = struct.unpack("<h", raw[off:off+2])[0]
    off += 2
    print(f"Num neighbours: {num_neighbours}")
    vector_len = struct.unpack("<l", raw[off:off+4])[0]
    off += 4
    print(f"Vector length: {vector_len}")
    for i in range(vector_len):
        vector = struct.unpack("<f", raw[off:off+4])[0]
        off += 4
        print(f"Vector[{i}]: {vector}")
    for i in range(num_neighbours):
        neighbour_vector_len = struct.unpack("<l", raw[off:off+4])[0]
        off += 4
        print(f"Neighbour {i} vector length: {neighbour_vector_len}")
        for j in range(neighbour_vector_len):
            vector = struct.unpack("<f", raw[off:off+4])[0]
            off += 4
            print(f"Neighbour {i} vector[{j}]: {vector}")
    off = neighbour_metadata_offset(blocksize, vector_len)
    print(f"Neighbour metadata offset: {off}")
    for i in range(num_neighbours):
        id = struct.unpack("<q", raw[off:off+8])[0]
        off += 8
        print(f"Neighbour {i} ID: {id}")
        offset = struct.unpack("<q", raw[off:off+8])[0]
        off += 8
        print(f"Neighbour {i} offset: {offset}")
    return True

def parse_header(file):
    raw_header = file.read(32)
    header = struct.unpack("<qhhhhqq", raw_header)
    block_size = header[1] << 9
    file.read(block_size - 32)
    return {
        "magic": header[0],
        "block_size": header[1],
        "vector_type": header[2],
        "vector_dims": header[3],
        "similarity_func": header[4],
        "entry_vector_offset": header[5],
        "first_free_offset": header[6]
    }

def parse(filename):
    with open(filename, 'rb') as file:
        header = parse_header(file)
        print("==========================")
        print(f"Magic: {hex(header['magic'])}")
        print(f"Block size: {header['block_size']}")
        print(f"Vector type: {header['vector_type']}")
        print(f"Vector dimensions: {header['vector_dims']}")
        print(f"Similarity function: {header['similarity_func']}")
        print(f"Entry vector offset: {header['entry_vector_offset']}")
        print(f"First free offset: {header['first_free_offset']}")
        print("==========================")
        blocksize = header['block_size'] << 9
        while parse_vector(file, blocksize):
            print("==========================")

parser = argparse.ArgumentParser()
parser.add_argument('filename')

args = parser.parse_args()

parse(args.filename)
