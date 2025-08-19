# Vector Concat and Slice Functions for libSQL

This project adds two new vector functions to libSQL to maintain compatibility with Turso:

1. `vector_concat(X, Y)` - Concatenates two vectors of the same type.
2. `vector_slice(X, start_idx, end_idx)` - Extracts a subvector from start_idx (inclusive) to end_idx (exclusive).

## Implementation

The implementation is based on the existing vector functions in libSQL. The new functions are added to the `vector.c` file in the `libsql-sqlite3/src` directory.

### `vector_concat(X, Y)`

This function concatenates two vectors of the same type. It performs the following steps:

1. Parse the two input vectors
2. Check that both vectors are of the same type
3. Allocate a new vector with dimensions equal to the sum of the dimensions of the input vectors
4. Copy the data from both vectors into the new vector
5. Return the new vector

### `vector_slice(X, start_idx, end_idx)`

This function extracts a slice of a vector from start_idx (inclusive) to end_idx (exclusive). It performs the following steps:

1. Parse the input vector
2. Validate the start and end indices:
   - Both must be non-negative
   - start_idx must not be greater than end_idx
   - Both must be within the bounds of the vector
3. Allocate a new vector with dimensions equal to end_idx - start_idx
4. Copy the appropriate slice of data from the input vector to the new vector
5. Return the new vector

Note: FLOAT1BIT vectors are not yet supported for the slice operation due to the complexity of bit-by-bit extraction.

## Usage

```sql
-- Create a test table with a vector column
CREATE TABLE test_vectors (
  id INTEGER PRIMARY KEY,
  vec VECTOR
);

-- Insert some test vectors
INSERT INTO test_vectors VALUES (1, vector32(1, 2, 3, 4, 5));
INSERT INTO test_vectors VALUES (2, vector32(6, 7, 8, 9, 10));

-- Concatenate vectors
SELECT vector_extract(vector_concat(vec, vector32(11, 12, 13))) FROM test_vectors WHERE id = 1;
-- Returns: [1.0, 2.0, 3.0, 4.0, 5.0, 11.0, 12.0, 13.0]

-- Slice a vector
SELECT vector_extract(vector_slice(vec, 1, 4)) FROM test_vectors WHERE id = 1;
-- Returns: [2.0, 3.0, 4.0]
```

## Testing

A test file `test_vector_functions.sql` is provided to verify the implementation.

## Building

The implementation is integrated directly into the libSQL SQLite fork. To build it, follow the standard libSQL build process.

## References

- Turso Implementation: https://github.com/tursodatabase/turso/pull/2336
- libSQL Issue: https://github.com/tursodatabase/libsql/issues/2136
