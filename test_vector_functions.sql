.open :memory:

-- Create a test table with a vector column
CREATE TABLE test_vectors (
  id INTEGER PRIMARY KEY,
  vec VECTOR
);

-- Insert some test vectors
INSERT INTO test_vectors VALUES (1, vector32(1, 2, 3, 4, 5));
INSERT INTO test_vectors VALUES (2, vector32(6, 7, 8, 9, 10));
INSERT INTO test_vectors VALUES (3, vector64(1.1, 2.2, 3.3, 4.4, 5.5));
INSERT INTO test_vectors VALUES (4, vector64(6.6, 7.7, 8.8, 9.9, 10.10));

-- Test vector_concat
SELECT id, vector_extract(vector_concat(vec, vector32(11, 12, 13))) AS concat_result
FROM test_vectors 
WHERE id = 1;

SELECT id, vector_extract(vector_concat(vec, vector32(11, 12, 13))) AS concat_result
FROM test_vectors 
WHERE id = 2;

SELECT id, vector_extract(vector_concat(vec, vector64(11.11, 12.12, 13.13))) AS concat_result
FROM test_vectors 
WHERE id = 3;

-- Test that concat requires same vector types
SELECT vector_extract(vector_concat(vector32(1, 2, 3), vector64(4, 5, 6))) AS should_fail;

-- Test vector_slice
SELECT id, vector_extract(vector_slice(vec, 1, 4)) AS slice_result
FROM test_vectors 
WHERE id = 1;

SELECT id, vector_extract(vector_slice(vec, 0, 2)) AS slice_result
FROM test_vectors 
WHERE id = 3;

-- Test vector_slice edge cases
-- Out of bounds
SELECT vector_extract(vector_slice(vector32(1, 2, 3, 4, 5), 5, 10)) AS should_fail;

-- Negative indices
SELECT vector_extract(vector_slice(vector32(1, 2, 3, 4, 5), -1, 3)) AS should_fail;

-- End smaller than start
SELECT vector_extract(vector_slice(vector32(1, 2, 3, 4, 5), 3, 1)) AS should_fail;

-- Zero length slice
SELECT vector_extract(vector_slice(vector32(1, 2, 3, 4, 5), 2, 2)) AS zero_length_slice;

-- Full slice
SELECT vector_extract(vector_slice(vector32(1, 2, 3, 4, 5), 0, 5)) AS full_slice;
