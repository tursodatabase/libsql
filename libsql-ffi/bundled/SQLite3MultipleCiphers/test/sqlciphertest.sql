.echo on
-- Test to access database encrypted with SQLCipher version 1
-- Result: 75709 1 1 one one 1 2 one two 1 2
.open test/sqlcipher-1.1.8-testkey.db
pragma cipher='sqlcipher';
pragma legacy=1;
pragma key='testkey';
SELECT COUNT(*) FROM t1;
SELECT DISTINCT * FROM t1;

-- Test to access database encrypted with SQLCipher version 2
-- using 4000 iterations for the HMAC key derivation and a HMAC salt mask of zero
-- Result: 38768 test-0-0 test-0-1 test-1-0 test-1-1
.open test/sqlcipher-2.0-beta-testkey.db
pragma cipher='sqlcipher';
pragma legacy=2;
pragma fast_kdf_iter=4000;
pragma hmac_salt_mask=0;
pragma key='testkey';
SELECT COUNT(*) FROM t1;
SELECT DISTINCT * FROM t1;

-- Test to access database encrypted with SQLCipher version 2
-- using the page number in big endian form (BE) for the HMAC calculation
-- Result: 78536 1 1 one one 1 2 one two
.open test/sqlcipher-2.0-be-testkey.db
pragma cipher='sqlcipher';
pragma legacy=2;
pragma hmac_pgno=2;
pragma key='testkey';
SELECT COUNT(*) FROM t1;
SELECT DISTINCT * FROM t1;

-- Test to access database encrypted with SQLCipher version 2
-- using the page number in little endian form (LE) for the HMAC calculation
-- Note: No change to the default initialization necessary
-- Result: 78536 1 1 one one 1 2 one two
.open test/sqlcipher-2.0-le-testkey.db
pragma cipher='sqlcipher';
pragma legacy=2;
pragma key='testkey';
SELECT COUNT(*) FROM t1;
SELECT DISTINCT * FROM t1;

--  // Test to access database encrypted with SQLCipher version 3
--  // Result: 78536 1 1 one one 1 2 one two
.open test/sqlcipher-3.0-testkey.db
pragma cipher='sqlcipher';
pragma legacy=3;
pragma key='testkey';
SELECT COUNT(*) FROM t1;
SELECT DISTINCT * FROM t1;

--  // Test to access database encrypted with SQLCipher version 4
--  // Result: 78536 1 1 one one 1 2 one two
.open test/sqlcipher-4.0-testkey.db
pragma cipher='sqlcipher';
pragma legacy=4;
pragma key='testkey';
SELECT COUNT(*) FROM t1;
SELECT DISTINCT * FROM t1;
