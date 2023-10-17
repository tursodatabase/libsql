.bail on
.echo on
.load ../../target/debug/bottomless
.open file:test.db?wal=bottomless
PRAGMA page_size=65536;
PRAGMA journal_mode=wal;
PRAGMA page_size;
DROP TABLE IF EXISTS test;
CREATE TABLE test(v);
INSERT INTO test VALUES (42);
INSERT INTO test VALUES (zeroblob(8193));
INSERT INTO test VALUES ('hey');
.mode column

BEGIN;
INSERT INTO test VALUES ('presavepoint');
INSERT INTO test VALUES (zeroblob(1600000));
INSERT INTO test VALUES (zeroblob(1600000));
INSERT INTO test VALUES (zeroblob(2400000));
SAVEPOINT test1;
INSERT INTO test VALUES (43);
INSERT INTO test VALUES (zeroblob(2000000));
INSERT INTO test VALUES (zeroblob(2000000));
INSERT INTO test VALUES (zeroblob(2000000));
INSERT INTO test VALUES ('heyyyy');
ROLLBACK TO SAVEPOINT test1;
COMMIT;

BEGIN;
INSERT INTO test VALUES (3.16);
INSERT INTO test VALUES (zeroblob(1000000));
INSERT INTO test VALUES (zeroblob(1000000));
INSERT INTO test VALUES (zeroblob(1000000));
ROLLBACK;

PRAGMA wal_checkpoint(FULL);

INSERT INTO test VALUES (3.14);
INSERT INTO test VALUES (zeroblob(31400));

PRAGMA wal_checkpoint(PASSIVE);
PRAGMA wal_checkpoint(PASSIVE);

INSERT INTO test VALUES (997);

SELECT v, length(v) FROM test;
.exit
