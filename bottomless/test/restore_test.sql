.bail on
.echo on
.load ../../target/debug/bottomless
.open file:test.db?wal=bottomless&immutable=1
.mode column
SELECT v, length(v) FROM test;
