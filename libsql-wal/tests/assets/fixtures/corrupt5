CREATE TABLE t1(a,b,c);
CREATE INDEX i1 ON t1(a,b);
PRAGMA writable_schema=ON;
UPDATE sqlite_master SET name=NULL, sql=NULL WHERE name='i1';