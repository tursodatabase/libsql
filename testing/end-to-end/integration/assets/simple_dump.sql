PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE person (name, age);
INSERT INTO person VALUES('adhoc',27);
INSERT INTO person VALUES('john',42);
CREATE TABLE pets (kind);
INSERT INTO pets VALUES('cat');
INSERT INTO pets VALUES('dog');
COMMIT;
