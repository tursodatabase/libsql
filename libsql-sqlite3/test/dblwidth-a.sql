/*
** Run this script using "sqlite3" to confirm that the command-line
** shell properly handles the output of double-width characters.
**
** https://sqlite.org/forum/forumpost/008ac80276
*/
.mode box
CREATE TABLE data(word TEXT, description TEXT);
INSERT INTO data VALUES('〈οὐκέτι〉','Greek without dblwidth <...>');
.print .mode box
SELECT * FROM data;
.mode table
.print .mode table
SELECT * FROM data;
.mode qbox
.print .mode qbox
SELECT * FROM data;
.mode column
.print .mode column
SELECT * FROM data;
