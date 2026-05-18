/*
** Run this script using the "sqlite3" command-line shell
** test test formatting of output text that contains
** vt100 escape sequences.
*/
.mode box -escape off
CREATE TEMP TABLE t1(a,b,c);
INSERT INTO t1 VALUES
  ('one','twotwotwo','thirty-three'),
  (unistr('\u001b[91mRED\u001b[0m'),'fourfour','fifty-five'),
  ('six','seven','eighty-eight');
.print With -escape off
SELECT * FROM t1;
.mode box -escape ascii
.print With -escape ascii
SELECT * FROM t1;
.mode box -escape symbol
.print With -escape symbol
SELECT * FROM t1;
