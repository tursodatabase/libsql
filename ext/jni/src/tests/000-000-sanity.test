/*
** This is a comment. There are many like it but this one is mine.
**
** SCRIPT_MODULE_NAME:      sanity-check
** xMIXED_MODULE_NAME:       mixed-module
** xMODULE_NAME:             module-name
** xREQUIRED_PROPERTIES:      small fast reliable
** xREQUIRED_PROPERTIES:      RECURSIVE_TRIGGERS
** xREQUIRED_PROPERTIES:      TEMPSTORE_FILE TEMPSTORE_MEM
** xREQUIRED_PROPERTIES:      AUTOVACUUM INCRVACUUM
**
*/
--print starting up 😃
--close all
--oom
--db 0
--new my.db
--null zilch
--testcase 1.0
SELECT 1, null;
--result 1 zilch
--glob *zil*
--notglob *ZIL*
SELECT 1, 2;
intentional error;
--run
--testcase json-1
SELECT json_array(1,2,3)
--json [1,2,3]
--testcase tableresult-1
  select 1, 'a';
  select 2, 'b';
--tableresult
  # [a-z]
  2 b
--end
--testcase json-block-1
  select json_array(1,2,3);
  select json_object('a',1,'b',2);
--json-block
  [1,2,3]
  {"a":1,"b":2}
--end
--testcase col-names-on
--column-names 1
  select 1 as 'a', 2 as 'b';
--result a 1 b 2
--testcase col-names-off
--column-names 0
  select 1 as 'a', 2 as 'b';
--result 1 2
--close
--print reached the end 😃
