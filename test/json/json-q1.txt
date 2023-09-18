.mode qbox
.timer on
.param set $label 'q87'
SELECT rowid, x->>$label FROM data1 WHERE x->>$label IS NOT NULL;

CREATE TEMP TABLE t2(x JSON TEXT);
WITH RECURSIVE
  c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<25000),
  array1(y) AS (
    SELECT json_group_array(
             json_object('x',x,'y',random(),'z',hex(randomblob(50)))
           )
      FROM c
  ),
  c2(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c2 WHERE n<5)
INSERT INTO t2(x)
  SELECT json_object('a',n,'b',n*2,'c',y,'d',3,'e',5,'f',6) FROM array1, c2;
CREATE INDEX t2x1 ON t2(x->>'a');
CREATE INDEX t2x2 ON t2(x->>'b');
CREATE INDEX t2x3 ON t2(x->>'e');
CREATE INDEX t2x4 ON t2(x->>'f');
UPDATE t2 SET x=json_replace(x,'$.f',(x->>'f')+1);
UPDATE t2 SET x=json_set(x,'$.e',(x->>'f')-1);
UPDATE t2 SET x=json_remove(x,'$.d');
