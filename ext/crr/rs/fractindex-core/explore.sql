SELECT crsql_orderings(after_primary_key, collection_column, collection_id, table, ordering_column);

WITH "cte" AS (
  SELECT "id", ${order_column}, row_number() OVER (ORDER BY ${order_column}) as "rn" FROM ${table} WHERE ${collection_id} = ${collection}
), "current" AS (
  SELECT "rn" FROM "cte"
  WHERE "id" = ${after_id}
)
SELECT "cte"."id", "cte".${order_column} FROM "cte", "current"
  WHERE ABS("cte"."rn" - "current"."rn") <= 1
ORDER BY "cte"."rn"


-- that'll get us the rows we need
-- then we need to go down on before until we hit a distinct before

-- if we collide on before too, run this to find before before.
SELECT "${order_column}" FROM ${table} WHERE ${collection_id} = ${collection} AND "${order_column}" < ${before} ORDER BY "${order_column}" DESC LIMIT 1


-- https://gist.github.com/Azarattum/0071f6dea0d2813c0b164b8d34ac2a1f


-- below would return the new assignments
-- where order is applied based on the selected row in the where statement
-- we know pks from schema.
-- we know order column if user defines it.
SELECT id, order FROM foo_order WHERE collection_id = 1 AND item_id = 1 ORDER BY order ASC;
/**


UPDATE foo_order SET 
*/

UPDATE todo
   SET order = orderings.order
  FROM (SELECT crsql_orderings(...)) AS orderings
 WHERE todo.id = orderings.id;

CREATE VIRTUAL TABLE foo_fract USING crsql_fractional_index (order_column_name);

^-- from here we create the vtab based on the existing table schema.
^-- - make all columns available
^-- - 

-- interesting but no good given they scan the entire sub-query
select prev_row, target_row, next_row
from (
    select 
           lag(_rowid_) over (order by {ordering}) as prev_row,
           _rowid_ as target_row,
           lead(_rowid_) over (order by {ordering}) as next_row
    from {tbl}
    where {collection_id} = {collection}
) as t
where target_pk = 'target_pk';


select crsql_fract_key_between(target_row_order, next_row_order)
from (
    select 
           _rowid_ as target_row,
           lead(_rowid_) over (order by {ordering}) as next_row
    from {tbl}
    where {collection_id} = {collection}
) as t
where target_pk = 'target_pk';


-- ^-- extend key_between to return NULL if provided with colliding keys


select prev_row, target_row, next_row
from (
    select 
           id,
           lag(_rowid_) over (order by id) as prev_row,
           _rowid_ as target_row,
           lead(_rowid_) over (order by id) as next_row
    from todo
    where list_id = 1
) as t
where t.id = 1;

-- as point queries:

-- Count the number of rows in the table with the same ordering value as the target row.
-- case when that.

SELECT crsql_fract_key_between(
  (SELECT ordering FROM todo WHERE id = 1 AND list_id = 2 ORDER BY ordering DESC LIMIT 1, 1),
  (SELECT ordering FROM todo WHERE id = 1 AND list_id = 2 ORDER BY ordering ASC LIMIT 1, 1)
);

SELECT max(
  (SELECT ordering FROM todo WHERE id = 1 AND list_id = 2 ORDER BY ordering DESC LIMIT 1, 1),
  (SELECT ordering FROM todo WHERE id = 1 AND list_id = 2 ORDER BY ordering ASC LIMIT 1, 1)
);

-- ^- simple. Two point queries. Common case of no collisions we'll just get what we need back.
-- If we collide, things get ineresting. We need to find the next distinct ordering value.

-- on collision we do this
SELECT ordering FROM todo WHERE list_id = y AND ordering < (SELECT ordering FROM todo WHERE id = x) ORDER BY ordering LIMIT 1;

-- and bump the target row down to a slot between the returned ordering and its current value.
-- down because we're inserting after.
-- the new insert receives the value that the old thing had

-- Find the row before the target row
-- we will need to move that row
SELECT _rowid_, ordering FROM todo 
  JOIN (SELECT list_id FROM todo WHERE id = 1) as t ON todo.list_id = t.list_id ORDER BY ordering DESC LIMIT 1, 1;


--  alt:

-- find row b4 target row

after_ordering = SELECT ordering FROM todo WHERE {after_id_predicates};

UPDATE todo SET ordering = crsql_fract_key_between(
  SELECT ordering FROM todo WHERE {list_predicates} AND ordering < {after_ordering},
  {after_ordering}
) WHERE {after_id_predicates}

return after_ordering;

---

CREATE TRIGGER IF NOT EXISTS \"{table}_fractindex_update_trig\"
  INSTEAD OF UPDATE ON \"{table}_fractindex\"
  BEGIN
    UPDATE \"{table}\" SET
      {base_sets_ex_order},
      \"{order_col}\" = CASE (
        SELECT count(*) FROM \"{table}\" WHERE {list_predicates} AND \"{order_col}\" = (
          SELECT \"{order_col}\" FROM \"{table}\" WHERE {after_predicates}
        )
      )
      WHEN 0 THEN crsql_fract_key_between(
        (SELECT \"{order_col}\" FROM \"{table}\" WHERE {after_predicates}),
        (SELECT \"{order_col}\" FROM \"{table}\" WHERE {list_predicates} AND \"{order_col}\" > (
          SELECT \"{order_col}\" FROM \"{table}\" WHERE {after_predicates}
        ) LIMIT 1)
      )
      ELSE crsql_fract_fix_conflict_return_old_key(
        ?, ?, {list_bind_slots}{maybe_comma}, -1, ?, {after_pk_values}
      );
  END;
  