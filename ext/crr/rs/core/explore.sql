-- backfill

INSERT INTO x__crsql_clock VALUES (SELECT * FROM x);

-- ^-- you need to unroll the columns