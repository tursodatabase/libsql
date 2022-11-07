# libSQL extensions

This document describes extensions to the library provided by libSQL, not available in upstream SQLite at the time of writing.

## RANDOM ROWID

Regular tables use an implicitly defined, unique, 64-bit rowid column as its primary key.
If rowid value is not specified during insertion, it's auto-generated with the following heuristics:
 1. Find the current max rowid value.
 2. If max value is less than i64::max, use the next available value
 3. If max value is i64::max:
     a. pick a random value
     b. if it's not taken, use it
     c. if it's taken, go to (a.), rinse, repeat

Based on this algorithm, the following trick can be used to trick libSQL into generating random rowid values instead of consecutive ones - simply insert a sentinel row with `rowid = i64::max`.

The newly introduced `RANDOM ROWID` option can be used to explicitly state that the table generates random rowid values on insertions, without having to insert a dummy row with special rowid value, or manually trying to generate a random unique rowid, which some user applications may find problematic.

### Usage

`RANDOM ROWID` keywords can be used during table creation, in a manner similar to its syntactic cousin, `WITHOUT ROWID`:
```sql
CREATE TABLE shopping_list(item text, quantity int) RANDOM ROWID;
```

On insertion, pseudorandom rowid values will be generated:
```sql
CREATE TABLE shopping_list(item text, quantity int) RANDOM ROWID;
INSERT INTO shopping_list(item, quantity) VALUES ('bread', 2);
INSERT INTO shopping_list(item, quantity) VALUES ('butter', 1);
.mode column
SELECT rowid, * FROM shopping_list;
rowid                item    quantity
-------------------  ------  --------
1177193729061749947  bread   2       
4433412906245401374  butter  1  
```

### Restrictions

`RANDOM ROWID` is mutually exclusive with `WITHOUT ROWID` option, and cannot be used with tables having an `AUTOINCREMENT` primary key.
