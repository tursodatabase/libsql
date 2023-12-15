/*
 * These tests are very similar to `pk_only_tables` tests.
 * What we want to test here is that rows whose primary keys get changed get
 * replicated correctly.
 *
 * Example:
 * ```
 * CREATE TABLE foo (id primary key, value);
 * ```
 *
 * | id | value |
 * | -- | ----- |
 * | 1  |  abc  |
 *
 * Now we:
 * ```
 * UPDATE foo SET id = 2 WHERE id = 1;
 * ```
 *
 * This should be a _delete_ of row id 1 and a _create_ of
 * row id 2, bringing all the values from row 1 to row 2.
 *
 * pk_only_tables.rs tested this for table that _only_
 * had primary key columns but not for tables that have
 * primary key columns + other columns.
 */
