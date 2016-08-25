IN-Operator Implementation Notes
================================

## Definitions:

An IN operator has one of the following formats:

>
     x IN (list)
     x IN (subquery)

The "x" is referred to as the LHS (left-hand side).  The list or subquery
on the right is called the RHS (right-hand side).  If the RHS is a list
it must be a non-empty list.  But if the RHS is a subquery, it can be an
empty set.

Both the LHS and RHS can be scalars or vectors.  The two must match.
In other words, they must both be scalar or else they must both be
vectors of the same length.

NULL values can occur in either or both of the LHS and RHS.
If the LHS contains only
NULL values then we say that it is a "total-NULL".  If the LHS contains
some NULL values and some non-NULL values, then it is a "partial-NULL".
For a scalar, there is no difference between a partial-NULL and a total-NULL.
The RHS is a partial-NULL if any row contains a NULL value.  The RHS is
a total-NULL if it contains one or more rows that contain only NULL values.
The LHS is called "non-NULL" if it contains no NULL values.  The RHS is
called "non-NULL" if it contains no NULL values in any row.

The result of an IN operator is one of TRUE, FALSE, or NULL.  A NULL result
means that it cannot be determined if the LHS is contained in the RHS due
to the presence of NULL values.  In some contexts (for example, when the IN
operator occurs in a WHERE clause)
the system only needs a binary result: TRUE or NOT-TRUE.  One can also
to define a binary result of FALSE and NOT-FALSE, but
it turns out that no extra optimizations are possible in that case, so if
the FALSE/NOT-FALSE binary is needed, we have to compute the three-state
TRUE/FALSE/NULL result and then combine the TRUE and NULL values into 
NOT-FALSE.

A "NOT IN" operator is computed by first computing the equivalent IN
operator, then interchanging the TRUE and FALSE results.

## Simple Full-Scan Algorithm

The following algorithm always compute the correct answer.  However, this
algorithm is suboptimal, especially if there are many rows on the RHS.

  1.  Set the null-flag to false
  2.  For each row in the RHS:
      <ol type='a'>
      <li>  Compare the LHS against the RHS
      <li>  If the LHS exactly matches the RHS, immediately return TRUE
      <li>  If the comparison result is NULL, set the null-flag to true
      </ol>
  3.  If the null-flag is true, return NULL.
  4.  Return FALSE

## Optimized Algorithm

The following procedure computes the same answer as the simple full-scan
algorithm, though it does so with less work in the common case.  This
is the algorithm that is implemented in SQLite.  The steps must occur
in the order specified.  Except for the INDEX_NOOP optimization of step 1,
none of the steps can be skipped.

  1.  If the RHS is a constant list of length 1 or 2, then rewrite the
      IN operator as a simple expression.  Implement

            x IN (y1,y2)

      as if it were

            x=y1 OR x=y2

      This is the INDEX_NOOP optimization and is only undertaken if the
      IN operator is used for membership testing.  If the IN operator is
      driving a loop, then skip this step entirely.

  2.  If the RHS is empty, return FALSE.

  3.  If the LHS is a total-NULL or if the RHS contains a total-NULL,
      then return NULL.

  4.  If the LHS is non-NULL, then use the LHS as a probe in a binary
      search of the RHS 

      <ol type='a'>
      <li> If the binary search finds an exact match, return TRUE

      <li> If the RHS is known to be not-null, return FALSE
      </ol>

  5.  At this point, it is known that the result cannot be TRUE.  All
      that remains is to distinguish between NULL and FALSE.
      If a NOT-TRUE result is acceptable, then return NOT-TRUE now.

  6.  For each row in the RHS, compare that row against the LHS and
      if the result is NULL, immediately return NULL.  This step is
      essentially the "Simple Full-scan Algorithm" above with the
      tests for TRUE removed, since we know that the result cannot be
      TRUE at this point.

  7.  Return FALSE.
