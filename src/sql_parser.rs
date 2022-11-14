pub fn is_transaction_start(stmt: &str) -> bool {
    // TODO: Add support for Savepoints
    //       Savepoints are named transactions that can be nested.
    //       See https://www.sqlite.org/lang_savepoint.html
    stmt.trim_start().to_uppercase().starts_with("BEGIN")
}

pub fn is_transaction_end(stmt: &str) -> bool {
    let stmt = stmt.trim_start().to_uppercase();
    stmt.starts_with("COMMIT") || stmt.starts_with("END") || stmt.starts_with("ROLLBACK")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[trace]
    fn test_transaction_start(
        #[values(
            "BEGIN",
            "BEGIN DEFERRED",
            "BEGIN IMMEDIATE",
            "BEGIN EXCLUSIVE",
            "BEGIN TRANSACTION",
            "BEGIN DEFERRED TRANSACTION",
            "BEGIN IMMEDIATE TRANSACTION",
            "BEGIN EXCLUSIVE TRANSACTION",
            "begin"
        )]
        tx_start_stmt: &str,
    ) {
        assert!(is_transaction_start(tx_start_stmt));
        assert!(!is_transaction_end(tx_start_stmt));
    }

    #[rstest]
    #[trace]
    fn test_transaction_end(
        #[values(
           "COMMIT",
           "COMMIT TRANSACTION",
           "commit",
           "END",
           "END TRANSACTION",
           "end",
           "ROLLBACK",
           "ROLLBACK TRANSACTION",
           "rollback",
       // TODO: add back when we support SAVEPOINTS
       //       See https://www.sqlite.org/lang_savepoint.html
       //    "ROLLBACK TO savepoint_name",
       //    "ROLLBACK TRANSACTION TO savepoint_name",
       //    "ROLLBACK TO SAVEPOINT savepoint_name",
       //    "ROLLBACK TRANSACTION TO SAVEPOINT savepoint_name"
       )]
        tx_end_stmt: &str,
    ) {
        assert!(is_transaction_end(tx_end_stmt));
        assert!(!is_transaction_start(tx_end_stmt));
    }
}
