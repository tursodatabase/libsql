use crate::messages::{ErrorCode, Message};
use anyhow::Result;
use sqlite::Connection;
use std::cell::RefCell;

pub(crate) struct Coordinator {
    /// In-memory SQLite database.
    database: Connection,
    /// Current interactive transaction owner; if one exists.
    tx: RefCell<Option<String>>,
}

fn is_transaction_start(stmt: &str) -> bool {
    // TODO: Add support for Savepoints
    //       Savepoints are named transactions that can be nested.
    //       See https://www.sqlite.org/lang_savepoint.html
    stmt.trim_start().to_uppercase().starts_with("BEGIN")
}

fn is_transaction_end(stmt: &str) -> bool {
    let stmt = stmt.trim_start().to_uppercase();
    stmt.starts_with("COMMIT") || stmt.starts_with("END") || stmt.starts_with("ROLLBACK")
}

impl Coordinator {
    pub fn start() -> Result<Coordinator> {
        let database = sqlite::open(":memory:")?;
        let tx = RefCell::new(None);
        Ok(Coordinator { database, tx })
    }

    pub fn on_execute(&self, endpoint: String, stmt: String) -> Message {
        if let Some(tx_owner) = &*self.tx.borrow() {
            if *tx_owner != endpoint {
                return Message::Error(ErrorCode::TxBusy, "Transaction in progress.".to_string());
            }
        }
        println!("{} => {}", endpoint, stmt);
        if is_transaction_start(&stmt) {
            self.tx.replace(Some(endpoint));
        }
        let mut rows = vec![];
        let result = self.database.iterate(stmt.clone(), |pairs| {
            for &(name, value) in pairs.iter() {
                rows.push(format!("{} = {}", name, value.unwrap()));
            }
            true
        });
        if is_transaction_end(&stmt) {
            self.tx.replace(None);
        }
        match result {
            Ok(_) => Message::ResultSet(rows),
            Err(err) => Message::Error(ErrorCode::SQLError, format!("{:?}", err)),
        }
    }

    pub fn on_disconnect(&self, endpoint: String) -> Result<()> {
        let mut tx = self.tx.borrow_mut();
        if *tx == Some(endpoint) {
            self.database.execute("ROLLBACK")?;
            *tx = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[trace]
    fn test_concurrent_interactive_transaction_is_rejected(
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
        let coordinator = Coordinator::start().unwrap();
        // TODO: add back when we support SAVEPOINTS
        //       See https://www.sqlite.org/lang_savepoint.html
        //let response = coordinator.on_execute("Node 0".to_string(), "SAVEPOINT savepoint_name".to_string());
        //assert!(matches!(response, Message::ResultSet(_)));
        let response = coordinator.on_execute("Node 0".to_string(), tx_start_stmt.to_string());
        assert!(matches!(response, Message::ResultSet(_)));
        let response = coordinator.on_execute("Node 1".to_string(), tx_start_stmt.to_string());
        assert!(matches!(response, Message::Error(ErrorCode::TxBusy, _)));
        let response = coordinator.on_execute("Node 0".to_string(), tx_end_stmt.to_string());
        assert!(matches!(response, Message::ResultSet(_)));
        let response = coordinator.on_execute("Node 1".to_string(), tx_start_stmt.to_string());
        assert!(matches!(response, Message::ResultSet(_)));
    }

    #[test]
    fn test_disconnect_aborts_interactive_transaction() {
        let coordinator = Coordinator::start().unwrap();
        let response = coordinator.on_execute("Node 0".to_string(), "BEGIN".to_string());
        assert!(matches!(response, Message::ResultSet(_)));
        coordinator.on_disconnect("Node 0".to_string()).unwrap();
        let response = coordinator.on_execute("Node 1".to_string(), "BEGIN".to_string());
        assert!(matches!(response, Message::ResultSet(_)));
    }
}
