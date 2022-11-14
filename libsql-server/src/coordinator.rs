use crate::messages::{ErrorCode, Message};
use crate::sql_parser;
use crate::types::NodeId;
use anyhow::Result;
use sqlite::Connection;
use std::cell::RefCell;

pub(crate) struct Coordinator {
    /// In-memory SQLite database.
    database: Connection,
    /// Current interactive transaction owner; if one exists.
    tx: RefCell<Option<Transaction>>,
}

struct Transaction {
    owner: NodeId,
}

impl Transaction {
    fn new(endpoint: NodeId) -> Self {
        Self { owner: endpoint }
    }
}

impl Coordinator {
    pub fn start() -> Result<Coordinator> {
        let database = sqlite::open(":memory:")?;
        let tx = RefCell::new(None);
        Ok(Coordinator { database, tx })
    }

    pub fn on_execute(&self, endpoint: NodeId, stmt: String) -> Result<Message> {
        if let Some(tx) = &*self.tx.borrow() {
            if tx.owner != endpoint {
                return Ok(Message::Error(
                    ErrorCode::TxBusy,
                    "Transaction in progress.".to_string(),
                ));
            }
        }
        println!("{} => {}", endpoint, stmt);
        if sql_parser::is_transaction_start(&stmt) {
            self.tx.replace(Some(Transaction::new(endpoint)));
        }
        let mut rows = vec![];
        let result = self.database.iterate(stmt.clone(), |pairs| {
            for &(name, value) in pairs.iter() {
                rows.push(format!("{} = {}", name, value.unwrap()));
            }
            true
        });
        if sql_parser::is_transaction_end(&stmt) {
            self.tx.replace(None);
        }
        Ok(match result {
            Ok(_) => Message::ResultSet(rows),
            Err(err) => Message::Error(ErrorCode::SQLError, format!("{:?}", err)),
        })
    }

    pub fn on_disconnect(&self, endpoint: NodeId) -> Result<()> {
        let mut tx = self.tx.borrow_mut();
        if let Some(t) = &*tx {
            if t.owner == endpoint {
                self.database.execute("ROLLBACK")?;
                *tx = None;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_concurrent_interactive_transaction_is_rejected() {
        let coordinator = Coordinator::start().unwrap();
        // TODO: add back when we support SAVEPOINTS
        //       See https://www.sqlite.org/lang_savepoint.html
        //let response = coordinator.on_execute("Node 0".to_string(), "SAVEPOINT savepoint_name".to_string());
        //assert!(matches!(response, Message::ResultSet(_)));
        let tx_start_stmt = "BEGIN";
        let tx_end_stmt = "COMMIT";
        let response = coordinator
            .on_execute("Node 0".to_string(), tx_start_stmt.to_string())
            .unwrap();
        assert!(matches!(response, Message::ResultSet(_)));
        let response = coordinator
            .on_execute("Node 1".to_string(), tx_start_stmt.to_string())
            .unwrap();
        assert!(matches!(response, Message::Error(ErrorCode::TxBusy, _)));
        let response = coordinator
            .on_execute("Node 0".to_string(), tx_end_stmt.to_string())
            .unwrap();
        assert!(matches!(response, Message::ResultSet(_)));
        let response = coordinator
            .on_execute("Node 1".to_string(), tx_start_stmt.to_string())
            .unwrap();
        assert!(matches!(response, Message::ResultSet(_)));
    }

    #[test]
    fn test_disconnect_aborts_interactive_transaction() {
        let coordinator = Coordinator::start().unwrap();
        let response = coordinator
            .on_execute("Node 0".to_string(), "BEGIN".to_string())
            .unwrap();
        assert!(matches!(response, Message::ResultSet(_)));
        coordinator.on_disconnect("Node 0".to_string()).unwrap();
        let response = coordinator
            .on_execute("Node 1".to_string(), "BEGIN".to_string())
            .unwrap();
        assert!(matches!(response, Message::ResultSet(_)));
    }
}
