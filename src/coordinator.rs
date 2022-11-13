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
        if stmt == "BEGIN" {
            self.tx.replace(Some(endpoint));
        }
        let mut rows = vec![];
        let result = self.database.iterate(stmt.clone(), |pairs| {
            for &(name, value) in pairs.iter() {
                rows.push(format!("{} = {}", name, value.unwrap()));
            }
            true
        });
        if stmt == "COMMIT" || stmt == "ROLLBACK" {
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

    #[test]
    fn test_concurrent_interactive_transaction_is_rejected() {
        let coordinator = Coordinator::start().unwrap();
        let response = coordinator.on_execute("Node 0".to_string(), "BEGIN".to_string());
        assert!(matches!(response, Message::ResultSet(_)));
        let response = coordinator.on_execute("Node 1".to_string(), "BEGIN".to_string());
        assert!(matches!(response, Message::Error(ErrorCode::TxBusy, _)));
        let response = coordinator.on_execute("Node 0".to_string(), "COMMIT".to_string());
        assert!(matches!(response, Message::ResultSet(_)));
        let response = coordinator.on_execute("Node 1".to_string(), "BEGIN".to_string());
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
