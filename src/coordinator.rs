use crate::messages::{ErrorCode, Message};
use crate::sql_parser;
use anyhow::Result;
use sqlite::Connection;
use std::cell::RefCell;
use std::time::{Duration, Instant};

pub(crate) struct Coordinator<NodeId> {
    /// In-memory SQLite database.
    database: Connection,
    /// Current interactive transaction owner; if one exists.
    tx: RefCell<Option<Transaction<NodeId>>>,
    on_transaction_timeout: Box<dyn Fn(&NodeId) -> ()>,
}

struct Transaction<NodeId> {
    owner: NodeId,
    last_action_at: Instant,
}

impl<NodeId> Transaction<NodeId> {
    fn new(endpoint: NodeId) -> Self {
        Self {
            owner: endpoint,
            last_action_at: Instant::now(),
        }
    }
}

impl<NodeId: std::cmp::PartialEq + std::fmt::Display> Coordinator<NodeId> {
    pub fn start(
        on_transaction_timeout: Box<dyn Fn(&NodeId) -> ()>,
    ) -> Result<Coordinator<NodeId>> {
        let database = sqlite::open(":memory:")?;
        let tx = RefCell::new(None);
        Ok(Coordinator {
            database,
            tx,
            on_transaction_timeout,
        })
    }

    pub fn on_execute(&self, endpoint: NodeId, stmt: String) -> Result<Message> {
        {
            let mut tx = self.tx.borrow_mut();
            if let Some(t) = &mut *tx {
                if t.owner != endpoint {
                    if Instant::now() - t.last_action_at >= Duration::from_millis(1000) {
                        self.database.execute("ROLLBACK")?;
                        (self.on_transaction_timeout)(&t.owner);
                        *tx = None;
                    } else {
                        return Ok(Message::Error(
                            ErrorCode::TxBusy,
                            "Transaction in progress.".to_string(),
                        ));
                    }
                } else {
                    t.last_action_at = Instant::now();
                }
            }
        }
        println!("{} => {}", endpoint, stmt);
        if sql_parser::is_transaction_start(&stmt) {
            self.tx.replace(Some(Transaction::<NodeId>::new(endpoint)));
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
    use std::rc::Rc;
    use std::thread;

    #[test]
    fn test_concurrent_interactive_transaction_is_rejected() {
        let coordinator = Coordinator::start(Box::new(|_: &String| {})).unwrap();
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
    fn test_interactive_transaction_timeout() {
        let timeout_count = Rc::new(RefCell::new(0));
        let timeout_count_copy = timeout_count.clone();
        let coordinator = Coordinator::start(Box::new(move |_: &String| {
            timeout_count_copy.replace_with(|&mut c| c + 1);
        }))
        .unwrap();
        let tx_start_stmt = "BEGIN";
        let response = coordinator
            .on_execute("Node 0".to_string(), tx_start_stmt.to_string())
            .unwrap();
        assert!(matches!(response, Message::ResultSet(_)));
        thread::sleep(Duration::from_millis(1000));
        let response = coordinator
            .on_execute("Node 1".to_string(), tx_start_stmt.to_string())
            .unwrap();
        assert!(matches!(response, Message::ResultSet(_)));
        assert_eq!(*timeout_count.borrow(), 1);
    }

    #[test]
    fn test_disconnect_aborts_interactive_transaction() {
        let coordinator = Coordinator::start(Box::new(|_: &String| {})).unwrap();
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
