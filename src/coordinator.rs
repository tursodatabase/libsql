use crate::messages::Message;
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
                return Message::Error("Transaction in progress.".to_string());
            }
        }
        println!("{} => {}", endpoint, stmt);
        let mut rows = vec![];
        let result = self.database.iterate(stmt.clone(), |pairs| {
            for &(name, value) in pairs.iter() {
                rows.push(format!("{} = {}", name, value.unwrap()));
            }
            true
        });
        if stmt == "COMMIT" {
            self.tx.replace(Some(endpoint));
        }
        if stmt == "ROLLBACK" || stmt == "COMMIT" {
            self.tx.replace(None);
        }
        match result {
            Ok(_) => Message::ResultSet(rows),
            Err(err) => Message::Error(format!("{:?}", err)),
        }
    }
}
