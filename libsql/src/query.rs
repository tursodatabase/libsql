use libsql_sys::hrana::{
    self,
    proto::{BatchCond, BatchStep, Stmt},
};

pub trait Query {
    fn into_step(self, batch: &mut hrana::proto::Batch);

    // Chain this query with another one. Next is only executed it this query is successful
    fn then<T: Query>(self, next: T) -> Then<Self, T>
    where
        Self: Sized,
    {
        Then {
            first: self,
            second: next,
        }
    }

    // Perform this query first, then another unconditionally.
    fn and<T: Query>(self, next: T) -> And<Self, T>
    where
        Self: Sized,
    {
        And {
            first: self,
            second: next,
        }
    }
}

pub struct And<L, R> {
    first: L,
    second: R,
}

impl<L, R> Query for And<L, R>
where
    L: Query,
    R: Query,
{
    fn into_step(self, batch: &mut hrana::proto::Batch) {
        self.first.into_step(batch);
        self.second.into_step(batch);
    }
}

pub struct Then<L, R> {
    first: L,
    second: R,
}

impl<L, R> Query for Then<L, R>
where
    L: Query,
    R: Query,
{
    fn into_step(self, batch: &mut hrana::proto::Batch) {
        self.first.into_step(batch);
        let next_step = batch.steps.len();
        self.second.into_step(batch);
        if batch.steps.len() > next_step {
            let cond = &mut batch.steps[next_step].condition;
            let prev_cond = cond.take();
            *cond = Some(BatchCond::And(hrana::proto::BatchCondList {
                conds: Some(BatchCond::Ok {
                    step: (next_step - 1) as _,
                })
                .into_iter()
                .chain(prev_cond.into_iter())
                .collect(),
            }));
        }
    }
}

pub struct Batch<I>(I);

impl<I, Q> Query for Batch<I>
where I: IntoIterator<Item = Q>,
      Q: Query,
{
    fn into_step(self, batch: &mut hrana::proto::Batch) {
        batch.steps.push(BatchStep {
            condition: None,
            stmt: Stmt {
                sql: Some("BEGIN".to_string()),
                sql_id: None,
                args: Vec::new(),
                named_args: Vec::new(),
                want_rows: None,
                replication_index: None,
            },
        });

        for stmt in self.0 {
            let next = batch.steps.len();
            stmt.into_step(batch);
            if batch.steps.len() > next {
                let cond = &mut batch.steps[next].condition;
                let prev_cond = cond.take();
                *cond = Some(BatchCond::And(hrana::proto::BatchCondList {
                    conds: Some(BatchCond::Ok {
                        step: (next - 1) as _,
                    })
                    .into_iter()
                    .chain(prev_cond.into_iter())
                    .collect(),
                }));
            }
        }

        let last = batch.steps.len();
        batch.steps.push(BatchStep {
            condition: Some(BatchCond::Error {
                step: (last - 1) as _,
            }),
            stmt: Stmt {
                sql: Some("ROLLBACK".to_string()),
                sql_id: None,
                args: Vec::new(),
                named_args: Vec::new(),
                want_rows: None,
                replication_index: None,
            },
        });
        // the `success` step is put last for other query to see
        batch.steps.push(BatchStep {
            condition: Some(BatchCond::Ok {
                step: (last - 1) as _,
            }),
            stmt: Stmt {
                sql: Some("COMMIT".to_string()),
                sql_id: None,
                args: Vec::new(),
                named_args: Vec::new(),
                want_rows: None,
                replication_index: None,
            },
        });
    }
}

impl Query for &str {
    fn into_step(self, batch: &mut hrana::proto::Batch) {
        batch.steps.push(BatchStep {
            condition: None,
            stmt: Stmt {
                sql: Some(self.to_string()),
                sql_id: None,
                args: Vec::new(),
                named_args: Vec::new(),
                want_rows: None,
                replication_index: None,
            },
        })
    }
}

#[cfg(test)]
mod test {
    use libsql_sys::rusqlite::Batch;

    use super::*;

    #[test]
    fn chain_stmts() {
        let mut batch = hrana::proto::Batch {
            steps: Vec::new(),
            replication_index: None,
        };

        "pragma foreign_key = true".then(super::Batch(["insert into test values (42)", "insert into test values (42)"])).into_step(&mut batch);

        dbg!(batch);
    }
}
