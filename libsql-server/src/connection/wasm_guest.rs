struct Connection<T> {
    instance: wasmtime::Instance,
    store: wasmtime::Store,
    stats: Arc<Stats>,
    config_store: Arc<DatabaseConfigStore>,
    builder_config: QueryBuilderConfig,
    current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
    // must be dropped after the connection because the connection refers to it
    state: Arc<TxnState<T>>,
    // current txn slot if any
    slot: Option<Arc<TxnSlot<T>>>,
}

#[async_trait::async_trait]
impl<T> super::Connection for LibSqlConnection<T>
where
    T: Wal + Send + 'static,
{
    async fn execute_program<B: QueryResultBuilder>(
        &self,
        pgm: Program,
        auth: Authenticated,
        builder: B,
        _replication_index: Option<FrameNo>,
    ) -> Result<B> {
        PROGRAM_EXEC_COUNT.increment(1);

        check_program_auth(auth, &pgm)?;
        let conn = self.inner.clone();
        tokio::task::spawn_blocking(move || Connection::run(conn, pgm, builder))
            .await
            .unwrap()
    }

    async fn describe(
        &self,
        sql: String,
        auth: Authenticated,
        _replication_index: Option<FrameNo>,
    ) -> Result<crate::Result<DescribeResponse>> {
        DESCRIBE_COUNT.increment(1);
        check_describe_auth(auth)?;
        let conn = self.inner.clone();
        let res = tokio::task::spawn_blocking(move || conn.lock().describe(&sql))
            .await
            .unwrap();

        Ok(res)
    }

    async fn is_autocommit(&self) -> Result<bool> {
        Ok(self.inner.lock().is_autocommit())
    }

    async fn checkpoint(&self) -> Result<()> {
        let conn = self.inner.clone();
        tokio::task::spawn_blocking(move || conn.lock().checkpoint())
            .await
            .unwrap()?;
        Ok(())
    }

    async fn vacuum_if_needed(&self) -> Result<()> {
        let conn = self.inner.clone();
        tokio::task::spawn_blocking(move || conn.lock().vacuum_if_needed())
            .await
            .unwrap()?;
        Ok(())
    }

    fn diagnostics(&self) -> String {
        match self.inner.try_lock() {
            Some(conn) => match conn.slot {
                Some(ref slot) => format!("{slot:?}"),
                None => "<no-transaction>".to_string(),
            },
            None => "[BUG] connection busy".to_string(),
        }
    }
}

// FAT TODO: actually implement by calling to the wasm instance
// and getting results back
impl<W: Wal> Connection<W> {
    fn new<T: WalManager<Wal = W>>(
        path: &Path,
        extensions: Arc<[PathBuf]>,
        wal_manager: T,
        stats: Arc<Stats>,
        config_store: Arc<DatabaseConfigStore>,
        builder_config: QueryBuilderConfig,
        current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
        state: Arc<TxnState<W>>,
    ) -> Result<Self> {
        let conn =
            open_conn_active_checkpoint(path, wal_manager, None, builder_config.auto_checkpoint)?;

        // register the lock-stealing busy handler
        unsafe {
            let ptr = Arc::as_ptr(&state) as *mut _;
            rusqlite::ffi::sqlite3_busy_handler(conn.handle(), Some(busy_handler::<W>), ptr);
        }

        let this = Self {
            conn,
            stats,
            config_store,
            builder_config,
            current_frame_no_receiver,
            state,
            slot: None,
        };

        for ext in extensions.iter() {
            unsafe {
                let _guard = rusqlite::LoadExtensionGuard::new(&this.conn).unwrap();
                if let Err(e) = this.conn.load_extension(ext, None) {
                    tracing::error!("failed to load extension: {}", ext.display());
                    Err(e)?;
                }
                tracing::debug!("Loaded extension {}", ext.display());
            }
        }

        Ok(this)
    }

    fn run<B: QueryResultBuilder>(
        this: Arc<Mutex<Self>>,
        pgm: Program,
        mut builder: B,
    ) -> Result<B> {
        use rusqlite::TransactionState as Tx;

        let state = this.lock().state.clone();

        let mut results = Vec::with_capacity(pgm.steps.len());
        builder.init(&this.lock().builder_config)?;
        let mut previous_state = this
            .lock()
            .conn
            .transaction_state(Some(DatabaseName::Main))?;

        let mut has_timeout = false;
        for step in pgm.steps() {
            let mut lock = this.lock();

            if let Some(slot) = &lock.slot {
                if slot.is_stolen.load(Ordering::Relaxed) || Instant::now() > slot.expires_at() {
                    // we mark ourselves as stolen to notify any waiting lock thief.
                    slot.is_stolen.store(true, Ordering::Relaxed);
                    lock.rollback();
                    has_timeout = true;
                }
            }

            // once there was a timeout, invalidate all the program steps
            if has_timeout {
                lock.slot = None;
                builder.begin_step()?;
                builder.step_error(Error::LibSqlTxTimeout)?;
                builder.finish_step(0, None)?;
                continue;
            }

            let res = lock.execute_step(step, &results, &mut builder)?;

            let new_state = lock.conn.transaction_state(Some(DatabaseName::Main))?;
            match (previous_state, new_state) {
                // lock was upgraded, claim the slot
                (Tx::None | Tx::Read, Tx::Write) => {
                    let slot = Arc::new(TxnSlot {
                        conn: this.clone(),
                        created_at: Instant::now(),
                        is_stolen: AtomicBool::new(false),
                    });

                    lock.slot.replace(slot.clone());
                    state.slot.write().replace(slot);
                }
                // lock was downgraded, notify a waiter
                (Tx::Write, Tx::None | Tx::Read) => {
                    let old_slot = lock
                        .slot
                        .take()
                        .expect("there should be a slot right after downgrading a txn");
                    let mut maybe_state_slot = state.slot.write();
                    // We need to make sure that the state slot is our slot before removing it.
                    if let Some(ref state_slot) = *maybe_state_slot {
                        if Arc::ptr_eq(state_slot, &old_slot) {
                            maybe_state_slot.take();
                        }
                    }

                    drop(maybe_state_slot);

                    state.notify.notify_waiters();
                }
                // nothing to do
                (_, _) => (),
            }

            previous_state = new_state;

            results.push(res);
        }

        {
            let mut lock = this.lock();
            let is_autocommit = lock.conn.is_autocommit();
            builder.finish(
                *(lock.current_frame_no_receiver.borrow_and_update()),
                is_autocommit,
            )?;
        }

        Ok(builder)
    }

    fn execute_step(
        &mut self,
        step: &Step,
        results: &[bool],
        builder: &mut impl QueryResultBuilder,
    ) -> Result<bool> {
        builder.begin_step()?;

        let mut enabled = match step.cond.as_ref() {
            Some(cond) => match eval_cond(cond, results, self.is_autocommit()) {
                Ok(enabled) => enabled,
                Err(e) => {
                    builder.step_error(e).unwrap();
                    false
                }
            },
            None => true,
        };

        let (affected_row_count, last_insert_rowid) = if enabled {
            match self.execute_query(&step.query, builder) {
                // builder error interrupt the execution of query. we should exit immediately.
                Err(e @ Error::BuilderError(_)) => return Err(e),
                Err(mut e) => {
                    if let Error::RusqliteError(err) = e {
                        let extended_code =
                            unsafe { rusqlite::ffi::sqlite3_extended_errcode(self.conn.handle()) };

                        e = Error::RusqliteErrorExtended(err, extended_code as i32);
                    };

                    builder.step_error(e)?;
                    enabled = false;
                    (0, None)
                }
                Ok(x) => x,
            }
        } else {
            (0, None)
        };

        builder.finish_step(affected_row_count, last_insert_rowid)?;

        Ok(enabled)
    }

    fn execute_query(
        &self,
        query: &Query,
        builder: &mut impl QueryResultBuilder,
    ) -> Result<(u64, Option<i64>)> {
        tracing::trace!("executing query: {}", query.stmt.stmt);

        increment_counter!("libsql_server_libsql_query_execute");

        let start = Instant::now();
        let config = self.config_store.get();
        let blocked = match query.stmt.kind {
            StmtKind::Read | StmtKind::TxnBegin | StmtKind::Other => config.block_reads,
            StmtKind::Write => config.block_reads || config.block_writes,
            StmtKind::TxnEnd | StmtKind::Release | StmtKind::Savepoint => false,
        };
        if blocked {
            return Err(Error::Blocked(config.block_reason.clone()));
        }

        let mut stmt = self.conn.prepare(&query.stmt.stmt)?;
        if stmt.readonly() {
            READ_QUERY_COUNT.increment(1);
        } else {
            WRITE_QUERY_COUNT.increment(1);
        }

        let cols = stmt.columns();
        let cols_count = cols.len();
        builder.cols_description(cols.iter())?;
        drop(cols);

        query
            .params
            .bind(&mut stmt)
            .map_err(Error::LibSqlInvalidQueryParams)?;

        let mut qresult = stmt.raw_query();

        let mut values_total_bytes = 0;
        builder.begin_rows()?;
        while let Some(row) = qresult.next()? {
            builder.begin_row()?;
            for i in 0..cols_count {
                let val = row.get_ref(i)?;
                values_total_bytes += value_size(&val);
                builder.add_row_value(val)?;
            }
            builder.finish_row()?;
        }
        histogram!("libsql_server_returned_bytes", values_total_bytes as f64);

        builder.finish_rows()?;

        // sqlite3_changes() is only modified for INSERT, UPDATE or DELETE; it is not reset for SELECT,
        // but we want to return 0 in that case.
        let affected_row_count = match query.stmt.is_iud {
            true => self.conn.changes(),
            false => 0,
        };

        // sqlite3_last_insert_rowid() only makes sense for INSERTs into a rowid table. we can't detect
        // a rowid table, but at least we can detect an INSERT
        let last_insert_rowid = match query.stmt.is_insert {
            true => Some(self.conn.last_insert_rowid()),
            false => None,
        };

        drop(qresult);

        self.update_stats(query.stmt.stmt.clone(), &stmt, Instant::now() - start);

        Ok((affected_row_count, last_insert_rowid))
    }

    fn rollback(&self) {
        if let Err(e) = self.conn.execute("ROLLBACK", ()) {
            tracing::error!("failed to rollback: {e}");
        }
    }

    fn checkpoint(&self) -> Result<()> {
        let start = Instant::now();
        self.conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", (), |_| Ok(()))?;
        WAL_CHECKPOINT_COUNT.increment(1);
        histogram!("libsql_server_wal_checkpoint_time", start.elapsed());
        Ok(())
    }

    fn vacuum_if_needed(&self) -> Result<()> {
        let page_count = self
            .conn
            .query_row("PRAGMA page_count", (), |row| row.get::<_, i64>(0))?;
        let freelist_count = self
            .conn
            .query_row("PRAGMA freelist_count", (), |row| row.get::<_, i64>(0))?;
        // NOTICE: don't bother vacuuming if we don't have at least 256MiB of data
        if page_count >= 65536 && freelist_count * 2 > page_count {
            tracing::info!("Vacuuming: pages={page_count} freelist={freelist_count}");
            self.conn.execute("VACUUM", ())?;
        } else {
            tracing::debug!("Not vacuuming: pages={page_count} freelist={freelist_count}");
        }
        VACUUM_COUNT.increment(1);
        Ok(())
    }

    fn update_stats(&self, sql: String, stmt: &rusqlite::Statement, elapsed: Duration) {
        histogram!("libsql_server_statement_execution_time", elapsed);
        let elapsed = elapsed.as_millis() as u64;
        let rows_read = stmt.get_status(StatementStatus::RowsRead) as u64;
        let rows_written = stmt.get_status(StatementStatus::RowsWritten) as u64;
        let mem_used = stmt.get_status(StatementStatus::MemUsed) as u64;
        histogram!("libsql_server_statement_mem_used_bytes", mem_used as f64);
        let rows_read = if rows_read == 0 && rows_written == 0 {
            1
        } else {
            rows_read
        };
        self.stats.inc_rows_read(rows_read);
        self.stats.inc_rows_written(rows_written);
        let weight = rows_read + rows_written;
        if self.stats.qualifies_as_top_query(weight) {
            self.stats.add_top_query(crate::stats::TopQuery::new(
                sql.clone(),
                rows_read,
                rows_written,
            ));
        }
        if self.stats.qualifies_as_slowest_query(elapsed) {
            self.stats
                .add_slowest_query(crate::stats::SlowestQuery::new(
                    sql.clone(),
                    elapsed,
                    rows_read,
                    rows_written,
                ));
        }

        self.stats
            .update_query_metrics(rows_read, rows_written, mem_used, elapsed)
    }

    fn describe(&self, sql: &str) -> crate::Result<DescribeResponse> {
        let stmt = self.conn.prepare(sql)?;

        let params = (1..=stmt.parameter_count())
            .map(|param_i| {
                let name = stmt.parameter_name(param_i).map(|n| n.into());
                DescribeParam { name }
            })
            .collect();

        let cols = stmt
            .columns()
            .into_iter()
            .map(|col| {
                let name = col.name().into();
                let decltype = col.decl_type().map(|t| t.into());
                DescribeCol { name, decltype }
            })
            .collect();

        let is_explain = stmt.is_explain() != 0;
        let is_readonly = stmt.readonly();
        Ok(DescribeResponse {
            params,
            cols,
            is_explain,
            is_readonly,
        })
    }

    fn is_autocommit(&self) -> bool {
        self.conn.is_autocommit()
    }
}