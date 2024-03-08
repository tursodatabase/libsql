use std::fmt;
use std::io::{self, ErrorKind};
use std::ops::{Deref, DerefMut};

use bytesize::ByteSize;
use libsql_sys::EncryptionConfig;
use rusqlite::types::ValueRef;
use serde::Serialize;
use serde_json::ser::Formatter;
use std::sync::atomic::AtomicUsize;

use crate::replication::FrameNo;

pub static TOTAL_RESPONSE_SIZE: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
pub enum QueryResultBuilderError {
    /// The response payload is too large
    ResponseTooLarge(u64),
    Internal(anyhow::Error),
}

impl fmt::Display for QueryResultBuilderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryResultBuilderError::ResponseTooLarge(s) => {
                write!(f, "query response exceeds the maximum size of {}. Try reducing the number of queried rows.", ByteSize(*s))
            }
            QueryResultBuilderError::Internal(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for QueryResultBuilderError {}

impl From<anyhow::Error> for QueryResultBuilderError {
    fn from(value: anyhow::Error) -> Self {
        Self::Internal(value)
    }
}

impl QueryResultBuilderError {
    pub fn from_any<E: Into<anyhow::Error>>(e: E) -> Self {
        Self::Internal(e.into())
    }
}

impl From<io::Error> for QueryResultBuilderError {
    fn from(value: io::Error) -> Self {
        if value.kind() == ErrorKind::OutOfMemory
            && value.get_ref().is_some()
            && value.get_ref().unwrap().is::<QueryResultBuilderError>()
        {
            return *value
                .into_inner()
                .unwrap()
                .downcast::<QueryResultBuilderError>()
                .unwrap();
        }
        Self::Internal(value.into())
    }
}

/// Identical to rusqlite::Column, with visible fields.
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub struct Column<'a> {
    pub(crate) name: &'a str,
    pub(crate) decl_ty: Option<&'a str>,
}

impl<'a> From<(&'a str, Option<&'a str>)> for Column<'a> {
    fn from((name, decl_ty): (&'a str, Option<&'a str>)) -> Self {
        Self { name, decl_ty }
    }
}

impl<'a> From<&'a rusqlite::Column<'a>> for Column<'a> {
    fn from(value: &'a rusqlite::Column<'a>) -> Self {
        Self {
            name: value.name(),
            decl_ty: value.decl_type(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueryBuilderConfig {
    pub max_size: Option<u64>,
    pub max_total_size: Option<u64>,
    // FIXME: this has absolutely nothing to do here.
    pub auto_checkpoint: u32,
    pub encryption_config: Option<EncryptionConfig>,
}

pub trait QueryResultBuilder: Send + 'static {
    type Ret: Sized + Send + 'static;

    /// (Re)initialize the builder. This method can be called multiple times.
    fn init(&mut self, config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError>;
    /// start serializing new step
    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError>;
    /// finish serializing current step
    fn finish_step(
        &mut self,
        affected_row_count: u64,
        last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError>;
    /// emit an error to serialize.
    fn step_error(&mut self, error: crate::error::Error) -> Result<(), QueryResultBuilderError>;
    /// add cols description for current step.
    /// This is called called at most once per step, and is always the first method being called
    fn cols_description<'a>(
        &mut self,
        cols: impl IntoIterator<Item = impl Into<Column<'a>>>,
    ) -> Result<(), QueryResultBuilderError>;
    /// start adding rows
    fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError>;
    /// begin a new row for the current step
    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError>;
    /// add value to current row
    fn add_row_value(&mut self, v: ValueRef) -> Result<(), QueryResultBuilderError>;
    /// finish current row
    fn finish_row(&mut self) -> Result<(), QueryResultBuilderError>;
    /// end adding rows
    fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError>;
    /// finish serialization.
    fn finish(
        &mut self,
        last_frame_no: Option<FrameNo>,
        _is_auto_commit: bool,
    ) -> Result<(), QueryResultBuilderError>;
    /// returns the inner ret
    fn into_ret(self) -> Self::Ret;
    /// Returns a `QueryResultBuilder` that wraps Self and takes at most `n` steps
    fn take(self, limit: usize) -> Take<Self>
    where
        Self: Sized,
    {
        Take {
            limit,
            count: 0,
            inner: self,
        }
    }
}

pub struct JsonFormatter<F>(pub F);

impl<F: Formatter> JsonFormatter<F> {
    pub fn serialize_key_value<V, W>(
        &mut self,
        mut w: W,
        k: &str,
        v: &V,
        first: bool,
    ) -> anyhow::Result<()>
    where
        V: Serialize + Sized,
        F: Formatter,
        W: io::Write,
    {
        self.serialize_key(&mut w, k, first)?;
        self.serialize_value(&mut w, v)?;

        Ok(())
    }

    pub fn serialize_key<W>(&mut self, mut w: W, key: &str, first: bool) -> anyhow::Result<()>
    where
        F: Formatter,
        W: io::Write,
    {
        self.0.begin_object_key(&mut w, first)?;
        serde_json::to_writer(&mut w, key)?;
        self.0.end_object_key(&mut w)?;
        Ok(())
    }

    fn serialize_value<V, W>(&mut self, mut w: W, v: &V) -> anyhow::Result<()>
    where
        V: Serialize,
        F: Formatter,
        W: io::Write,
    {
        self.0.begin_object_value(&mut w)?;
        serde_json::to_writer(&mut w, v)?;
        self.0.end_object_value(&mut w)?;

        Ok(())
    }

    pub fn serialize_array_iter<V, W>(
        &mut self,
        mut w: W,
        iter: impl Iterator<Item = V>,
    ) -> anyhow::Result<()>
    where
        W: io::Write,
        V: Serialize,
    {
        self.0.begin_array(&mut w)?;
        let mut first = true;
        for item in iter {
            self.serialize_array_value(&mut w, &item, first)?;
            first = false;
        }
        self.0.end_array(&mut w)?;

        Ok(())
    }

    pub fn serialize_array_value<V, W>(
        &mut self,
        mut w: W,
        v: &V,
        first: bool,
    ) -> anyhow::Result<()>
    where
        V: Serialize + Sized,
        F: Formatter,
        W: io::Write,
    {
        self.0.begin_array_value(&mut w, first)?;
        serde_json::to_writer(&mut w, v)?;
        self.0.end_array_value(&mut w)?;
        Ok(())
    }
}

impl<F> Deref for JsonFormatter<F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<F> DerefMut for JsonFormatter<F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
pub enum StepResult {
    Ok,
    Err(crate::error::Error),
    Skipped,
}
/// A `QueryResultBuilder` that ignores rows, but records the outcome of each step in a `StepResult`
#[derive(Debug, Default)]
pub struct StepResultsBuilder {
    current: Option<crate::error::Error>,
    step_results: Vec<StepResult>,
    is_skipped: bool,
}

impl QueryResultBuilder for StepResultsBuilder {
    type Ret = Vec<StepResult>;

    fn init(&mut self, _config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
        *self = Default::default();
        Ok(())
    }

    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        self.is_skipped = true;
        Ok(())
    }

    fn finish_step(
        &mut self,
        _affected_row_count: u64,
        _last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        let res = match self.current.take() {
            Some(e) => StepResult::Err(e),
            None if self.is_skipped => StepResult::Skipped,
            None => StepResult::Ok,
        };

        self.step_results.push(res);

        Ok(())
    }

    fn step_error(&mut self, error: crate::error::Error) -> Result<(), QueryResultBuilderError> {
        assert!(self.current.is_none());
        self.current = Some(error);

        Ok(())
    }

    fn cols_description<'a>(
        &mut self,
        _cols: impl IntoIterator<Item = impl Into<Column<'a>>>,
    ) -> Result<(), QueryResultBuilderError> {
        self.is_skipped = false;
        Ok(())
    }

    fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn add_row_value(&mut self, _v: ValueRef) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn finish(
        &mut self,
        _last_frame_no: Option<FrameNo>,
        _is_autocommit: bool,
    ) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn into_ret(self) -> Self::Ret {
        self.step_results
    }
}

pub struct IgnoreResult;

impl QueryResultBuilder for IgnoreResult {
    type Ret = ();

    fn init(&mut self, _config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn finish_step(
        &mut self,
        _affected_row_count: u64,
        _last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn step_error(&mut self, _error: crate::error::Error) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn cols_description<'a>(
        &mut self,
        _cols: impl IntoIterator<Item = impl Into<Column<'a>>>,
    ) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn add_row_value(&mut self, _v: ValueRef) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn finish(
        &mut self,
        _last_frame_no: Option<FrameNo>,
        _is_autocommit: bool,
    ) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn into_ret(self) -> Self::Ret {}
}

// A builder that wraps another builder, but takes at most `n` steps
pub struct Take<B> {
    limit: usize,
    count: usize,
    inner: B,
}

impl<B> Take<B> {
    pub fn into_inner(self) -> B {
        self.inner
    }
}

impl<B: QueryResultBuilder> QueryResultBuilder for Take<B> {
    type Ret = B::Ret;

    fn init(&mut self, config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
        self.count = 0;
        self.inner.init(config)
    }

    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        if self.count < self.limit {
            self.inner.begin_step()
        } else {
            Ok(())
        }
    }

    fn finish_step(
        &mut self,
        affected_row_count: u64,
        last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        if self.count < self.limit {
            self.inner
                .finish_step(affected_row_count, last_insert_rowid)?;
            self.count += 1;
        }

        Ok(())
    }

    fn step_error(&mut self, error: crate::error::Error) -> Result<(), QueryResultBuilderError> {
        if self.count < self.limit {
            self.inner.step_error(error)
        } else {
            Ok(())
        }
    }

    fn cols_description<'a>(
        &mut self,
        cols: impl IntoIterator<Item = impl Into<Column<'a>>>,
    ) -> Result<(), QueryResultBuilderError> {
        if self.count < self.limit {
            self.inner.cols_description(cols)
        } else {
            Ok(())
        }
    }

    fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        if self.count < self.limit {
            self.inner.begin_rows()
        } else {
            Ok(())
        }
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        if self.count < self.limit {
            self.inner.begin_row()
        } else {
            Ok(())
        }
    }

    fn add_row_value(&mut self, v: ValueRef) -> Result<(), QueryResultBuilderError> {
        if self.count < self.limit {
            self.inner.add_row_value(v)
        } else {
            Ok(())
        }
    }

    fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
        if self.count < self.limit {
            self.inner.finish_row()
        } else {
            Ok(())
        }
    }

    fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        if self.count < self.limit {
            self.inner.finish_rows()
        } else {
            Ok(())
        }
    }

    fn finish(
        &mut self,
        last_frame_no: Option<FrameNo>,
        is_autocommit: bool,
    ) -> Result<(), QueryResultBuilderError> {
        self.inner.finish(last_frame_no, is_autocommit)
    }

    fn into_ret(self) -> Self::Ret {
        self.inner.into_ret()
    }
}

#[cfg(test)]
pub mod test {
    use std::fmt;

    use arbitrary::{Arbitrary, Unstructured};
    use itertools::Itertools;
    use rand::{
        distributions::{Standard, WeightedIndex},
        prelude::Distribution,
        thread_rng, Fill, Rng,
    };
    use FsmState::*;

    use crate::query::Value;

    use super::*;

    #[derive(Default)]
    pub struct TestBuilder {
        steps: Vec<StepResult>,
        current_step: StepResultBuilder,
    }

    pub type Row = Vec<Value>;
    pub type StepResult = crate::Result<Vec<Row>>;

    #[derive(Default)]
    pub struct StepResultBuilder {
        rows: Vec<Row>,
        current_row: Row,
        err: Option<crate::Error>,
    }

    impl QueryResultBuilder for TestBuilder {
        type Ret = Vec<StepResult>;

        fn init(&mut self, _config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
            self.steps.clear();
            self.current_step = Default::default();
            Ok(())
        }

        fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
            Ok(())
        }

        fn finish_step(
            &mut self,
            _affected_row_count: u64,
            _last_insert_rowid: Option<i64>,
        ) -> Result<(), QueryResultBuilderError> {
            let current = std::mem::take(&mut self.current_step);
            if let Some(err) = current.err {
                self.steps.push(Err(err));
            } else {
                self.steps.push(Ok(current.rows));
            }

            Ok(())
        }

        fn step_error(
            &mut self,
            error: crate::error::Error,
        ) -> Result<(), QueryResultBuilderError> {
            self.current_step.err = Some(error);
            Ok(())
        }

        fn cols_description<'a>(
            &mut self,
            _cols: impl IntoIterator<Item = impl Into<Column<'a>>>,
        ) -> Result<(), QueryResultBuilderError> {
            Ok(())
        }

        fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
            Ok(())
        }

        fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
            Ok(())
        }

        fn add_row_value(&mut self, v: ValueRef) -> Result<(), QueryResultBuilderError> {
            let v = match v {
                ValueRef::Null => Value::Null,
                ValueRef::Integer(i) => Value::Integer(i),
                ValueRef::Real(x) => Value::Real(x),
                ValueRef::Text(s) => Value::Text(String::from_utf8(s.to_vec()).unwrap()),
                ValueRef::Blob(x) => Value::Blob(x.to_vec()),
            };
            self.current_step.current_row.push(v);
            Ok(())
        }

        fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
            let row = std::mem::take(&mut self.current_step.current_row);
            self.current_step.rows.push(row);
            Ok(())
        }

        fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
            Ok(())
        }

        fn finish(
            &mut self,
            _last_frame_no: Option<FrameNo>,
            _is_autocommitk: bool,
        ) -> Result<(), QueryResultBuilderError> {
            Ok(())
        }

        fn into_ret(self) -> Self::Ret {
            self.steps
        }
    }

    /// a dummy QueryResultBuilder that encodes the QueryResultBuilder FSM. It can be passed to a
    /// driver to ensure that it is not mis-used

    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    #[repr(usize)]
    // do not reorder!
    pub enum FsmState {
        Init = 0,
        Finish,
        BeginStep,
        FinishStep,
        StepError,
        ColsDescription,
        FinishRows,
        BeginRows,
        FinishRow,
        BeginRow,
        AddRowValue,
        BuilderError,
    }

    #[rustfmt::skip]
    static TRANSITION_TABLE: [[bool; 12]; 12] = [
      //FROM:
      //Init    Finish  BeginStep FinishStep StepError ColsDes FinishRows BegRows FinishRow  BegRow AddRowVal BuidlerErr TO:
        [true , true ,  true ,    true ,     true ,    true ,  true ,     true ,  true ,     true , true ,    false], // Init,
        [true , false,  false,    true ,     false,    false,  false,     false,  false,     false, false,    false], // Finish,
        [true , false,  false,    true ,     false,    false,  false,     false,  false,     false, false,    false], // BeginStep
        [false, false,  true ,    false,     true ,    false,  true ,     false,  false,     false, false,    false], // FinishStep
        [false, false,  true ,    false,     false,    true ,  true ,     true ,  true ,     true , true ,    false], // StepError
        [false, false,  true ,    false,     false,    false,  false,     false,  false,     false, false,    false], // ColsDescr
        [false, false,  false,    false,     false,    false,  false,     true ,  true ,     false, false,    false], // FinishRows
        [false, false,  false,    false,     false,    true ,  false,     false,  false,     false, false,    false], // BeginRows
        [false, false,  false,    false,     false,    false,  false,     false,  false,     true , true ,    false], // FinishRow
        [false, false,  false,    false,     false,    false,  false,     true ,  true ,     false, false,    false], // BeginRow,
        [false, false,  false,    false,     false,    false,  false,     false,  false,     true , true ,    false], // AddRowValue
        [true , true ,  true ,    true ,     true ,    true ,  true ,     true ,  true ,     true , true ,    false], // BuilderError
    ];

    impl FsmState {
        /// returns a random valid transition from the current state
        fn rand_transition(self, allow_init: bool) -> Self {
            let valid_next_states = TRANSITION_TABLE[..TRANSITION_TABLE.len() - 1] // ignore
                // builder error
                .iter()
                .enumerate()
                .skip(if allow_init { 0 } else { 1 })
                .filter_map(|(i, ss)| ss[self as usize].then_some(i))
                .collect_vec();
            // distribution is somewhat tweaked to be biased towards more real-world test cases
            let weigths = valid_next_states
                .iter()
                .enumerate()
                .map(|(p, i)| i.pow(p as _))
                .collect_vec();
            let dist = WeightedIndex::new(weigths).unwrap();
            unsafe { std::mem::transmute(valid_next_states[dist.sample(&mut thread_rng())]) }
        }

        /// moves towards the finish step as fast as possible
        fn toward_finish(self) -> Self {
            match self {
                Init => Finish,
                BeginStep => FinishStep,
                FinishStep => Finish,
                StepError => FinishStep,
                BeginRows | BeginRow | AddRowValue | FinishRow | FinishRows | ColsDescription => {
                    StepError
                }
                Finish => Finish,
                BuilderError => Finish,
            }
        }
    }

    pub fn random_transition(mut max_steps: usize) -> Vec<FsmState> {
        let mut trace = Vec::with_capacity(max_steps);
        let mut state = Init;
        trace.push(state);
        loop {
            if max_steps > 0 {
                state = state.rand_transition(false);
            } else {
                state = state.toward_finish()
            }

            trace.push(state);
            if state == FsmState::Finish {
                break;
            }

            max_steps = max_steps.saturating_sub(1);
        }
        trace
    }

    pub fn fsm_builder_driver<B: QueryResultBuilder>(trace: &[FsmState], mut b: B) -> B {
        let mut rand_data = [0; 10_000];
        rand_data.try_fill(&mut rand::thread_rng()).unwrap();
        let mut u = Unstructured::new(&rand_data);

        #[derive(Arbitrary)]
        pub enum ValueRef<'a> {
            Null,
            Integer(i64),
            Real(f64),
            Text(&'a str),
            Blob(&'a [u8]),
        }

        impl<'a> From<ValueRef<'a>> for rusqlite::types::ValueRef<'a> {
            fn from(value: ValueRef<'a>) -> Self {
                match value {
                    ValueRef::Null => rusqlite::types::ValueRef::Null,
                    ValueRef::Integer(i) => rusqlite::types::ValueRef::Integer(i),
                    ValueRef::Real(x) => rusqlite::types::ValueRef::Real(x),
                    ValueRef::Text(s) => rusqlite::types::ValueRef::Text(s.as_bytes()),
                    ValueRef::Blob(b) => rusqlite::types::ValueRef::Blob(b),
                }
            }
        }

        for state in trace {
            match state {
                Init => b.init(&QueryBuilderConfig::default()).unwrap(),
                BeginStep => b.begin_step().unwrap(),
                FinishStep => b
                    .finish_step(
                        Arbitrary::arbitrary(&mut u).unwrap(),
                        Arbitrary::arbitrary(&mut u).unwrap(),
                    )
                    .unwrap(),
                StepError => b.step_error(crate::Error::LibSqlTxBusy).unwrap(),
                ColsDescription => b
                    .cols_description(<Vec<Column>>::arbitrary(&mut u).unwrap())
                    .unwrap(),
                BeginRows => b.begin_rows().unwrap(),
                BeginRow => b.begin_row().unwrap(),
                AddRowValue => b
                    .add_row_value(ValueRef::arbitrary(&mut u).unwrap().into())
                    .unwrap(),
                FinishRow => b.finish_row().unwrap(),
                FinishRows => b.finish_rows().unwrap(),
                Finish => {
                    b.finish(Some(0), true).unwrap();
                    break;
                }
                BuilderError => return b,
            }
        }

        b
    }

    /// A Builder that validates a given execution trace
    pub struct ValidateTraceBuilder {
        trace: Vec<FsmState>,
        current: usize,
    }

    impl ValidateTraceBuilder {
        pub fn new(trace: Vec<FsmState>) -> Self {
            Self { trace, current: 0 }
        }
    }

    impl QueryResultBuilder for ValidateTraceBuilder {
        type Ret = ();

        fn init(&mut self, _config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
            assert_eq!(self.trace[self.current], FsmState::Init);
            self.current += 1;
            Ok(())
        }

        fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
            assert_eq!(self.trace[self.current], FsmState::BeginStep);
            self.current += 1;
            Ok(())
        }

        fn finish_step(
            &mut self,
            _affected_row_count: u64,
            _last_insert_rowid: Option<i64>,
        ) -> Result<(), QueryResultBuilderError> {
            assert_eq!(self.trace[self.current], FsmState::FinishStep);
            self.current += 1;
            Ok(())
        }

        fn step_error(
            &mut self,
            _error: crate::error::Error,
        ) -> Result<(), QueryResultBuilderError> {
            assert_eq!(self.trace[self.current], FsmState::StepError);
            self.current += 1;
            Ok(())
        }

        fn cols_description<'a>(
            &mut self,
            _cols: impl IntoIterator<Item = impl Into<Column<'a>>>,
        ) -> Result<(), QueryResultBuilderError> {
            assert_eq!(self.trace[self.current], FsmState::ColsDescription);
            self.current += 1;
            Ok(())
        }

        fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
            assert_eq!(self.trace[self.current], FsmState::BeginRows);
            self.current += 1;
            Ok(())
        }

        fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
            assert_eq!(self.trace[self.current], FsmState::BeginRow);
            self.current += 1;
            Ok(())
        }

        fn add_row_value(&mut self, _v: ValueRef) -> Result<(), QueryResultBuilderError> {
            assert_eq!(self.trace[self.current], FsmState::AddRowValue);
            self.current += 1;
            Ok(())
        }

        fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
            assert_eq!(self.trace[self.current], FsmState::FinishRow);
            self.current += 1;
            Ok(())
        }

        fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
            assert_eq!(self.trace[self.current], FsmState::FinishRows);
            self.current += 1;
            Ok(())
        }

        fn finish(
            &mut self,
            _last_frame_no: Option<FrameNo>,
            _is_autocommitk: bool,
        ) -> Result<(), QueryResultBuilderError> {
            assert_eq!(self.trace[self.current], FsmState::Finish);
            self.current += 1;
            Ok(())
        }

        fn into_ret(self) -> Self::Ret {
            assert_eq!(self.current, self.trace.len());
        }
    }

    pub struct FsmQueryBuilder {
        state: FsmState,
        inject_errors: bool,
    }

    impl fmt::Display for FsmState {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let s = match self {
                Init => "init",
                BeginStep => "begin_step",
                FinishStep => "finish_step",
                StepError => "step_error",
                ColsDescription => "cols_description",
                BeginRows => "begin_rows",
                BeginRow => "begin_row",
                AddRowValue => "add_row_value",
                FinishRow => "finish_row",
                FinishRows => "finish_rows",
                Finish => "finish",
                BuilderError => "a builder error",
            };

            f.write_str(s)
        }
    }

    impl FsmQueryBuilder {
        fn new(inject_errors: bool) -> Self {
            Self {
                state: Init,
                inject_errors,
            }
        }

        fn transition(&mut self, to: FsmState) -> Result<(), QueryResultBuilderError> {
            let from = self.state as usize;
            if TRANSITION_TABLE[to as usize][from] {
                self.state = to;
            } else {
                panic!("{} can't be called after {}", to, self.state);
            }

            Ok(())
        }

        fn maybe_inject_error(&mut self) -> Result<(), QueryResultBuilderError> {
            if self.inject_errors {
                let val: f32 = thread_rng().sample(Standard);
                // < 0.1% change to generate error
                if val < 0.001 {
                    self.state = BuilderError;
                    Err(anyhow::anyhow!("dummy"))?;
                }
            }

            Ok(())
        }
    }

    impl QueryResultBuilder for FsmQueryBuilder {
        type Ret = ();

        fn init(&mut self, _config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(Init)
        }

        fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(BeginStep)
        }

        fn finish_step(
            &mut self,
            _affected_row_count: u64,
            _last_insert_rowid: Option<i64>,
        ) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(FinishStep)
        }

        fn step_error(
            &mut self,
            _error: crate::error::Error,
        ) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(StepError)
        }

        fn cols_description<'a>(
            &mut self,
            _cols: impl IntoIterator<Item = impl Into<Column<'a>>>,
        ) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(ColsDescription)
        }

        fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(BeginRows)
        }

        fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(BeginRow)
        }

        fn add_row_value(&mut self, _v: ValueRef) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(AddRowValue)
        }

        fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(FinishRow)
        }

        fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(FinishRows)
        }

        fn finish(
            &mut self,
            _last_frame_no: Option<FrameNo>,
            _is_autocommitk: bool,
        ) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(Finish)
        }

        fn into_ret(self) -> Self::Ret {}
    }

    pub fn test_driver(iter: usize, f: impl Fn(FsmQueryBuilder) -> crate::Result<FsmQueryBuilder>) {
        for _ in 0..iter {
            // inject random errors
            let builder = FsmQueryBuilder::new(true);
            match f(builder) {
                Ok(b) => {
                    assert_eq!(b.state, Finish);
                }
                Err(e) => {
                    assert!(matches!(e, crate::Error::BuilderError(_)));
                }
            }
        }
    }

    #[test]
    fn test_fsm_ok() {
        let mut builder = FsmQueryBuilder::new(false);
        builder.init(&QueryBuilderConfig::default()).unwrap();

        builder.begin_step().unwrap();
        builder.cols_description([("hello", None)]).unwrap();
        builder.begin_rows().unwrap();
        builder.begin_row().unwrap();
        builder.add_row_value(ValueRef::Null).unwrap();
        builder.finish_row().unwrap();
        builder
            .step_error(crate::error::Error::LibSqlTxBusy)
            .unwrap();
        builder.finish_step(0, None).unwrap();

        builder.begin_step().unwrap();
        builder.cols_description([("hello", None)]).unwrap();
        builder.begin_rows().unwrap();
        builder.begin_row().unwrap();
        builder.add_row_value(ValueRef::Null).unwrap();
        builder.finish_row().unwrap();
        builder.finish_rows().unwrap();
        builder.finish_step(0, None).unwrap();

        builder.finish(Some(0), true).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_fsm_invalid() {
        let mut builder = FsmQueryBuilder::new(false);
        builder.init(&QueryBuilderConfig::default()).unwrap();
        builder.begin_step().unwrap();
        builder.begin_rows().unwrap();
    }
}
