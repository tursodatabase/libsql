use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use hashbrown::HashMap;
use parking_lot::Mutex;

#[derive(Default, Clone, Debug)]
pub struct Timings {
    records: Arc<Mutex<HashMap<&'static str, Duration>>>,
}

impl Timings {
    pub fn record(&self, k: &'static str, d: Duration) {
        self.records.lock().insert(k, d);
    }

    fn format(&self) -> String {
        let mut out = String::new();
        let records = self.records.lock();
        for (k, v) in records.iter() {
            write!(&mut out, "{k};dur={v:?},").unwrap();
        }
        out
    }
}

tokio::task_local! {
    pub static TIMINGS: Timings;
}

#[macro_export]
macro_rules! record_time {
    ($k:literal; $($rest:tt)*) => {
        {
            let __before__ = std::time::Instant::now();
            let __ret__ = {
                $($rest)*
            };
            let __elapsed__ = __before__.elapsed();
            tracing::debug!(target: "timings", name = $k, elapsed = tracing::field::debug(__elapsed__));
            let _ = $crate::http::user::timing::TIMINGS.try_with(|t| t.record($k, __elapsed__));
            __ret__
        }
    };
}

pub fn sample_time(name: &'static str, duration: Duration) {
    tracing::debug!(target: "timings", name = name, elapsed = tracing::field::debug(duration));
    let _ = TIMINGS.try_with(|t| t.record(name, duration));
}

#[tracing::instrument(skip_all, fields(req_id = tracing::field::debug(uuid::Uuid::new_v4())))]
pub(crate) async fn timings_middleware<B>(request: Request<B>, next: Next<B>) -> Response {
    // tracing::error!("hello");
    TIMINGS
        .scope(Default::default(), async move {
            let mut response = record_time! {
                "query_total";
                next.run(request).await
            };
            let timings = TIMINGS.get().format();
            response
                .headers_mut()
                .insert("Server-Timing", timings.parse().unwrap());
            response
        })
        .await
}
