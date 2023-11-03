#![allow(dead_code)]

use std::collections::HashMap;

use metrics::{SharedString, Unit};
use metrics_util::{
    debugging::{DebugValue, Snapshotter},
    CompositeKey, MetricKind,
};

pub mod http;
pub mod net;

pub fn print_metrics() {
    let snapshot = MetricsSnapshot::current();

    eprintln!("{:?}", snapshot);
}

#[track_caller]
pub fn snapshot_metrics() -> MetricsSnapshot {
    MetricsSnapshot::current()
}

#[derive(Debug)]
pub struct MetricsSnapshot {
    snapshot: HashMap<CompositeKey, (Option<Unit>, Option<SharedString>, DebugValue)>,
}

impl MetricsSnapshot {
    #[track_caller]
    pub fn current() -> Self {
        let snapshot = Snapshotter::current_thread_snapshot()
            .expect("No snapshot available")
            .into_hashmap();

        MetricsSnapshot { snapshot }
    }

    pub fn get_counter(&self, metric_name: &str) -> Option<u64> {
        for (key, (_, _, val)) in &self.snapshot {
            if key.kind() == MetricKind::Counter && key.key().name() == metric_name {
                match val {
                    DebugValue::Counter(v) => return Some(*v),
                    _ => unreachable!(),
                }
            }
        }

        None
    }

    pub fn get_gauge(&self, metric_name: &str) -> Option<f64> {
        for (key, (_, _, val)) in &self.snapshot {
            if key.kind() == MetricKind::Gauge && key.key().name() == metric_name {
                match val {
                    DebugValue::Gauge(v) => return Some(v.0),
                    _ => unreachable!(),
                }
            }
        }

        None
    }

    #[track_caller]
    pub fn assert_gauge(&self, metric_name: &str, value: f64) -> &Self {
        let val = self.get_gauge(metric_name).expect("metric does not exist");

        assert_eq!(val, value);
        self
    }

    #[track_caller]
    pub fn assert_counter(&self, metric_name: &str, value: u64) -> &Self {
        let val = self
            .get_counter(metric_name)
            .expect("metric does not exist");

        assert_eq!(val, value);
        self
    }
}
