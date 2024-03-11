#![allow(dead_code)]
#![allow(clippy::mutable_key_type)]

use std::collections::HashMap;

use metrics::{SharedString, Unit};
use metrics_util::{
    debugging::{DebugValue, Snapshotter},
    CompositeKey, MetricKind,
};

pub mod auth;
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

    pub fn get_counter_label(&self, metric_name: &str, label: (&str, &str)) -> Option<u64> {
        for (key, (_, _, val)) in &self.snapshot {
            if key.kind() == MetricKind::Counter && key.key().name() == metric_name {
                if !key
                    .key()
                    .labels()
                    .any(|l| l.key() == label.0 && l.value() == label.1)
                {
                    continue;
                }

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

    pub fn snapshot(
        &self,
    ) -> &HashMap<CompositeKey, (Option<Unit>, Option<SharedString>, DebugValue)> {
        &self.snapshot
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

    #[track_caller]
    pub fn assert_counter_label(
        &self,
        metric_name: &str,
        label: (&str, &str),
        value: u64,
    ) -> &Self {
        let val = self
            .get_counter_label(metric_name, label)
            .expect("metric does not exist");

        assert_eq!(val, value);
        self
    }
}
