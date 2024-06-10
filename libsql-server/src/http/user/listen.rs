use std::{
    collections::HashMap,
    mem,
    pin::Pin,
    task::{Context, Poll},
};

use crate::broadcaster::{Action, BroadcastMsg, UpdateSubscription};
use crate::error::Error;
use crate::{
    auth::Authenticated,
    namespace::{NamespaceName, NamespaceStore},
};
use axum::http::Uri;
use axum::response::{IntoResponse, Response};
use axum::{body::BoxBody, extract::State as AxumState};
use axum_extra::{extract::Query, json_lines::JsonLines};
use futures::{Stream, StreamExt};
use hyper::HeaderMap;
use serde::{Deserialize, Serialize};

use super::db_factory::namespace_from_headers;
use super::AppState;

#[derive(Deserialize)]
pub struct ListenQuery {
    table: String,
    action: Vec<Action>,
}

pub(super) async fn handle_listen(
    auth: Authenticated,
    AxumState(state): AxumState<AppState>,
    headers: HeaderMap,
    uri: Uri,
    mut query: Query<ListenQuery>,
) -> crate::Result<Response> {
    let namespace = namespace_from_headers(
        &headers,
        state.disable_default_namespace,
        state.disable_namespaces,
    )?;

    if !auth.is_namespace_authorized(&namespace) {
        return Err(Error::NamespaceDoesntExist(namespace.to_string()));
    }

    if let Some(primary_url) = state.primary_url {
        return Ok(Response::builder()
            .status(307)
            .header("Location", primary_url + uri.path())
            .body(BoxBody::default())
            .unwrap());
    }

    // TODO: validate table
    let table = mem::take(&mut query.table);
    let actions = mem::take(&mut query.action);

    let stream =
        SubscriptionAggregator::new(state.namespaces.clone(), namespace, table, actions).await?;
    Ok(JsonLines::new(stream).into_response())
}

static LAGGED_MSG: &str = "some changes were lost";

type AggregatorState = HashMap<Action, u64>;

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum AggregatorEvent {
    Error(&'static str),
    #[serde(untagged)]
    Changes(AggregatorState),
}

type AggregatorResult = Result<AggregatorEvent, Error>;
type AggregatorPoll = Poll<Option<AggregatorResult>>;

struct SubscriptionAggregator {
    actions: Vec<Action>,
    subscription: UpdateSubscription,
    state: AggregatorState,
    store: NamespaceStore,
    namespace: NamespaceName,
    table: String,
    errored: bool,
}

impl SubscriptionAggregator {
    async fn new(
        store: NamespaceStore,
        namespace: NamespaceName,
        table: String,
        actions: Vec<Action>,
    ) -> crate::Result<Self> {
        let subscription = store
            .subscribe(namespace.clone(), table.clone())
            .await
            .unwrap();
        Ok(Self {
            actions,
            subscription,
            state: HashMap::new(),
            store,
            namespace,
            table,
            errored: false,
        })
    }
}

impl Drop for SubscriptionAggregator {
    fn drop(&mut self) {
        let namespace = mem::take(&mut self.namespace);
        let table = mem::take(&mut self.table);
        let store = self.store.clone();
        tokio::spawn(async move { _ = store.unsubscribe(namespace, table).await });
    }
}

impl Stream for SubscriptionAggregator {
    type Item = AggregatorResult;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(poll) = self.poll_inner(cx) {
                return poll;
            }
        }
    }
}

impl SubscriptionAggregator {
    fn poll_inner(self: &mut Self, cx: &mut Context) -> Option<AggregatorPoll> {
        match self.subscription.inner.poll_next_unpin(cx) {
            Poll::Pending => Some(Poll::Pending),
            Poll::Ready(value) => match value {
                None => Some(Poll::Ready(None)),
                Some(result) => match result {
                    Ok(item) => match item {
                        BroadcastMsg::Change { action, .. } => self.register(action),
                        BroadcastMsg::Rollback => self.clear(),
                        BroadcastMsg::Commit => self.flush(),
                    },
                    Err(_) => self.error(),
                },
            },
        }
    }

    fn flush(&mut self) -> Option<AggregatorPoll> {
        self.errored = false;
        if self.state.is_empty() {
            return None;
        }
        let changes = mem::take(&mut self.state);
        Some(Poll::Ready(Some(Ok(AggregatorEvent::Changes(changes)))))
    }

    fn error(&mut self) -> Option<AggregatorPoll> {
        let errored = self.errored;
        self.clear();
        self.errored = true;
        if errored {
            return None;
        }
        Some(Poll::Ready(Some(Ok(AggregatorEvent::Error(&LAGGED_MSG)))))
    }

    fn register(&mut self, action: Action) -> Option<AggregatorPoll> {
        if self.actions.is_empty() || self.actions.contains(&action) {
            let total = self.state.entry(action).or_insert(0);
            *total += 1;
        }
        None
    }

    fn clear(&mut self) -> Option<AggregatorPoll> {
        self.errored = false;
        self.state.clear();
        None
    }
}
