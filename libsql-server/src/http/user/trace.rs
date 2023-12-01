use std::time::Duration;

use hyper::{http, HeaderMap, Request, Response};
use tonic::Status;
use tower_http::{
    classify::{
        ClassifiedResponse, ClassifyResponse, GrpcCode, GrpcErrorsAsFailures, GrpcFailureClass,
    },
    trace::{DefaultOnResponse, OnResponse},
};
use tracing::{Level, Span};

use crate::metrics::CLIENT_VERSION;

pub(crate) fn request<B>(req: &Request<B>, span: &Span) {
    let _s = span.enter();

    tracing::debug!(
        "got request: {} {} {:?}",
        req.method(),
        req.uri(),
        req.headers()
    );
    if let Some(v) = req.headers().get("x-libsql-client-version") {
        if let Ok(s) = v.to_str() {
            metrics::increment_counter!(CLIENT_VERSION, "version" => s.to_string());
        }
    }
}

pub(crate) fn response<B>(res: &Response<B>, latency: Duration, span: &Span) {
    let on_response = DefaultOnResponse::new()
        .level(Level::DEBUG)
        .latency_unit(tower_http::LatencyUnit::Micros);

    let _s = span.enter();

    on_response.on_response(res, latency, span);

    let is_grpc = res
        .headers()
        .get(http::header::CONTENT_TYPE)
        .map_or(false, |value| {
            value.as_bytes().starts_with("application/grpc".as_bytes())
        });

    if !is_grpc {
        let status = res.status();
        metrics::increment_counter!(
            "libsql_server_user_http_response",
            "protocol" => "http",
            "status" => status.as_str().to_string()
        );

        if status.is_server_error() {
            metrics::increment_counter!("libsql_server_user_http_fault", "protcol" => "http");
        }
    } else {
        let grpc = GrpcErrorsAsFailures::new().with_success(GrpcCode::FailedPrecondition);

        let code = match grpc.classify_response(res) {
            ClassifiedResponse::Ready(Ok(())) => "0".to_string(),
            ClassifiedResponse::Ready(Err(GrpcFailureClass::Code(code))) => {
                metrics::increment_counter!("libsql_server_user_http_fault", "protcol" => "grpc");
                code.to_string()
            }
            ClassifiedResponse::Ready(Err(GrpcFailureClass::Error(_))) => return,
            // TODO(lucio): We need to fix this as this is not correct, right now we
            // assume that if the grpc-status is not in the init header frame then it will
            // be sent in the trailers, in our use case most of our gRPC calls will Error
            // initially on response rather than in the end of the stream.
            //
            // The problem here is for some reason on_eos is not getting called allowing us
            // to inspect the trailer headers. So for now we will short cut and treat any
            // trailing grpc-status code to be treated as a success.
            ClassifiedResponse::RequiresEos(_) => "0".to_string(),
        };

        metrics::increment_counter!(
            "libsql_server_user_http_response",
            "protocol" => "grpc",
            "status" => code,
        );
    }
}

pub(crate) fn eos(trailers: Option<&HeaderMap>, _duration: Duration, _span: &Span) {
    if let Some(t) = trailers {
        if let Some(s) = Status::from_header_map(t) {
            let code = s.code().to_string();

            metrics::increment_counter!(
                "libsql_server_user_http_response",
                "protocol" => "grpc",
                "status" => code,
            );
        }
    }
}

pub(crate) fn failure<F>(_: F, _: Duration, _: &Span) {
    metrics::increment_counter!("libsql_server_user_http_fault", "protcol" => "http");
}
