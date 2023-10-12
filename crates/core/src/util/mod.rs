cfg_http! {
    pub mod box_clone_service;
    mod http;
    pub(crate) use self::http::{coerce_url_scheme, ConnectorService, Socket};
}
