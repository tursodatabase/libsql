use tonic::Status;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum AuthError {
    #[error("The `Authorization` HTTP header is required but was not specified")]
    HttpAuthHeaderMissing,
    #[error("The `Authorization` HTTP header has invalid value")]
    HttpAuthHeaderInvalid,
    #[error("The authentication scheme in the `Authorization` HTTP header is not supported")]
    HttpAuthHeaderUnsupportedScheme,
    #[error("The `Basic` HTTP authentication scheme is not allowed")]
    BasicNotAllowed,
    #[error("The `Basic` HTTP authentication credentials were rejected")]
    BasicRejected,
    #[error("Authentication is required but no JWT was specified")]
    JwtMissing,
    #[error("Authentication using a JWT is not allowed")]
    JwtNotAllowed,
    #[error("The JWT is invalid")]
    JwtInvalid,
    #[error("The JWT has expired")]
    JwtExpired,
    #[error("The JWT is immature (not valid yet)")]
    JwtImmature,
    #[error("Auth string does not conform to '<scheme> <token>' form")]
    AuthStringMalformed,
    #[error("Expected authorization header but none given")]
    AuthHeaderNotFound,
    #[error("Non-ASCII auth header")]
    AuthHeaderNonAscii,
    #[error("Authentication failed")]
    Other,
}

impl AuthError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::HttpAuthHeaderMissing => "AUTH_HTTP_HEADER_MISSING",
            Self::HttpAuthHeaderInvalid => "AUTH_HTTP_HEADER_INVALID",
            Self::HttpAuthHeaderUnsupportedScheme => "AUTH_HTTP_HEADER_UNSUPPORTED_SCHEME",
            Self::BasicNotAllowed => "AUTH_BASIC_NOT_ALLOWED",
            Self::BasicRejected => "AUTH_BASIC_REJECTED",
            Self::JwtMissing => "AUTH_JWT_MISSING",
            Self::JwtNotAllowed => "AUTH_JWT_NOT_ALLOWED",
            Self::JwtInvalid => "AUTH_JWT_INVALID",
            Self::JwtExpired => "AUTH_JWT_EXPIRED",
            Self::JwtImmature => "AUTH_JWT_IMMATURE",
            Self::AuthStringMalformed => "AUTH_HEADER_MALFORMED",
            Self::AuthHeaderNotFound => "AUTH_HEADER_NOT_FOUND",
            Self::AuthHeaderNonAscii => "AUTH_HEADER_MALFORMED",
            Self::Other => "AUTH_FAILED",
        }
    }
}

impl From<AuthError> for Status {
    fn from(e: AuthError) -> Self {
        Status::unauthenticated(format!("AuthError: {}", e))
    }
}
