macro_rules! caution {
    ($msg:expr) => {
        tracing::error!(concat!("BOTTOMLESS CAUTION: ", $msg));
    };
    ($msg:expr, $($arg:tt)*) => {
        tracing::error!(concat!("BOTTOMLESS CAUTION: ", $msg), $($arg)*);
    };
}

pub (crate) use caution;
