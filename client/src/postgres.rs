use anyhow::Result;
use bytes::BytesMut;
use postgres_protocol::message::{backend, frontend};
use std::collections::HashMap;
use std::io::prelude::*;
use std::net::TcpStream;
use url::Url;

pub struct Connection {
    stream: TcpStream,
    rx_buf: BytesMut,
    username: String,
}

impl Connection {
    pub fn connect(addr: &str) -> Result<Self> {
        let url = Url::parse(addr)?;
        let host = url.host_str().unwrap();
        let port = url.port().unwrap();
        let stream = TcpStream::connect((host, port))?;
        let rx_buf = BytesMut::with_capacity(1024);
        let username = url.username().into();
        Ok(Self {
            stream,
            rx_buf,
            username,
        })
    }

    pub fn send_startup(&mut self) -> Result<()> {
        let mut msg = BytesMut::new();
        let mut params = HashMap::new();
        params.insert("user", self.username.as_str());
        frontend::startup_message(params.into_iter(), &mut msg)?;
        self.stream.write_all(&msg)?;
        Ok(())
    }

    pub fn send_simple_query(&mut self, sql: &str) -> Result<()> {
        let mut msg = BytesMut::new();
        frontend::query(sql, &mut msg)?;
        self.stream.write_all(&msg)?;
        Ok(())
    }

    pub fn wait_until_ready(&mut self) -> Result<()> {
        loop {
            let msg = backend::Message::parse(&mut self.rx_buf)?;
            match msg {
                Some(msg) => {
                    if !self.process_msg(msg) {
                        return Ok(());
                    }
                }
                None => {
                    // FIXME: Optimize with spare_capacity_mut() to make zero-copy.
                    let mut buf = [0u8; 1024];
                    let nr = self.stream.read(&mut buf)?;
                    self.rx_buf.extend_from_slice(&buf[0..nr]);
                }
            }
        }
    }

    fn process_msg(&mut self, msg: backend::Message) -> bool {
        match msg {
            backend::Message::AuthenticationCleartextPassword => todo!(),
            backend::Message::AuthenticationGss => todo!(),
            backend::Message::AuthenticationKerberosV5 => todo!(),
            backend::Message::AuthenticationMd5Password(_) => todo!(),
            backend::Message::AuthenticationOk => {
                println!("TRACE postgres -> AuthenticationOk");
            }
            backend::Message::AuthenticationScmCredential => todo!(),
            backend::Message::AuthenticationSspi => todo!(),
            backend::Message::AuthenticationGssContinue(_) => todo!(),
            backend::Message::AuthenticationSasl(_) => todo!(),
            backend::Message::AuthenticationSaslContinue(_) => todo!(),
            backend::Message::AuthenticationSaslFinal(_) => todo!(),
            backend::Message::BackendKeyData(_) => {
                println!("TRACE postgres -> BackendKeyData");
            }
            backend::Message::BindComplete => todo!(),
            backend::Message::CloseComplete => todo!(),
            backend::Message::CommandComplete(_) => {
                println!("TRACE postgres -> CommandComplete");
            }
            backend::Message::CopyData(_) => todo!(),
            backend::Message::CopyDone => todo!(),
            backend::Message::CopyInResponse(_) => todo!(),
            backend::Message::CopyOutResponse(_) => todo!(),
            backend::Message::DataRow(_) => {
                println!("TRACE postgres -> DataRow");
            }
            backend::Message::EmptyQueryResponse => todo!(),
            backend::Message::ErrorResponse(_) => {
                println!("TRACE postgres -> ErrorResponse");
            }
            backend::Message::NoData => todo!(),
            backend::Message::NoticeResponse(_) => {
                println!("TRACE postgres -> NoticeResponse");
            }
            backend::Message::NotificationResponse(_) => todo!(),
            backend::Message::ParameterDescription(_) => todo!(),
            backend::Message::ParameterStatus(_) => {
                println!("TRACE postgres -> ParameterStatus");
            }
            backend::Message::ParseComplete => todo!(),
            backend::Message::PortalSuspended => todo!(),
            backend::Message::ReadyForQuery(_) => {
                println!("TRACE postgres -> ReadyForQuery");
                return false;
            }
            backend::Message::RowDescription(_) => {
                println!("TRACE postgres -> RowDescription");
            }
            _ => todo!(),
        }
        true
    }
}
