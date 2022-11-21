use std::io::Write;

use crate::messages::Message;
use crate::net::Connection;

use anyhow::Result;
use futures::pin_mut;
use rustyline_async::{Readline, ReadlineError};
use tokio::net::ToSocketAddrs;

pub async fn start(addr: impl ToSocketAddrs) -> Result<()> {
    let mut client = Connection::connect(addr).await?;
    let (mut rl, stdout) = Readline::new(">>".into())?;

    let message_sender = client.sender();
    let stdout_clone = stdout.clone();
    client.set_on_message(move |msg| {
        let mut stdout = stdout_clone.clone();
        match msg {
            Message::Error(_code, msg) => {
                writeln!(stdout, "{msg}").unwrap();
            }
            Message::ResultSet(data) => {
                for row in data {
                    writeln!(stdout, "{row}").unwrap();
                }
            }
            _ => (),
        }
        Ok(())
    });

    let stdout_clone = stdout.clone();
    client.set_on_disconnect(move || {
        let mut stdout = stdout_clone.clone();
        writeln!(stdout, "disconnected").unwrap();
    });

    let client_fut = client.run();
    pin_mut!(client_fut);

    loop {
        tokio::select! {
            res = rl.readline() => {
                match res {
                    Ok(line) => {
                        rl.add_history_entry(line.clone());
                        if let Err(_) = message_sender.send(Message::Execute(line)) {
                            break;
                        }
                    }
                    Err(ReadlineError::Interrupted) => {
                        println!("CTRL-C");
                        break;
                    }
                    Err(ReadlineError::Eof) => {
                        println!("CTRL-D");
                        break;
                    }
                    Err(err) => {
                        println!("Error: {:?}", err);
                        break;
                    }
                }
            }
            _ = &mut client_fut => break,
        }
    }

    rl.flush()?;

    Ok(())
}
