use crate::messages::Message;

use anyhow::Result;
use message_io::network::{NetEvent, Transport};
use message_io::node::{self, NodeHandler, NodeListener};
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::thread;

const HISTORY_FILE: &str = ".edge_history";

pub(crate) fn start() -> Result<()> {
    let (handler, node_listener): (NodeHandler<()>, NodeListener<()>) = node::split();
    let discovery_addr = "127.0.0.1:5000";
    let (endpoint, _) = handler
        .network()
        .connect(Transport::FramedTcp, discovery_addr)?;
    let mut rl = Editor::<()>::new().unwrap();
    if rl.load_history(HISTORY_FILE).is_err() {
        println!("No previous history.");
    }
    let listener = thread::spawn(|| {
        node_listener.for_each(move |event| match event.network() {
            NetEvent::Connected(_, _) => (),
            NetEvent::Accepted(_, _) => (),
            NetEvent::Message(_endpoint, input_data) => {
                let message: Message = bincode::deserialize(input_data).unwrap();
                match message {
                    Message::ResultSet(rows) => {
                        println!(">> {:?}", rows);
                    }
                    Message::Error(message) => {
                        println!(">> {}", message);
                    }
                    _ => {
                        todo!();
                    }
                }
            }
            NetEvent::Disconnected(_endpoint) => {}
        });
    });
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                let message = Message::Execute(line.to_string());
                let output_data = bincode::serialize(&message).unwrap();
                handler.network().send(endpoint, &output_data);
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
    rl.save_history(HISTORY_FILE)?;
    listener.join().unwrap();
    Ok(())
}
