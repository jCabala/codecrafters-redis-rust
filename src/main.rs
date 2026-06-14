#![allow(unused_imports)]

mod resp;
mod store;

use resp::{Command, CommandName, RespMessage};
use store::Store;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    if let Err(err) = run().await {
        println!("server error: {}", err);
    }
}

async fn run() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let store = Store::new();

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let store = store.clone();
                tokio::spawn(async move {
                    handle_connection(stream, store).await;
                });
            }
            Err(err) => {
                println!("accept error: {}", err);
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream, store: Store) {
    loop {
        match run_request(&mut stream, &store).await {
            Ok(false) => {
                // Client closed the connection.
                break;
            }
            Ok(true) => {
                // Handled one request, keep waiting for more.
            }
            Err(err) => {
                println!("connection error: {}", err);
                break;
            }
        }
    }
}

async fn run_request(stream: &mut TcpStream, store: &Store) -> std::io::Result<bool> {
    let mut buffer = [0; 512];

    let size = stream.read(&mut buffer).await?;

    if size == 0 {
        return Ok(false);
    }

    println!(
        "Received {} bytes: {:?}",
        size,
        String::from_utf8_lossy(&buffer[..size])
    );

    let response = match Command::parse(&buffer[..size]) {
        Ok(command) => match command.name {
            CommandName::Ping => RespMessage::SimpleString("PONG".to_string()),
            CommandName::Echo => match command.args.first() {
                Some(arg) => RespMessage::BulkString(arg.clone()),
                None => RespMessage::Error(
                    "ERR wrong number of arguments for 'echo' command".to_string(),
                ),
            },
            CommandName::Set => match (command.args.first(), command.args.get(1)) {
                (Some(key), Some(value)) => {
                    store.set(key.clone(), value.clone());
                    RespMessage::SimpleString("OK".to_string())
                }
                _ => RespMessage::Error(
                    "ERR wrong number of arguments for 'set' command".to_string(),
                ),
            },
            CommandName::Get => match command.args.first() {
                Some(key) => match store.get(key) {
                    Some(value) => RespMessage::BulkString(value),
                    None => RespMessage::NullBulkString,
                },
                None => RespMessage::Error(
                    "ERR wrong number of arguments for 'get' command".to_string(),
                ),
            },
        },
        Err(err) => err,
    };

    stream.write_all(response.encode().as_bytes()).await?;

    Ok(true)
}