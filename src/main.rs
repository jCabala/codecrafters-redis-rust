#![allow(unused_imports)]

mod resp;

use resp::{Command, CommandName, RespMessage};
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

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                tokio::spawn(async move {
                    handle_connection(stream).await;
                });
            }
            Err(err) => {
                println!("accept error: {}", err);
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream) {
    loop {
        match run_request(&mut stream).await {
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

async fn run_request(stream: &mut TcpStream) -> std::io::Result<bool> {
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
        },
        Err(err) => err,
    };

    stream.write_all(response.encode().as_bytes()).await?;

    Ok(true)
}