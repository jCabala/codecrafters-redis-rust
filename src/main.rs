#![allow(unused_imports)]

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

    stream.write_all("+PONG\r\n".as_bytes()).await?;

    Ok(true)
}