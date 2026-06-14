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

    let response = match parse_command(&buffer[..size]) {
        Some(args) if !args.is_empty() => match args[0].to_uppercase().as_str() {
            "PING" => "+PONG\r\n".to_string(),
            "ECHO" => match args.get(1) {
                Some(arg) => format!("${}\r\n{}\r\n", arg.len(), arg),
                None => "-ERR wrong number of arguments for 'echo' command\r\n".to_string(),
            },
            cmd => format!("-ERR unknown command '{}'\r\n", cmd),
        },
        _ => "-ERR Protocol error: invalid request\r\n".to_string(),
    };

    stream.write_all(response.as_bytes()).await?;

    Ok(true)
}

/// Parses a RESP array of bulk strings (e.g. `*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n`)
/// into its component strings.
fn parse_command(input: &[u8]) -> Option<Vec<String>> {
    let text = std::str::from_utf8(input).ok()?;
    let mut lines = text.split("\r\n");

    let header = lines.next()?;
    let count: usize = header.strip_prefix('*')?.parse().ok()?;

    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        let len_line = lines.next()?;
        let len: usize = len_line.strip_prefix('$')?.parse().ok()?;

        let data = lines.next()?;
        if data.len() != len {
            return None;
        }

        args.push(data.to_string());
    }

    Some(args)
}