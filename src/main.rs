#![allow(unused_imports)]

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

fn main() {
    println!("Logs from your program will appear here!");

    if let Err(err) = run() {
        println!("server error: {}", err);
    }
}

fn run() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")?;

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                loop {
                    match run_request(&mut stream) {
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
            Err(err) => {
                println!("accept error: {}", err);
            }
        }
    }

    Ok(())
}

fn run_request(stream: &mut TcpStream) -> std::io::Result<bool> {
    let mut buffer = [0; 512];

    let size = stream.read(&mut buffer)?;

    if size == 0 {
        return Ok(false);
    }

    println!(
        "Received {} bytes: {:?}",
        size,
        String::from_utf8_lossy(&buffer[..size])
    );

    stream.write_all("+PONG\r\n".as_bytes())?;

    Ok(true)
}