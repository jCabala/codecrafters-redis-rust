#![allow(unused_imports)]
use std::{io::Write, net::TcpListener};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                if let Err(err) = _stream.write("PONG".as_bytes()) {
                    print!("write err: {}", err);
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
