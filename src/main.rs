use std::env;

use redis_starter_rust::state::State;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    //

    let mut port = None;

    let args: Vec<String> = env::args().collect();

    for i in 1..args.len() {
        match args[i].as_str() {
            "--port" | "-p" => {
                if i + 1 < args.len() {
                    port = Some(args[i + 1].clone());
                }
            }
            _ => (),
        }
    }
    let port = match port {
        Some(p) => p,
        None => "6379".to_string(),
    };
    let address = format!("127.0.0.1:{port}");

    let state = State::default();
    let listener = TcpListener::bind(address).await?;
    loop {
        let (mut socket, _) = listener.accept().await?;

        let mut state = state.clone();
        tokio::spawn(async move {
            let mut buf = [0; 1024];
            loop {
                match socket.read(&mut buf).await {
                    Ok(n) => {
                        if n == 0 {
                            // connection closed
                            return;
                        }
                        let request = String::from_utf8_lossy(&buf[..n]);
                        let response = state
                            .handle_response(&request)
                            .expect("failed to generate response");
                        socket.write_all(response.as_bytes()).await.unwrap();
                    }
                    Err(e) => {
                        eprintln!("failed to read from socket; err = {:?}", e);
                        return;
                    }
                };
            }
        });
    }
}
