use std::env;

use redis_starter_rust::{
    replica::{initiate_replica_connection, send_write_to_replica},
    resp::RedisData,
    state::{MasterConfig, State},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{net::TcpListener, sync::broadcast};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    //

    let mut port = None;
    let mut replicaof = false;
    let mut master_host = None;
    let mut master_port = None;
    let args: Vec<String> = env::args().collect();

    for i in 1..args.len() {
        match args[i].as_str() {
            "--port" | "-p" => {
                if i + 1 < args.len() {
                    port = Some(args[i + 1].clone());
                }
            }
            "--replicaof" => {
                if i + 2 < args.len() {
                    master_host = Some(args[i + 1].clone());
                    master_port = Some(args[i + 2].parse().clone()?);
                }
                replicaof = true;
            }

            _ => (),
        }
    }
    let port = match port {
        Some(p) => p,
        None => "6379".to_string(),
    };
    let address = format!("127.0.0.1:{port}");

    let master_config = if let (Some(host), Some(port)) = (master_host, master_port) {
        Some(MasterConfig { host, port })
    } else {
        None
    };

    let state = State::new(replicaof, master_config.clone());
    if let Some(config) = master_config {
        let state = state.clone();
        tokio::spawn(async move { initiate_replica_connection(state, config).await });
    };

    let listener = TcpListener::bind(address).await?;

    let (tx, _rx) = broadcast::channel(100);

    loop {
        let (mut socket, _) = listener.accept().await?;
        let mut state = state.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let mut buf = [0; 1024];
            let mut replica_initialized = false;
            loop {
                match socket.read(&mut buf).await {
                    Ok(n) => {
                        if n == 0 {
                            // connection closed
                            return;
                        }
                        let request = String::from_utf8_lossy(&buf[..n]);
                        let redis_data =
                            RedisData::parse(&request).expect("failed to parse request");
                        let _ = match redis_data {
                            RedisData::Set(_, _, _) => tx.send(buf[..n].to_vec()),
                            _ => Ok(0),
                        };
                        let response = state
                            .handle_response(&redis_data)
                            .expect("failed to generate response");
                        socket.write_all(response.as_bytes()).await.unwrap();
                        if let RedisData::Psync(_, _) = redis_data {
                            let rdb = state.replica_request().unwrap();
                            let _ = socket.write_all(&rdb).await;
                            replica_initialized = true;
                        };
                    }
                    Err(e) => {
                        eprintln!("failed to read from socket; err = {:?}", e);
                        return;
                    }
                };
                if replica_initialized {
                    break;
                }
            }
            if replica_initialized {
                let rx = tx.subscribe();
                tokio::spawn(async move { send_write_to_replica(rx, socket).await });
            }
        });
    }
}
