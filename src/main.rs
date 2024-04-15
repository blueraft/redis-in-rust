use std::sync::Arc;

use redis_starter_rust::{
    config::{load_config, Config},
    replica::{initiate_replica_connection, send_write_to_replica},
    resp::RedisData,
    state::State,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
};
use tokio::{net::TcpListener, sync::broadcast};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Config {
        master_config,
        port,
        replicaof,
    } = load_config()?;

    let address = format!("127.0.0.1:{port}");
    let state = State::new(replicaof, master_config.clone());
    if let Some(config) = master_config {
        let state = state.clone();
        tokio::spawn(async move { initiate_replica_connection(state, config).await });
    };

    let listener = TcpListener::bind(address).await?;

    let (replica_tx, _rx) = broadcast::channel(100);

    loop {
        let (socket, _socket_addr) = listener.accept().await?;
        let mut state = state.clone();
        let socket = Arc::new(Mutex::new(socket));
        let replica_tx = replica_tx.clone();
        tokio::spawn(async move {
            let mut buf = [0; 1024];
            loop {
                match socket.lock().await.read(&mut buf).await {
                    Ok(n) => {
                        if n == 0 {
                            // connection closed
                            return;
                        }
                        let request = String::from_utf8_lossy(&buf[..n]);
                        let redis_data =
                            RedisData::parse(&request).expect("failed to parse request");

                        let _ = match &redis_data {
                            RedisData::Set(_, _, _) => replica_tx.send(buf[..n].to_vec()),
                            _ => Ok(0),
                        };

                        if let RedisData::Wait(num_replicas, timeout) = redis_data {
                            let getack = b"*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n";
                            let _ = replica_tx.send(getack.to_vec());
                        } else {
                            let response = state
                                .handle_response(&redis_data)
                                .expect("failed to generate response");
                            let mut socket_write = socket.lock().await;
                            socket_write.write_all(response.as_bytes()).await.unwrap();
                            if let RedisData::Psync(_, _) = redis_data {
                                let rdb = state.replica_request().unwrap();
                                let _ = socket_write.write_all(&rdb).await;
                                state.increment_num_replicas();
                                let rx = replica_tx.subscribe();
                                let socket = socket.clone();
                                tokio::spawn(
                                    async move { send_write_to_replica(rx, socket).await },
                                );
                            };
                        }
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
