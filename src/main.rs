use std::time::Duration;

use redis_starter_rust::{
    config::{load_config, Config},
    db::Database,
    replica::{initiate_replica_connection, send_write_to_client, send_write_to_replica},
    resp::RedisData,
    state::State,
};
use tokio::{
    io::AsyncReadExt,
    sync::{mpsc, watch},
};
use tokio::{net::TcpListener, sync::broadcast};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Config {
        master_config,
        port,
        replicaof,
        db_config,
    } = load_config()?;

    let address = format!("127.0.0.1:{port}");
    println!("main address: {address}");
    let db = Database::initialize(db_config);
    let state = State::new(replicaof, db);
    if let Some(config) = &master_config {
        let state = state.clone();
        let config = config.clone();
        tokio::spawn(async move { initiate_replica_connection(state, config).await });
    };

    let listener = TcpListener::bind(address).await?;

    let (replica_tx, _rx) = broadcast::channel(100);
    let (xadd_tx, _rx) = watch::channel("hello");
    let (ack_tx, _rx) = broadcast::channel(100);

    loop {
        let (socket, socket_addr) = listener.accept().await?;
        let (client_tx, client_rx) = mpsc::channel::<Vec<u8>>(100);
        let mut state = state.clone();
        let (mut reader, writer) = socket.into_split();
        let replica_tx = replica_tx.clone();
        let ack_tx = ack_tx.clone();
        let xadd_tx = xadd_tx.clone();
        let master_config = master_config.clone();
        println!("got connection from {socket_addr:?}");
        tokio::spawn(async move { send_write_to_client(client_rx, writer).await });
        tokio::spawn(async move {
            let mut buf = [0; 1024];
            loop {
                match reader.read(&mut buf).await {
                    Ok(n) => {
                        if n == 0 {
                            // connection closed
                            return;
                        }
                        let request = String::from_utf8_lossy(&buf[..n]);
                        let redis_data =
                            RedisData::parse(&request).expect("failed to parse request");
                        println!("got data {redis_data:?}");

                        let redis_data =
                            if let RedisData::Xread(stream, pairs, block_duration) = redis_data {
                                // if the user requests blocking reads using $, we have to swap the
                                // start time with the maximum ID
                                RedisData::Xread(stream, state.swap_pairs(&pairs), block_duration)
                            } else {
                                redis_data
                            };

                        match &redis_data {
                            RedisData::Set(_, _, _) => {
                                let _ = replica_tx.send(buf[..n].to_vec());
                                if master_config.is_none() {
                                    println!("Incrementing primary by {n}");
                                    state.increment_offset(n);
                                };
                            }
                            RedisData::ReplConf(cmd, arg) => {
                                if let "ack" = cmd.data.to_lowercase().as_str() {
                                    let offset: usize =
                                        arg.data.parse().expect("failed to parse ack offset");
                                    println!("sending {offset} from {socket_addr}");
                                    let _ = ack_tx.send((offset, socket_addr));
                                }
                            }
                            RedisData::Xread(_, _, Some(duration)) => {
                                if *duration == 0 {
                                    let mut xadd_rx = xadd_tx.subscribe();
                                    match xadd_rx.changed().await {
                                        Ok(_) => {
                                            println!("data set in stream");
                                        }
                                        Err(_) => panic!("failed to send"),
                                    }
                                } else {
                                    let duration = Duration::from_millis(*duration);
                                    tokio::time::sleep(duration).await;
                                }
                            }

                            _ => (),
                        };

                        if let RedisData::Wait(target_num_replicas, timeout) = redis_data {
                            let getack = b"*3\r\n$8\r\nreplconf\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
                            let _ = replica_tx.send(getack.to_vec());
                            let rx = ack_tx.subscribe();
                            let synced_replicas = state
                                .count_synced_replicas(target_num_replicas, timeout, rx)
                                .await
                                .expect("failed get synced replica count");
                            let response = format!(":{}\r\n", synced_replicas);
                            let _ = client_tx.send(response.as_bytes().to_vec()).await;
                        } else {
                            // TODO: improve response handling
                            let response = state
                                .handle_response(&redis_data)
                                .expect("failed to generate response");
                            if !response.is_empty() {
                                client_tx.send(response.as_bytes().to_vec()).await.unwrap();
                            }
                            if let RedisData::Xadd(_, _, _) = redis_data {
                                xadd_tx.send("xadd").expect("failed to send xadd ack");
                            }
                            if let RedisData::Psync(_, _) = redis_data {
                                let rdb = state.replica_request().unwrap();
                                client_tx.send(rdb.to_vec()).await.unwrap();
                                state.increment_num_replicas();
                                let client_tx = client_tx.clone();
                                let replica_rx = replica_tx.subscribe();
                                tokio::spawn(async move {
                                    send_write_to_replica(replica_rx, client_tx).await
                                });
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
