use regex::Regex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{tcp::OwnedWriteHalf, TcpStream},
    sync::{broadcast, mpsc},
};

use crate::{
    resp::RedisData,
    state::{MasterConfig, State},
};

/// The replica initiates a conection with the primary and starts the replication process
///
/// 1. PING (expecting +PONG\r\n back)
/// 2. REPLCONF listening-port <PORT> (expecting +OK\r\n back)
/// 3. REPLCONF capa eof capa psync2 (expecting +OK\r\n back)
/// 4. PSYNC ? -1 (expecting +FULLRESYNC <REPL_ID> 0\r\n back)
/// 5. Listen to redis_data from the primary
pub async fn initiate_replica_connection(
    mut state: State,
    config: MasterConfig,
) -> anyhow::Result<()> {
    let address = format!("{}:{}", config.host, config.port);
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(b"*1\r\n$4\r\nping\r\n").await.unwrap();
    let mut buf = [0; 1024];

    match stream.read(&mut buf).await {
        Ok(n) => {
            if n == 0 {
                return Ok(());
            }

            let request = String::from_utf8_lossy(&buf[..n]);
            println!("replica got request {request}");
            let replconf1 = b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n";
            stream.write_all(replconf1).await.unwrap();
            let replconf2 = b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
            stream.write_all(replconf2).await.unwrap();
        }
        Err(_) => todo!(),
    }
    match stream.read(&mut buf).await {
        Ok(n) => {
            if n == 0 {
                return Ok(());
            }
            let psync_initial = b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
            stream.write_all(psync_initial).await.unwrap();
        }

        Err(_) => todo!(),
    }
    println!("initialization complete");

    loop {
        match stream.read(&mut buf).await {
            Ok(n) => {
                if n == 0 {
                    // connection closed
                    return Ok(());
                }
                let request = String::from_utf8_lossy(&buf[..n]);
                let re = Regex::new(r"\*\d+\r\n").unwrap();
                for request in re.split(&request) {
                    if request.is_empty() {
                        continue;
                    }
                    match RedisData::parse(request) {
                        Ok(redis_data) => {
                            // after handshake is complete, only the replconf provides responses to
                            // primary
                            match redis_data {
                                RedisData::ReplConf(_, _) => {
                                    let response = state
                                        .handle_response(&redis_data)
                                        .expect("failed to generate response");
                                    stream.write_all(response.as_bytes()).await
                                }
                                _ => Ok(()),
                            }
                            .expect("failed to send response");
                            // TODO: this should be optimised
                            let total_req = format!(
                                "*{}\r\n{}",
                                request.split('$').collect::<Vec<&str>>().len(),
                                request
                            );
                            let n = total_req.as_bytes().len();
                            state.increment_offset(n);
                            println!("Incrementing replica by {n}");
                        }
                        Err(e) => {
                            eprintln!("failed to parse request {request:?}; err = {e:?}");
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("failed to read from socket; err = {:?}", e);
                return Ok(());
            }
        };
    }
}

/// Send `SET` requests from primary to replicas
pub async fn send_write_to_replica(
    mut rx: broadcast::Receiver<Vec<u8>>,
    client_tx: mpsc::Sender<Vec<u8>>,
) -> anyhow::Result<()> {
    while let Ok(msg) = rx.recv().await {
        client_tx.send(msg).await?;
    }
    Ok(())
}

pub async fn send_write_to_client(
    mut rx: mpsc::Receiver<Vec<u8>>,
    mut writer: OwnedWriteHalf,
) -> anyhow::Result<()> {
    while let Some(msg) = rx.recv().await {
        writer.write_all(&msg).await?;
    }
    Ok(())
}
