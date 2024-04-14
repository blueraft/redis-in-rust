use regex::Regex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::broadcast::Receiver,
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
                    match RedisData::parse(request) {
                        Ok(redis_data) => {
                            let response = state
                                .handle_response(&redis_data)
                                .expect("failed to generate response");
                            // after handshake is complete, only the replconf provides responses to
                            // primary
                            match redis_data {
                                RedisData::ReplConf(_, _) => {
                                    stream.write_all(response.as_bytes()).await
                                }
                                _ => Ok(()),
                            }
                            .expect("failed to send response");
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
    mut rx: Receiver<Vec<u8>>,
    mut socket: TcpStream,
) -> anyhow::Result<()> {
    while let Ok(msg) = rx.recv().await {
        socket.write_all(&msg).await?;
    }
    Ok(())
}
