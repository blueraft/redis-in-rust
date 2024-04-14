use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
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
                let redis_data = RedisData::parse(&request).expect("failed to parse request");
                let response = state
                    .handle_response(&redis_data)
                    .expect("failed to generate response");
                stream.write_all(response.as_bytes()).await.unwrap();
            }
            Err(e) => {
                eprintln!("failed to read from socket; err = {:?}", e);
                return Ok(());
            }
        };
    }
}
