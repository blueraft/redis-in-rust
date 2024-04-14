use std::env;

use redis_starter_rust::{
    resp::RedisData,
    state::{MasterConfig, State},
};
use tokio::net::TcpListener;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

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

    {
        let mut state = state.clone();
        tokio::spawn(async move {
            if let Some(config) = master_config {
                let address = format!("{}:{}", config.host, config.port);
                let mut stream = TcpStream::connect(address).await.unwrap();
                stream.write_all(b"*1\r\n$4\r\nping\r\n").await.unwrap();
                let mut buf = [0; 1024];

                match stream.read(&mut buf).await {
                    Ok(n) => {
                        if n == 0 {
                            return;
                        }
                        let replconf1 =
                            b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n";
                        stream.write_all(replconf1).await.unwrap();
                        let replconf2 = b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                        stream.write_all(replconf2).await.unwrap();
                    }
                    Err(_) => todo!(),
                }
                match stream.read(&mut buf).await {
                    Ok(n) => {
                        if n == 0 {
                            return;
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
                                return;
                            }
                            let request = String::from_utf8_lossy(&buf[..n]);
                            let redis_data =
                                RedisData::parse(&request).expect("failed to parse request");
                            let response = state
                                .handle_response(&redis_data)
                                .expect("failed to generate response");
                            stream.write_all(response.as_bytes()).await.unwrap();
                        }
                        Err(e) => {
                            eprintln!("failed to read from socket; err = {:?}", e);
                            return;
                        }
                    };
                }
            }
        });
    }

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
                        let redis_data =
                            RedisData::parse(&request).expect("failed to parse request");
                        let response = state
                            .handle_response(&redis_data)
                            .expect("failed to generate response");
                        socket.write_all(response.as_bytes()).await.unwrap();
                        match redis_data {
                            RedisData::Psync(_, _) => {
                                let rdb = state.replica_request().unwrap();
                                socket.write_all(&rdb).await
                            }
                            _ => Ok(()),
                        }
                        .expect("Failed to write to replica");
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
