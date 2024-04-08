use anyhow::Context;
use redis_starter_rust::resp::RedisData;
use tokio::net::TcpListener;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

async fn handle_response(request: &str, socket: &mut TcpStream) -> anyhow::Result<()> {
    let redis_data = RedisData::parse(request)?;
    let response = match redis_data.command {
        redis_starter_rust::resp::Command::Ping => "+PONG\r\n".to_owned(),
        redis_starter_rust::resp::Command::Echo => {
            if let Some(data) = redis_data.data {
                data.decode()
            } else {
                anyhow::bail!("No data provided")
            }
        }
    };
    socket
        .write_all(response.as_bytes())
        .await
        .context("write response")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    //
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        let (mut socket, _) = listener.accept().await?;

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
                        handle_response(&request, &mut socket).await.unwrap();
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
