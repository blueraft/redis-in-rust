use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    //
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            // In a loop, read data from the socket and write the data back.
            let _ = socket.write_all(b"+PONG\r\n").await;
        });
    }
}
