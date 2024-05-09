[![progress-banner](https://backend.codecrafters.io/progress/redis/5bab33ac-dbad-4d21-bdea-ccf4c389b689)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

This is a starting point for Rust solutions to the
["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

A simplified implementation of Redis. The complete challenge on codecrafters, including all of the extension challenges.

- Limited support basic commands like SET, GET, PING, INFO. 
- Limited support of replication for master and slave nodes (propagate commands from master-to-slave and vice versa, sync DB)
- Limited reading of rdb dump files (only BulkString representations for now)
- Limited streaming command support


## Prerequisites
Before running the Redis server locally, ensure you have the following installed:
- Rust and Cargo: Follow the instructions at [rustup.rs](https://rustup.rs/) to install Rust and Cargo.

## Installation and Usage
1. Clone this repository to your local machine:
    ```
    git clone https://github.com/blueraft/redis-in-rust.git
    ```
2. Navigate into the cloned directory:
    ```
    cd redis-in-redis
    ```
3. Build and run the Redis server using Cargo:
    ```
    cargo run
    ```
   This will compile the code and start the Redis server locally. You should see log messages indicating that the server is running.

## Interacting with Redis
After starting the Redis server, you can interact with it using the Redis command-line interface (`redis-cli`).

### Using `redis-cli`
1. Open a new terminal window or tab.
2. Run `redis-cli` to start the Redis command-line interface:
    ```
    redis-cli
    ```
3. Once connected, you can start issuing Redis commands. For example:
    ```
    SET mykey "Hello"
    ```
   This command sets the value of the key `mykey` to `"Hello"`.
4. You can retrieve the value of `mykey` by using the `GET` command:
    ```
    GET mykey
    ```
   This will return `"Hello"`.
