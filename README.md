[![progress-banner](https://backend.codecrafters.io/progress/redis/5bab33ac-dbad-4d21-bdea-ccf4c389b689)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

This is a starting point for Rust solutions to the
["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

A simplified implementation of Redis. The complete challenge on codecrafters, including all of the extension challenges.

- Limited support basic commands like SET, GET, PING, INFO. 
- Limited support of replication for master and slave nodes (propagate commands from master-to-slave and vice versa, sync DB)
- Limited reading of rdb dump files (only BulkString representations for now)
- Limited streaming command support

