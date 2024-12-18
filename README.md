Peer-to-Peer Network Implementation
This project implements a peer-to-peer network architecture where all nodes function as both clients and servers. Instead of a traditional client-server model, each peer can broadcast messages throughout the network until they reach their destination.

Key Features:
- Decentralized network architecture with no central server
- Broadcast messaging between peers
- Snowflake ID generation scheme for unique peer identification
- Multi-threaded peer implementation


Implementation Details:
- Each peer combines both server and client functionality
- Threaded execution for concurrent message sending and receiving
- Support for user message input without blocking other operations
- Protocol buffer implementation for handling multiple message types
- Built-in message confirmation system for reliable delivery
