# Probable Fiesta

Probable Fiesta is a simple demonstration of a producer-consumer message processing pipeline using Apache Pulsar. The project includes:

- **Producers**: Send messages to a Pulsar topic.
- **Consumers**: Consume and process messages from the Pulsar topic.

The project is written in **Rust** and uses the [Apache Pulsar Rust client](https://crates.io/crates/pulsar).

## Features

- Multiple producers and consumers, each running as separate processes.
- Messages are serialized using `serde` and transmitted in JSON format.
- Pulsar is run in a Docker container for convenience.
- Demonstrates a shared subscription model for load balancing among consumers.

## Prerequisites

### Install Rust
Ensure you have Rust installed. You can install it using [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Install Docker

Install Docker to run Pulsar in a container. Follow the installation instructions for your platform: [Docker Installation Guide](https://docs.docker.com/engine/install/).

## Setting Up Apache Pulsar

To start Pulsar in Docker, a script named `pulsar.sh` is included in the root directory. This script will pull the Pulsar Docker image and run it in standalone mode.

### Running Pulsar

Run the following command:

```
./pulsar.sh
```

This script starts a Pulsar container with the following configuration:

**Broker URL**: `pulsar://127.0.0.1:6650`
**Admin Console**: `http://127.0.0.1:8080`

### Stopping Pulsar

To stop the Pulsar container, use:

```
docker stop pulsar-standalone
```

## Project Structure

```
probable-fiesta/
├── producer/           # Producer binary
│   ├── Cargo.toml
│   └── src/
│       └── main.rs
├── consumer/           # Consumer binary
│   ├── Cargo.toml
│   └── src/
│       └── main.rs
├── src
│   └── main
│       └── java
│           └── io.devhands.streams.BasicStream # Java Streams Application
├── pulsar.sh           # Script to launch Pulsar in Docker
├── Cargo.toml          # Workspace manifest
├── pom.xml             # maven project for Java applications
└── README.md           # Project documentation
```

## Building the Project

Clone the repository and navigate to the project directory:

```
git clone git@github.com:Ingvord/probable-fiesta.git
cd probable-fiesta
```

Build all workspace members:

```
cargo build --release
```

Build Java Streams application using maven:

```
mvn clean package
```

## Running the Project

**Start Producers**

Run multiple producer instances with a unique ID:

```
cargo run --release --bin producer -- 1 &
cargo run --release --bin producer -- 2 &
```

**Start Consumers**

Run multiple consumer instances with a unique ID:

```
cargo run --release --bin consumer -- 1 &
cargo run --release --bin consumer -- 2 &
```

Analytics Consumer

```
cargo run --release --bin analytics_consumer
```

**Java Stream Application**

Requires changes in the producer:

```rust
>>>>
let topic = "user-events";
====
let topic = "streams-input";
<<<<
```

```
java -jar target/streams-1.0-SNAPSHOT.jar
```


**Viewing Output**

Producers will log the messages they send.

Consumers will log the messages they receive and process.

## Configuration

**Topic**

The topic used in this project is `persistent://public/default/my-topic`.

You can modify the topic in the producer and consumer source code if needed.

**Subscription**

Consumers use a Shared subscription for load balancing. This can be modified to other subscription types (e.g., Exclusive or Failover) in the consumer code.

Example Output

Producer
```
Producer 1: Sent MyMessage { content: "Message 1 from producer 1" }
Producer 2: Sent MyMessage { content: "Message 1 from producer 2" }
```

Consumer
```
Consumer 1: Received message: MyMessage { content: "Message 1 from producer 1" }
Consumer 2: Received message: MyMessage { content: "Message 1 from producer 2" }
```

Java Streams Application
```
In  >> key: 934:        {"event_type":"click","user_id":934,"item_id":539,"timestamp":1732750026.818}
In  >> key: 355:        {"event_type":"click","user_id":355,"item_id":82,"timestamp":1732750030.998}
In  >> key: 920:        {"event_type":"click","user_id":920,"item_id":573,"timestamp":1732750028.854}
In  >> key: 471:        {"event_type":"click","user_id":471,"item_id":721,"timestamp":1732750030.431}
```


**Stopping All Processes**

To clean up, stop all producer and consumer processes, and stop the Pulsar container:

```
killall producer consumer
docker stop pulsar-standalone
```

## License

This project is licensed under the MIT License. See the LICENSE file for details.

