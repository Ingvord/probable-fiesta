use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use serde::{Deserialize, Serialize};
use futures::StreamExt;

#[derive(Debug, Serialize, Deserialize)]
struct Event {
    event_type: String,
    user_id: i32,
    item_id: i32,
    timestamp: f64,
}

#[tokio::main]
async fn main() {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "user-events-group") // Consumer group ID
        .set("bootstrap.servers", "localhost:19092")
        .set("enable.partition.eof", "false")
        .create()
        .expect("Failed to create Kafka consumer");

    let topic = "user-events";

    consumer
        .subscribe(&[topic])
        .expect("Failed to subscribe to topic");

    println!("Consumer listening on topic: {}", topic);

    while let Some(result) = consumer.stream().next().await {
        match result {
            Ok(message) => {
                // Extract and display the key
                let key = match message.key() {
                    Some(key) => String::from_utf8_lossy(key).to_string(),
                    None => "None".to_string(),
                };

                // Extract and deserialize the payload
                if let Some(payload) = message.payload() {
                    match serde_json::from_slice::<Event>(payload) {
                        Ok(event) => {
                            println!(
                                "Consumed event: Key = {}, Event = {:?}",
                                key, event
                            );
                        }
                        Err(err) => {
                            eprintln!("Failed to deserialize payload: {:?}", err);
                        }
                    }
                } else {
                    eprintln!("Message payload is empty");
                }
            }
            Err(err) => eprintln!("Error consuming message: {:?}", err),
        }
    }
}
