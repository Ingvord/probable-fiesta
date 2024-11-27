use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Serialize, Deserialize};
use rand::Rng;
use tokio::time::{sleep, Duration};

#[derive(Serialize, Deserialize, Debug)]
struct Event {
    event_type: String,
    user_id: i32,
    item_id: i32,
    timestamp: f64,
}

#[tokio::main]
async fn main() {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:19092")
        .create()
        .expect("Failed to create Kafka producer");

    let topic = "user-events";
    let mut rng = rand::thread_rng();

    loop {
        let user_id = rng.gen_range(1..=1000); // Random user ID
        let event = Event {
            event_type: "click".to_string(),
            user_id,
            item_id: rng.gen_range(1..=1000), // Random item ID
            timestamp: chrono::Utc::now().timestamp_millis() as f64 / 1000.0, // Current timestamp
        };

        let key = user_id.to_string();
        let value = serde_json::to_string(&event).expect("Failed to serialize event");

        // Send the message with the user_id as the key
        producer
            .send(
                FutureRecord::to(topic)
                    .key(&key) // Set the random key
                    .payload(&value), // Set the event as the payload
                Duration::from_secs(0),
            )
            .await
            .unwrap_or_else(|e| {
                eprintln!("Failed to send message: {:?}", e);
                (-1, -1) // Fallback value
            });

        println!("Produced event: {:?}", event);

        // Random delay between 0.3 and ~0.6 seconds
        sleep(Duration::from_secs_f64(0.3 + rng.gen_range(0.0..=0.3))).await;
    }
}
