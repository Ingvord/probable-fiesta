use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

fn main() {
    // Load configuration from config.yaml
    // Create Kafka consumer
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:19092")
        .set("group.id", "analytics-group")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Failed to create Kafka consumer");

    consumer
        .subscribe(&["user-events"])
        .expect("Failed to subscribe to topic");

    let mut event_counts: HashMap<String, u32> = HashMap::from([
        ("page_view".to_string(), 0),
        ("click".to_string(), 0),
        ("purchase".to_string(), 0),
    ]);
    let mut distinct_users: HashSet<i32> = HashSet::new();

    let mut last_print_time = Instant::now();

    println!("Consumer started. Listening for messages...");

    loop {
        match consumer.poll(Duration::from_secs(1)) {
            Some(result) => match result {
                Ok(msg) => {
                    // Decode the message payload
                    if let Some(payload) = msg.payload() {
                        let event: Value = serde_json::from_slice(payload).unwrap_or_else(|_| {
                            eprintln!("Failed to parse message payload");
                            Value::Null
                        });

                        let event_type = event["event_type"].as_str().unwrap_or("");
                        let user_id = event["user_id"].as_i64();

                        if let Some(count) = event_counts.get_mut(event_type) {
                            *count += 1;
                        }

                        if let Some(user_id) = user_id {
                            distinct_users.insert(user_id as i32);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Kafka error: {}", e);
                }
            },
            None => {
                // No message received; continue
            }
        }

        // Print aggregates every 3 seconds
        if last_print_time.elapsed() > Duration::from_secs(3) {
            print_aggregates(&event_counts, &distinct_users);

            // Reset aggregates
            event_counts.values_mut().for_each(|v| *v = 0);
            distinct_users.clear();

            last_print_time = Instant::now();
        }
    }
}

fn print_aggregates(event_counts: &HashMap<String, u32>, distinct_users: &HashSet<i32>) {
    println!("Event Counts:");
    for (event_type, count) in event_counts {
        println!("  {}: {}", event_type, count);
    }
    println!("Distinct Users: {}", distinct_users.len());
}
