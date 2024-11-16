use pulsar::{Pulsar, SerializeMessage, TokioExecutor};
use tokio::time::{sleep, Duration};
use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MyMessage {
    content: String,
}

impl SerializeMessage for MyMessage {
    fn serialize_message(input: Self) -> Result<pulsar::producer::Message, pulsar::Error> {
        let payload = serde_json::to_vec(&input).map_err(|e| pulsar::Error::Custom(e.to_string()))?;
        Ok(pulsar::producer::Message {
            payload,
            ..Default::default()
        })
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let default_instance_id = "0".to_string();
    let instance_id = args.get(1).unwrap_or(&default_instance_id);

    let addr = "pulsar://localhost:6650";
    let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor)
        .build()
        .await
        .expect("Failed to create Pulsar client");

    let topic = "persistent://public/default/my-topic";
    let mut producer = pulsar
        .producer()
        .with_topic(topic)
        .build()
        .await
        .expect("Failed to create producer");

    let mut count = 0;
    loop {
        let message = MyMessage {
            content: format!("Message {} from producer {}", count, instance_id),
        };

        producer
            .send_non_blocking(message.clone())
            .await
            .expect("Failed to send message");

        println!("Producer {}: Sent {:?}", instance_id, message);
        count += 1;

        // Throttle message production
        sleep(Duration::from_secs(1)).await;
    }
}