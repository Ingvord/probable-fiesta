use pulsar::{DeserializeMessage, Payload, Consumer, Pulsar, SubType, TokioExecutor};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MyMessage {
    content: String,
}

impl DeserializeMessage for MyMessage {
    type Output = Result<Self, pulsar::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        serde_json::from_slice(&payload.data)
            .map_err(|e| pulsar::Error::Custom(e.to_string()))
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
    let mut consumer: Consumer<MyMessage, _> = pulsar
        .consumer()
        .with_topic(topic)
        .with_subscription_type(SubType::Shared)
        .with_subscription("my-subscription")
        .build()
        .await
        .expect("Failed to create consumer");

    println!("Consumer {}: Listening for messages...", instance_id);

    while let Some(message) = consumer.try_next().await.unwrap() {
        let payload = message.deserialize().expect("Failed to deserialize message");
        println!(
            "Consumer {}: Received message: {:?}",
            instance_id, payload
        );
        consumer.ack(&message).await.expect("Failed to acknowledge message");
    }
}