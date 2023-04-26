use futures_util::StreamExt;
use google_cloud_gax::grpc::Status;
use google_cloud_gax::retry::RetrySetting;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscription::SubscriptionConfig;

pub struct Scaler {
    client: Client,
    worker: Worker,
}

impl Scaler {
    pub async fn new(config: ClientConfig, topic_name: &str) -> Result<Scaler, Status> {
        let client = Client::new(config).await.unwrap();

        // Get the topic to subscribe to.
        let topic = client.topic(topic_name);

        // Create subscription
        // If subscription name does not contain a "/", then the project is taken from client above. Otherwise, the
        // name will be treated as a fully qualified resource name
        let config = SubscriptionConfig {
            // Enable message ordering if needed (https://cloud.google.com/pubsub/docs/ordering)
            enable_message_ordering: true,
            ..Default::default()
        };

        // Create subscription
        let subscription = client.subscription(format!("{}-subscription", topic_name).as_str());
        if !subscription.exists(None).await? {
            subscription
                .create(topic.fully_qualified_name(), config, None)
                .await?;
        }

        let worker = Worker::new();

        Ok(Scaler { client, worker })
    }

    pub async fn scale(&self) -> Result<(), Status> {
        let subscriptions = self
            .client
            .get_subscriptions(Some(RetrySetting::default()))
            .await?;

        for subscription in &subscriptions {
            // Read the messages as a stream
            // Note: This blocks the current thread but helps working with non clonable data
            let mut stream = subscription.subscribe(None).await?;
            while let Some(message) = stream.next().await {
                // Ack or Nack message.
                let _ = message.ack().await;

                self.worker.execute(Message::NewJob(message.message)).await;
            }
        }
        Ok(())
    }
}

pub struct Worker {}

impl Worker {
    fn new() -> Worker {
        Worker {}
    }

    async fn execute(&self, job: Message) {
        match job {
            Message::NewJob(job) => {
                // Handle data.
                println!("Got Message: {:?}", job);
            }
        }
    }
}

pub enum Message {
    NewJob(PubsubMessage),
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_default::WithAuthExt;

    #[tokio::test]
    async fn test_new_scaler() {
        let config = ClientConfig::default().with_auth().await.unwrap();
        let _ = Scaler::new(config, "test-topic");
    }

    #[tokio::test]
    async fn test_new_worker() {
        let worker = Worker::new();
        let msg = PubsubMessage {
            data: "abc".into(),
            // Set ordering_key if needed (https://cloud.google.com/pubsub/docs/ordering)
            ordering_key: "order".into(),
            ..Default::default()
        };
        worker.execute(Message::NewJob(msg)).await;
    }
}
