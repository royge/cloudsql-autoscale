use google_cloud_gax::grpc::Status;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscription::SubscriptionConfig;
use std::rc::Rc;

pub struct Receiver {
    client: Rc<Client>,
}

impl Receiver {
    pub async fn new(config: ClientConfig, topic_name: &str) -> Result<Receiver, Status> {
        let client = Client::new(config).await.unwrap();

        let client = Rc::new(client);

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

        Ok(Receiver { client })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_new_receiver() {

    }
}
