use futures_util::StreamExt;
use google_cloud_gax::grpc::Status;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscription::{Subscription, SubscriptionConfig};
use std::cell::RefCell;
use std::sync::{Arc, Mutex};

pub struct Scaler<W> {
    client: Option<Client>,
    worker: Option<Arc<Mutex<W>>>,
    subscription: Option<Subscription>,
    artifact_store: RefCell<Vec<String>>,
}

impl<W: Worker> Scaler<W> {
    pub async fn new(config: ClientConfig, topic_name: &str) -> Result<Scaler<W>, Status> {
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

        Ok(Scaler {
            client: Some(client),
            worker: None,
            subscription: Some(subscription),
            artifact_store: RefCell::new(vec![]),
        })
    }

    pub async fn scale(&self) -> Result<(), Status> {
        let subscription = self.subscription.as_ref().unwrap();
        // Read the messages as a stream
        // Note: This blocks the current thread but helps working with non clonable data
        let mut stream = subscription.subscribe(None).await?;
        while let Some(message) = stream.next().await {
            // Ack or Nack message.
            let _ = message.ack().await;

            let msg = message.message.clone();
            let msg_id = msg.message_id;

            if self.artifact_store.borrow().contains(&msg_id) {
                continue;
            }

            let status = self
                .worker
                .as_ref()
                .expect("No job worker defined!")
                .lock()
                .unwrap()
                .execute(Message::NewJob(message.message));

            self.artifact_store.borrow_mut().push(msg_id);

            match status {
                JobStatus::Stop => break,
                JobStatus::Continue => {}
            }
        }
        Ok(())
    }
}

pub trait Worker {
    fn execute(&self, job: Message) -> JobStatus;
}

pub struct JobWorker {}

impl JobWorker {
    fn new() -> JobWorker {
        JobWorker {}
    }
}

impl Worker for JobWorker {
    fn execute(&self, job: Message) -> JobStatus {
        match job {
            Message::NewJob(job) => {
                // Handle data.
                println!("Got Message: {:?}", job);
            }
        }

        JobStatus::Continue
    }
}

pub enum Message {
    NewJob(PubsubMessage),
}

pub enum JobStatus {
    Continue,
    Stop,
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_default::WithAuthExt;
    use std::cell::RefCell;
    use std::str::from_utf8;

    struct DummyWorker {
        data: RefCell<Vec<String>>,
        iteration: RefCell<usize>,
    }

    impl DummyWorker {
        fn new() -> DummyWorker {
            DummyWorker {
                data: RefCell::new(Vec::new()),
                iteration: RefCell::new(0),
            }
        }
    }

    async fn publish_data(client: Client, inputs: Vec<String>, topic_name: &str) {
        // Create topic.
        let topic = client.topic(topic_name);

        // Start publisher.
        let publisher = topic.new_publisher(None);

        for msg in inputs.iter() {
            let msg = PubsubMessage {
                data: msg.as_bytes().to_vec(),
                // Set ordering_key if needed (https://cloud.google.com/pubsub/docs/ordering)
                ordering_key: "order".into(),
                ..Default::default()
            };
            println!("publishing: {:?}", msg.data);

            // Send a message. There are also `publish_bulk` and `publish_immediately` methods.
            let awaiter = publisher.publish(msg).await;

            // The get method blocks until a server-generated ID or an error is returned for the published message.
            awaiter.get().await.unwrap();
            println!("done!");
        }

        // Wait for publishers in topic finish.
        let mut publisher = publisher;
        publisher.shutdown().await;
    }

    impl Worker for DummyWorker {
        fn execute(&self, job: Message) -> JobStatus {
            match job {
                Message::NewJob(job) => {
                    println!("Got Message Data: {:?}", job.data);
                    self.data
                        .borrow_mut()
                        .push(from_utf8(&job.data).unwrap().to_string());
                }
            }

            let iter = *self.iteration.borrow();
            if iter == 1 {
                return JobStatus::Stop;
            }

            *self.iteration.borrow_mut() = iter - 1;

            JobStatus::Continue
        }
    }

    #[tokio::test]
    async fn test_new_scaler() {
        let topic_name = "test-cloudsql-autoscaler";
        let config = ClientConfig::default().with_auth().await.unwrap();
        let scaler = Scaler::new(config, topic_name);
        let mut scaler = scaler.await.unwrap();

        let mut dummy_worker = DummyWorker::new();
        let mut inputs = vec![
            String::from("test1"),
            String::from("test2"),
            String::from("test1"),
            String::from("test2"),
            String::from("test3"),
            String::from("test1"),
            String::from("test2"),
            String::from("test1"),
            String::from("test2"),
            String::from("test3"),
            String::from("test3"),
            String::from("test1"),
            String::from("test2"),
            String::from("test3"),
            String::from("test1"),
            String::from("test2"),
            String::from("test3"),
            String::from("test3"),
        ];
        dummy_worker.iteration = RefCell::new(inputs.len());
        scaler.worker = Some(Arc::new(Mutex::new(dummy_worker)));

        let client = scaler.client.as_ref().unwrap();
        let client = client.clone();
        let inputs_clone = inputs.clone();
        let handle = tokio::spawn(async move {
            publish_data(client, inputs_clone, topic_name).await;
        });

        scaler.scale().await.unwrap();
        handle.await.unwrap();

        let mut results = scaler.worker.unwrap().lock().unwrap().data.take();
        assert_eq!(inputs.sort(), results.sort());
    }

    fn test_new_worker() {
        let worker = JobWorker::new();
        let msg = PubsubMessage {
            data: "abc".into(),
            // Set ordering_key if needed (https://cloud.google.com/pubsub/docs/ordering)
            ordering_key: "order".into(),
            ..Default::default()
        };
        worker.execute(Message::NewJob(msg));
    }
}
