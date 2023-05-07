use colored::Colorize;
use futures_util::StreamExt;
use google_cloud_auth::error::Error;
use google_cloud_auth::project::Config;
use google_cloud_auth::token::DefaultTokenSourceProvider;
use google_cloud_gax::grpc::Status;
use google_cloud_gax::retry::RetrySetting;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscription::SubscriptionConfig;
use google_cloud_pubsub::topic::Topic;
use google_cloud_token::TokenSourceProvider;
use reqwest;
use serde_json::to_string_pretty;
use std::cell::RefCell;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};

pub struct Scaler<W> {
    pub worker: Option<Arc<Mutex<W>>>,
    topic: Option<Topic>,
    artifact_store: Arc<Mutex<RefCell<Vec<String>>>>,
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
            worker: None,
            topic: Some(topic),
            artifact_store: Arc::new(Mutex::new(RefCell::new(vec![]))),
        })
    }

    pub async fn scale(&self) -> Result<(), Status> {
        let subscriptions = self
            .topic
            .as_ref()
            .unwrap()
            .subscriptions(Some(RetrySetting::default()));

        // Only listen to first subscriber.
        for subscription in subscriptions.await.unwrap() {
            // Read the messages as a stream
            // Note: This blocks the current thread but helps working with non clonable data
            let mut stream = subscription.subscribe(None).await?;
            while let Some(message) = stream.next().await {
                // Ack or Nack message.
                let _ = message.ack().await;

                let msg = message.message.clone();
                let msg_id = msg.message_id;

                if self
                    .artifact_store
                    .lock()
                    .unwrap()
                    .borrow()
                    .contains(&msg_id)
                {
                    continue;
                }

                let status = self
                    .worker
                    .as_ref()
                    .expect("No job worker defined!")
                    .lock()
                    .unwrap()
                    .execute(Message::NewJob(message.message));

                self.artifact_store
                    .lock()
                    .unwrap()
                    .borrow_mut()
                    .push(msg_id);

                match status {
                    JobStatus::Stop => break,
                    JobStatus::Continue => {}
                }
            }
        }
        Ok(())
    }
}

pub trait Worker {
    fn execute(&self, job: Message) -> JobStatus;
}

pub struct JobWorker {
    replicator: Option<Box<dyn Replicator>>,
}

impl JobWorker {
    pub fn new() -> JobWorker {
        JobWorker { replicator: None }
    }
}

impl Worker for JobWorker {
    fn execute(&self, job: Message) -> JobStatus {
        match job {
            Message::NewJob(job) => {
                // Handle data.
                let data = from_utf8(&job.data).unwrap().to_string();
                let data = to_string_pretty(&data).unwrap();
                println!("{}", data.green());
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

pub trait Replicator {
    fn add_read_replica(&self) -> Result<(), String>;
    fn remove_read_replica(&self) -> Result<(), String>;
}

pub struct CloudSQLReplicator {
    client: reqwest::Client,
}

impl CloudSQLReplicator {
    fn new() -> CloudSQLReplicator {
        let client = reqwest::Client::new();

        CloudSQLReplicator { client }
    }
}

impl Replicator for CloudSQLReplicator {
    fn add_read_replica(&self) -> Result<(), String> {
        Ok(())
    }

    fn remove_read_replica(&self) -> Result<(), String> {
        Ok(())
    }
}

pub struct Authenticator {}

impl Authenticator {
    fn new() -> Authenticator {
        Authenticator {}
    }

    async fn authenticate(&self) -> Result<String, Error> {
        let audience = "https://sqladmin.googleapis.com/";
        let scopes = [
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/sqlservice.admin",
        ];
        let config = Config {
            // audience is required only for service account jwt-auth
            // https://developers.google.com/identity/protocols/oauth2/service-account#jwt-auth
            audience: Some(audience),
            // scopes is required only for service account Oauth2
            // https://developers.google.com/identity/protocols/oauth2/service-account
            scopes: Some(&scopes),
        };
        let tp = DefaultTokenSourceProvider::new(config).await?;
        let ts = tp.token_source();

        Ok(ts.token().await.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_default::WithAuthExt;
    use reqwest;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::dbg;
    use std::env;
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

    async fn publish_test_data(topic: &Topic, inputs: Vec<String>) {
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

        let topic = scaler.topic.as_ref().unwrap().clone();
        let inputs_clone = inputs.clone();
        let handle = tokio::spawn(async move {
            publish_test_data(&topic, inputs_clone).await;
        });

        scaler.scale().await.unwrap();
        handle.await.unwrap();

        let mut results = scaler.worker.unwrap().lock().unwrap().data.take();
        assert_eq!(inputs.sort(), results.sort());
    }

    #[test]
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

    #[test]
    fn test_new_cloud_sql_replicator() {
        let _ = CloudSQLReplicator::new();
    }

    #[tokio::test]
    async fn test_authenticator_authenticate() {
        let auth = Authenticator::new();
        let token = auth.authenticate().await.unwrap();
        assert_ne!("", token);
    }

    #[tokio::test]
    async fn test_get_sql_instances() {
        let auth = Authenticator::new();
        let token = auth.authenticate().await.unwrap();

        let project_id = env::var("PROJECT_ID").unwrap();
        let api_url = format!(
            "https://www.googleapis.com/sql/v1beta4/projects/{}/instances",
            project_id
        );

        dbg!(&api_url);
        let client = reqwest::Client::new();
        let resp = client
            .get(api_url)
            .bearer_auth(token.replace("Bearer ", ""))
            .send().await
            .unwrap()
            .text().await
            .unwrap();
        dbg!(resp);
    }
}
