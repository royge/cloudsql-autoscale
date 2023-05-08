use futures_util::StreamExt;
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
use serde_json;
use serde_json::Value;
use std::cell::RefCell;
use std::error::Error;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};

pub struct Scaler<W> {
    pub worker: Option<Arc<Mutex<W>>>,
    pub topic: Option<Topic>,
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

    pub fn todo(&self, job: &str) -> Option<Action> {
        if let Ok(v) = serde_json::from_str::<Value>(job) {
            let comparison = &v["incident"]["condition"]["conditionThreshold"]["comparison"];
            if comparison == "COMPARISON_GT" {
                return Some(Action::Add(""));
            } else {
                return Some(Action::Remove(""));
            }
        }
        None
    }
}

impl Worker for JobWorker {
    fn execute(&self, job: Message) -> JobStatus {
        match job {
            Message::NewJob(job) => {
                // Handle data.
                let data = from_utf8(&job.data).unwrap();
                let action = self.todo(data);
                match action {
                    Some(Action::Add(name)) => {
                        let _ = self.replicator.as_ref().unwrap().add_read_replica(name);
                    },
                    Some(Action::Remove(name)) => {
                        let _ = self.replicator.as_ref().unwrap().remove_read_replica(name);
                    },
                    None => {}
                }
            }
        }

        JobStatus::Continue
    }
}

#[derive(Debug, PartialEq)]
pub enum Action<'a> {
    Add(&'a str),
    Remove(&'a str),
}

pub enum Message {
    NewJob(PubsubMessage),
}

pub enum JobStatus {
    Continue,
    Stop,
}

pub trait Replicator {
    fn add_read_replica(&self, name: &str) -> Result<(), Box<dyn Error>>;
    fn remove_read_replica(&self, name: &str) -> Result<(), Box<dyn Error>>;
}

pub struct CloudSQLReplicator {
    client: reqwest::Client,
}

impl CloudSQLReplicator {
    pub fn new() -> CloudSQLReplicator {
        let client = reqwest::Client::new();

        CloudSQLReplicator { client }
    }
}

impl Replicator for CloudSQLReplicator {
    fn add_read_replica(&self, name: &str) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn remove_read_replica(&self, name: &str) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

pub struct Authenticator {}

impl Authenticator {
    pub fn new() -> Authenticator {
        Authenticator {}
    }

    pub async fn authenticate(&self) -> Result<String, Box<dyn Error>> {
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

        match ts.token().await {
            Ok(token) => {
                return Ok(token);
            }
            Err(error) => {
                return Err(error);
            }
        }
    }
}
