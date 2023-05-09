use cloudsql_autoscale::{
    Action, Authenticator, CloudSQLReplicator, JobStatus, JobWorker, Message, Scaler, Worker,
};
use google_cloud_default::WithAuthExt;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::ClientConfig;
use google_cloud_pubsub::topic::Topic;
use reqwest;
use reqwest::header::HeaderMap;
use reqwest::header::CONTENT_LENGTH;
use serde::Serialize;
use serde_json;
use serde_json::Value;
use std::cell::RefCell;
use std::dbg;
use std::env;
use std::fs::File;
use std::io::read_to_string;
use std::io::BufReader;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};

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
fn test_worker_todo() {
    let worker = JobWorker::new();

    let file = File::open("./data.json").unwrap();
    let reader = BufReader::new(file);
    let data = read_to_string(reader).unwrap();

    let action = worker.todo(data.as_str());
    assert_eq!(Some(Action::Add("")), action);
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
async fn test_get_sql_instance() {
    let auth = Authenticator::new();
    let token = auth.authenticate().await.unwrap();

    let project_id = env::var("PROJECT_ID").unwrap();
    let instance_name = env::var("INSTANCE_NAME").unwrap();
    let api_url = format!(
        "https://www.googleapis.com/sql/v1beta4/projects/{}/instances/{}",
        project_id, instance_name,
    );

    dbg!(&api_url);
    let client = reqwest::Client::new();
    let resp = client
        .get(api_url)
        .bearer_auth(token.replace("Bearer ", ""))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    dbg!(resp);
}

#[derive(Debug, Serialize)]
struct ReplicaSettings {
    tier: String,
    availabilityType: String,
    pricingPlan: String,
}

#[derive(Debug, Serialize)]
struct ReplicaInstance {
    name: String,
    instanceType: String,
    masterInstanceName: String,
    databaseVersion: String,
    region: String,
    settings: ReplicaSettings,
}

#[tokio::test]
async fn test_add_sql_instance_replica() {
    let auth = Authenticator::new();
    let token = auth.authenticate().await.unwrap();

    let project_id = env::var("PROJECT_ID").unwrap();
    let instance_name = env::var("INSTANCE_NAME").unwrap();

    let api_url = format!(
        "https://www.googleapis.com/sql/v1beta4/projects/{}/instances/{}",
        project_id, instance_name,
    );

    let client = reqwest::Client::new();
    let resp = client
        .get(api_url)
        .bearer_auth(token.replace("Bearer ", ""))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    let v: Value = serde_json::from_str(resp.as_str()).unwrap();

    dbg!(v["databaseVersion"].as_str().unwrap());

    let api_url = format!(
        "https://sqladmin.googleapis.com/sql/v1beta4/projects/{}/instances",
        project_id,
    );

    let replica_instance = ReplicaInstance {
        name: format!("{}-replica", v["name"].as_str().unwrap().to_string()),
        instanceType: "READ_REPLICA_INSTANCE".to_string(),
        masterInstanceName: v["name"].as_str().unwrap().to_string(),
        databaseVersion: v["databaseVersion"].as_str().unwrap().to_string(),
        region: v["region"].as_str().unwrap().to_string(),
        settings: ReplicaSettings {
            tier: v["settings"]["tier"].as_str().unwrap().to_string(),
            availabilityType: v["settings"]["availabilityType"].as_str().unwrap().to_string(),
            pricingPlan: v["settings"]["pricingPlan"].as_str().unwrap().to_string(),
        },
    };

    dbg!(&api_url);
    let client = reqwest::Client::new();
    let resp = client
        .post(api_url)
        .bearer_auth(token.replace("Bearer ", ""))
        // .headers(headers)
        .json(&replica_instance)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    dbg!(resp);
}

#[test]
fn test_decode_incident_data() {
    let file = File::open("./data.json").unwrap();
    let reader = BufReader::new(file);
    let v: Value = serde_json::from_reader(reader).unwrap();

    let comparison = &v["incident"]["condition"]["conditionThreshold"]["comparison"];
    let threshold_value = &v["incident"]["threshold_value"];
    let observed_value = &v["incident"]["observed_value"];

    assert_eq!("COMPARISON_GT", comparison);
    assert_eq!("0.09", threshold_value);
    assert_eq!("0.107", observed_value);
}
