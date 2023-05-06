use cloudsql_autoscale::{JobWorker, Scaler};
use google_cloud_default::WithAuthExt;
use google_cloud_pubsub::client::ClientConfig;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;
use std::env;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let topic_name = env::var("SQL_AUTOSCALE_TOPIC").unwrap();
        let config = ClientConfig::default().with_auth().await.unwrap();

        let scaler = Scaler::<JobWorker>::new(config, topic_name.as_str());
        let mut scaler = scaler.await.unwrap();
        scaler.worker = Some(Arc::new(Mutex::new(JobWorker::new())));
        scaler.scale().await.unwrap();
    })
}
