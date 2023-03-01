use std::time::Duration;

use octopod::App;
use serde_json::json;

const DUMP_DATA: &[u8] = include_bytes!("../assets/simple_dump.sql");
#[octopod::test(app = "simple-cluster")]
async fn load_dump_from_primary(app: App) {
    let primary_ip = app.service("primary").unwrap().ip().await.unwrap();
    let url = format!("http://{primary_ip}:8080/load-dump");
    let client = reqwest::Client::new();
    let resp = client.post(url).body(DUMP_DATA).send().await.unwrap();
    assert!(resp.status().is_success(), "{}", resp.text().await.unwrap());

    // wait for dump to be loaded and replicated
    tokio::time::sleep(Duration::from_secs(3)).await;

    let url = format!("http://{primary_ip}:8080");
    let resp = client
        .post(url)
        .json(&json!({
            "statements": ["select * from person", "select * from pets"]
        }))
        .send()
        .await
        .unwrap();

    insta::assert_json_snapshot!(resp.json::<serde_json::Value>().await.unwrap());

    // ensure replica is up to date
    let replica_id = app.service("replica").unwrap().ip().await.unwrap();
    let url = format!("http://{replica_id}:8080");
    let resp = client
        .post(url)
        .json(&json!({
            "statements": ["select * from person", "select * from pets"]
        }))
        .send()
        .await
        .unwrap();
    insta::assert_json_snapshot!(resp.json::<serde_json::Value>().await.unwrap());
}

// ignored: https://github.com/libsql/sqld/issues/197
#[octopod::test(app = "simple-cluster", ignore)]
async fn load_dump_from_replica(app: App) {
    let replica_ip = app.service("replica").unwrap().ip().await.unwrap();
    let url = format!("http://{replica_ip}:8080/load-dump");
    let client = reqwest::Client::new();
    let resp = client.post(url).body(DUMP_DATA).send().await.unwrap();
    assert!(resp.status().is_success(), "{}", resp.text().await.unwrap());

    // wait for dump to be loaded and replicated
    tokio::time::sleep(Duration::from_secs(4)).await;

    let url = format!("http://{replica_ip}:8080");
    let resp = client
        .post(url)
        .json(&json!({
            "statements": ["select * from person", "select * from pets"]
        }))
        .send()
        .await
        .unwrap();

    insta::assert_json_snapshot!(resp.json::<serde_json::Value>().await.unwrap());
}

#[octopod::test(app = "simple-cluster")]
async fn load_large_dump(app: App) {
    let primary_ip = app.service("primary").unwrap().ip().await.unwrap();
    let url = format!("http://{primary_ip}:8080/load-dump");
    let client = reqwest::Client::new();

    let dump = std::iter::once("CREATE TABLE test (who);\n")
        .chain(std::iter::repeat("INSERT INTO test VALUES ('hello world');\n").take(10000))
        .fold(String::new(), |acc, s| acc + s);

    let resp = client.post(url).body(dump).send().await.unwrap();
    assert!(resp.status().is_success(), "{}", resp.text().await.unwrap());

    // wait for dump to be loaded and replicated
    tokio::time::sleep(Duration::from_secs(3)).await;

    let url = format!("http://{primary_ip}:8080");
    let resp = client
        .post(url)
        .json(&json!({
            "statements": ["select count(*) from test"]
        }))
        .send()
        .await
        .unwrap();

    insta::assert_json_snapshot!(resp.json::<serde_json::Value>().await.unwrap());

    // ensure replica is up to date
    let replica_id = app.service("replica").unwrap().ip().await.unwrap();
    let url = format!("http://{replica_id}:8080");
    let resp = client
        .post(url)
        .json(&json!({
            "statements": ["select count(*) from test"]
        }))
        .send()
        .await
        .unwrap();
    insta::assert_json_snapshot!(resp.json::<serde_json::Value>().await.unwrap());
}
