use std::time::Duration;

use octopod::App;
use serde_json::json;

#[octopod::test(app = "simple-cluster")]
async fn proxy_write(app: App) {
    let replica_ip = app.service("replica").unwrap().ip().await.unwrap();
    let primary_ip = app.service("primary").unwrap().ip().await.unwrap();
    let primary_url = format!("http://{primary_ip}:8080/");
    let replica_url = format!("http://{replica_ip}:8080/");
    let client = reqwest::Client::new();

    // perform a write to the writer and ensure it's proxied to to primary
    let payload =
        json!({ "statements": ["create table test (x)", "insert into test values (123)"] });
    let resp = client
        .post(&replica_url)
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    insta::assert_json_snapshot!(json);

    // read from primary to ensure it got the write
    let payload = json!({ "statements": ["select * from test"] });
    let resp = client
        .post(&primary_url)
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    insta::assert_json_snapshot!(json);

    // wait for replication
    tokio::time::sleep(Duration::from_secs(2)).await;

    // read from replica to ensure replication
    let resp = client
        .post(&replica_url)
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    insta::assert_json_snapshot!(json);
}

#[octopod::test(app = "simple-cluster")]
async fn replica_catch_up(app: App) {
    let replica = app.service("replica").unwrap();
    replica.pause().await.unwrap();
    let primary_ip = app.service("primary").unwrap().ip().await.unwrap();
    let primary_url = format!("http://{primary_ip}:8080/");
    let client = reqwest::Client::new();

    let payload = json!({ "statements": ["create table test (x)"] });
    let resp = client
        .post(&primary_url)
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    // insert a few entries
    for i in 0..100 {
        let payload = json!({ "statements": [format!("insert into test values (\"value{i}\")")] });
        let resp = client
            .post(&primary_url)
            .json(&payload)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    // check that everything is there
    let payload = json!({ "statements": [format!("select * from test")] });
    let resp = client
        .post(&primary_url)
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    insta::assert_json_snapshot!(json);

    // bring back replica to the network
    replica.unpause().await.unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;

    let replica_ip = replica.ip().await.unwrap();
    let replica_url = format!("http://{replica_ip}:8080/");
    // check that everything is there
    let payload = json!({ "statements": [format!("select * from test")] });
    let resp = client
        .post(&replica_url)
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    insta::assert_json_snapshot!(json);
}
