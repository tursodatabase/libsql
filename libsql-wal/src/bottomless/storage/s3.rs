//! S3 implementation of storage

use std::{path::Path, pin::Pin};

use aws_config::SdkConfig;
use aws_sdk_s3::{primitives::ByteStream, Client};
use libsql_sys::name::NamespaceName;
use tokio::io::AsyncBufRead;

use super::{fs::RemoteStorage, SegmentMeta};
use crate::bottomless::Result;

pub struct S3Storage {
    client: Client,

    config: S3Config,
}

pub struct S3Config {
    bucket: String,
    aws_config: SdkConfig,
    cluster_id: String,
}

fn s3_folder_key(cluster_id: &str, ns: &NamespaceName) -> String {
    format!("ns-{}:{}-v2", cluster_id, ns)
}

impl RemoteStorage for S3Storage {
    type FetchStream = Pin<Box<dyn AsyncBufRead + Send>>;

    async fn upload(&self, file_path: &Path, key: &str, meta: &SegmentMeta) -> Result<()> {
        let folder_key = s3_folder_key(&self.config.cluster_id, &meta.namespace);
        let s3_key = format!("{}/segments/{}", folder_key, key);

        let stream = ByteStream::from_path(file_path).await.unwrap();

        self.client
            .put_object()
            .bucket(&self.config.bucket)
            .body(stream)
            .key(s3_key)
            .send()
            .await
            .unwrap();

        Ok(())
    }

    async fn fetch(
        &self,
        namespace: &NamespaceName,
        frame_no: u64,
        out_folder: &Path,
    ) -> Result<Self::FetchStream> {
        let folder_key = s3_folder_key(&self.config.cluster_id, &namespace);
        let s3_prefix = format!("{}/segments", folder_key);

        let mut objects = self
            .client
            .list_objects_v2()
            .bucket(&self.config.bucket)
            .prefix(s3_prefix)
            .max_keys(10)
            .into_paginator()
            .send();

        while let Some(result) = objects.next().await {
            for object in result.unwrap().contents() {
                let key = object.key().unwrap();
                let (start_frame_no, end_frame_no) = super::fs::parse_segment_file_name(&key)?;

                if start_frame_no <= frame_no && end_frame_no >= frame_no {
                    let out = self
                        .client
                        .get_object()
                        .bucket(&self.config.bucket)
                        .key(key)
                        .send()
                        .await
                        .unwrap();

                    let stream = out.body.into_async_read();
                    return Ok(Box::pin(stream));
                }
            }
        }

        todo!()
    }
}
