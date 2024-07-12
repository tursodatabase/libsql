//! S3 implementation of storage

use std::{path::Path, pin::Pin};

use aws_config::SdkConfig;
use aws_sdk_s3::{primitives::ByteStream, types::CreateBucketConfiguration, Client};
use libsql_sys::name::NamespaceName;
use tokio::io::AsyncBufRead;

use super::{fs::RemoteStorage, SegmentMeta};
use crate::storage::{Error, Result};

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

impl S3Storage {
    pub(crate) async fn from_sdk_config(
        aws_config: SdkConfig,
        bucket: String,
        cluster_id: String,
    ) -> Result<Self> {
        let config = S3Config {
            bucket,
            cluster_id,
            aws_config,
        };

        let client = Client::new(&config.aws_config);

        let bucket_config = CreateBucketConfiguration::builder()
            .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::UsWest2)
            .build();
        client
            .create_bucket()
            .create_bucket_configuration(bucket_config)
            .bucket(&config.bucket)
            .send()
            .await
            .unwrap();

        Ok(Self { client, config })
    }
}

impl RemoteStorage for S3Storage {
    type FetchStream = Pin<Box<dyn AsyncBufRead + Send>>;

    async fn upload(&self, file_path: &Path, meta: &SegmentMeta) -> Result<()> {
        let folder_key = s3_folder_key(&self.config.cluster_id, &meta.namespace);
        let key = super::fs::generate_key(&meta);

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
    ) -> Result<(String, Self::FetchStream)> {
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

                let file_name = key.split("/").last().unwrap();

                let (start_frame_no, end_frame_no) =
                    super::fs::parse_segment_file_name(&file_name)?;

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
                    return Ok((file_name.to_string(), Box::pin(stream)));
                }
            }
        }

        Err(Error::FrameNotFound(frame_no))
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use aws_config::{BehaviorVersion, Region, SdkConfig};
    use aws_sdk_s3::config::SharedCredentialsProvider;
    use chrono::Utc;
    use s3s::{
        auth::SimpleAuth,
        service::{S3ServiceBuilder, SharedS3Service},
    };
    use uuid::Uuid;

    use super::*;

    #[track_caller]
    fn setup(dir: impl AsRef<Path>) -> (SdkConfig, SharedS3Service) {
        std::fs::create_dir_all(&dir).unwrap();
        let s3_impl = s3s_fs::FileSystem::new(dir).unwrap();

        let cred = aws_credential_types::Credentials::for_tests();

        let mut s3 = S3ServiceBuilder::new(s3_impl);
        s3.set_auth(SimpleAuth::from_single(
            cred.access_key_id(),
            cred.secret_access_key(),
        ));
        s3.set_base_domain("localhost:8014");
        let s3 = s3.build().into_shared();

        let client = s3s_aws::Client::from(s3.clone());

        let config = aws_config::SdkConfig::builder()
            .http_client(client)
            .behavior_version(BehaviorVersion::latest())
            .region(Region::from_static("us-west-2"))
            .credentials_provider(SharedCredentialsProvider::new(cred))
            .endpoint_url("http://localhost:8014")
            .build();

        (config, s3)
    }

    #[tokio::test]
    async fn basic() {
        let _ = tracing_subscriber::fmt::try_init();
        let dir = tempfile::tempdir().unwrap();
        let (aws_config, _s3) = setup(&dir);

        let storage =
            S3Storage::from_sdk_config(aws_config, "testbucket".into(), "123456789".into())
                .await
                .unwrap();

        let f_path = dir.path().join("fs-segments");
        std::fs::write(&f_path, vec![0; 8092]).unwrap();

        let ns = NamespaceName::from_string("foobarbaz".into());

        storage
            .upload(
                &f_path,
                &SegmentMeta {
                    namespace: ns.clone(),
                    segment_id: Uuid::new_v4(),
                    start_frame_no: 0u64.into(),
                    end_frame_no: 64u64.into(),
                    created_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        storage.fetch(&ns, 1).await.unwrap();

        assert!(storage.fetch(&ns, 65).await.is_err());
    }
}
