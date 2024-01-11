use crate::replicator::{env_var, env_var_or};
use anyhow::anyhow;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
use aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::list_objects::builders::ListObjectsFluentBuilder;
use aws_sdk_s3::operation::list_objects::ListObjectsOutput;
use aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config};

/// S3 related options.
#[derive(Clone, Debug)]
pub struct S3Options {
    pub create_bucket_if_not_exists: bool,
    pub aws_endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub region: Option<String>,
    /// Bucket directory name where all S3 objects are backed up. General schema is:
    /// - `{db-name}-{uuid-v7}` subdirectories:
    ///   - `.meta` file with database page size and initial WAL checksum.
    ///   - Series of files `{first-frame-no}-{last-frame-no}.{compression-kind}` containing
    ///     the batches of frames from which the restore will be made.
    pub bucket_name: String,
    /// Maximum number of S3 file upload requests that may happen in parallel.
    pub s3_upload_max_parallelism: usize,
    /// Max number of retries for S3 operations
    pub s3_max_retries: u32,
}

impl S3Options {
    pub async fn client_config(&self) -> super::Result<Config> {
        let mut loader = aws_config::from_env();
        if let Some(endpoint) = self.aws_endpoint.as_deref() {
            loader = loader.endpoint_url(endpoint);
        }
        let region = self
            .region
            .clone()
            .ok_or(anyhow!("LIBSQL_BOTTOMLESS_AWS_DEFAULT_REGION was not set"))?;
        let access_key_id = self
            .access_key_id
            .clone()
            .ok_or(anyhow!("LIBSQL_BOTTOMLESS_AWS_ACCESS_KEY_ID was not set"))?;
        let secret_access_key = self.secret_access_key.clone().ok_or(anyhow!(
            "LIBSQL_BOTTOMLESS_AWS_SECRET_ACCESS_KEY was not set"
        ))?;
        let conf = aws_sdk_s3::config::Builder::from(&loader.load().await)
            .force_path_style(true)
            .region(Region::new(region))
            .credentials_provider(Credentials::new(
                access_key_id,
                secret_access_key,
                None,
                None,
                "Static",
            ))
            .retry_config(
                aws_sdk_s3::config::retry::RetryConfig::standard()
                    .with_max_attempts(self.s3_max_retries),
            )
            .build();
        Ok(conf)
    }

    pub fn from_env() -> super::Result<Self> {
        let aws_endpoint = env_var("LIBSQL_BOTTOMLESS_ENDPOINT").ok();
        let bucket_name = env_var_or("LIBSQL_BOTTOMLESS_BUCKET", "bottomless");
        let access_key_id = env_var("LIBSQL_BOTTOMLESS_AWS_ACCESS_KEY_ID").ok();
        let secret_access_key = env_var("LIBSQL_BOTTOMLESS_AWS_SECRET_ACCESS_KEY").ok();
        let region = env_var("LIBSQL_BOTTOMLESS_AWS_DEFAULT_REGION").ok();
        let s3_upload_max_parallelism =
            env_var_or("LIBSQL_BOTTOMLESS_S3_PARALLEL_MAX", 32).parse::<usize>()?;
        let s3_max_retries = env_var_or("LIBSQL_BOTTOMLESS_S3_MAX_RETRIES", 10).parse::<u32>()?;

        Ok(Self {
            create_bucket_if_not_exists: true,
            s3_upload_max_parallelism,
            aws_endpoint,
            access_key_id,
            secret_access_key,
            region,
            bucket_name,
            s3_max_retries,
        })
    }
}

/// S3 client.
#[derive(Clone, Debug)]
pub struct S3Client {
    client: Client,
    options: S3Options,
}

impl S3Client {
    /// Create a new s3 client from [`S3Options`]
    pub async fn from_options(options: &S3Options) -> super::Result<Self> {
        let client = Client::from_conf(options.client_config().await?);
        let client = Self {
            client,
            options: options.clone(),
        };
        Ok(client)
    }

    /// Get the bucket name from S3Client.
    pub fn bucket(&self) -> &str {
        &self.options.bucket_name
    }

    /// Get the options from S3Client.
    pub fn options(&self) -> &S3Options {
        &self.options
    }

    /// Check if bucket exist, return Ok(()) if exist. If bucket does not exist, create it
    /// if `create_bucket_if_not_exists` is set to true. Otherwise return an error.
    pub async fn check_bucket(&self, error_on_not_exist: bool) -> super::Result<()> {
        let bucket = self.options.bucket_name.clone();
        match self.client.head_bucket().bucket(&bucket).send().await {
            Ok(_) => tracing::info!("Bucket {} exists and is accessible", bucket),
            Err(SdkError::ServiceError(err)) if !error_on_not_exist && err.err().is_not_found() => {
                if self.options.create_bucket_if_not_exists {
                    tracing::info!("Bucket {} not found, recreating", bucket);
                    self.client.create_bucket().bucket(&bucket).send().await?;
                } else {
                    tracing::error!("Bucket {} does not exist", bucket);
                    return Err(SdkError::ServiceError(err).into());
                }
            }
            Err(e) => {
                tracing::error!("Bucket checking error: {}", e);
                return Err(e.into());
            }
        }

        Ok(())
    }

    // Lists objects from the current bucket
    pub async fn list_objects(&self, prefix: &str) -> super::Result<ListObjectsOutput> {
        let bucket = self.options.bucket_name.clone();
        let response = self
            .client
            .list_objects()
            .bucket(&bucket)
            .prefix(prefix)
            .send()
            .await?;
        Ok(response)
    }

    // Lists objects from the current bucket
    pub fn list_objects_builder(&self) -> ListObjectsFluentBuilder {
        let bucket = self.options.bucket_name.clone();
        self.client.list_objects().bucket(&bucket)
    }

    // Gets an object from the current bucket
    pub async fn get_object(&self, key: &str) -> super::Result<GetObjectOutput> {
        let bucket = self.options.bucket_name.clone();
        let response = self
            .client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await?;
        Ok(response)
    }

    // Gets an object from the current bucket
    pub fn get_object_builder(&self) -> GetObjectFluentBuilder {
        let bucket = self.options.bucket_name.clone();
        self.client.get_object().bucket(&bucket)
    }

    pub async fn put_object(&self, key: &str, body: ByteStream) -> super::Result<PutObjectOutput> {
        let bucket = self.options.bucket_name.clone();
        let response = self
            .client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(body)
            .send()
            .await?;
        Ok(response)
    }

    pub fn put_object_builder(&self) -> PutObjectFluentBuilder {
        let bucket = self.options.bucket_name.clone();
        self.client.put_object().bucket(&bucket)
    }

    pub async fn delete_object(&self, key: &str) -> super::Result<DeleteObjectOutput> {
        let bucket = self.options.bucket_name.clone();
        let response = self
            .client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await?;
        Ok(response)
    }
}
