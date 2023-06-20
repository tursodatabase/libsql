use anyhow::Result;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::types::ObjectAttributes;
use aws_sdk_s3::Client;
use aws_smithy_types::date_time::Format;

pub(crate) struct Replicator {
    inner: bottomless::replicator::Replicator,
}

impl std::ops::Deref for Replicator {
    type Target = bottomless::replicator::Replicator;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for Replicator {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

fn uuid_to_datetime(uuid: &uuid::Uuid) -> chrono::NaiveDateTime {
    let (seconds, nanos) = uuid
        .get_timestamp()
        .map(|ts| ts.to_unix())
        .unwrap_or((0, 0));
    let (seconds, nanos) = (253370761200 - seconds, 999000000 - nanos);
    chrono::NaiveDateTime::from_timestamp_opt(seconds as i64, nanos)
        .unwrap_or(chrono::NaiveDateTime::MIN)
}

pub(crate) async fn detect_db(client: &Client, bucket: &str, db_name: &str) -> Option<String> {
    let response = match client
        .list_objects()
        .bucket(bucket)
        .set_delimiter(Some("/".to_string()))
        .prefix(db_name)
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(_) => return None,
    };

    let prefix = response.common_prefixes()?.first()?.prefix()?;
    // 38 is the length of the uuid part
    if let Some('-') = prefix.chars().nth(prefix.len().saturating_sub(38)) {
        Some(prefix[..prefix.len().saturating_sub(38)].to_owned())
    } else {
        None
    }
}

impl Replicator {
    pub async fn new(db: String) -> Result<Self> {
        let inner = bottomless::replicator::Replicator::new(db).await?;
        Ok(Replicator { inner })
    }

    pub(crate) async fn print_snapshot_summary(&self, generation: &uuid::Uuid) -> Result<()> {
        match self
            .client
            .get_object_attributes()
            .bucket(&self.bucket)
            .key(format!("{}-{}/db.gz", self.db_name, generation))
            .object_attributes(ObjectAttributes::ObjectSize)
            .send()
            .await
        {
            Ok(attrs) => {
                println!("\tmain database snapshot:");
                println!("\t\tobject size:   {}", attrs.object_size());
                println!(
                    "\t\tlast modified: {}",
                    attrs
                        .last_modified()
                        .map(|s| s.fmt(Format::DateTime).unwrap_or_else(|e| e.to_string()))
                        .as_deref()
                        .unwrap_or("never")
                );
            }
            Err(SdkError::ServiceError(err)) if err.err().is_no_such_key() => {
                println!("\tno main database snapshot file found")
            }
            Err(e) => println!("\tfailed to fetch main database snapshot info: {e}"),
        };
        Ok(())
    }

    pub(crate) async fn list_generations(
        &self,
        limit: Option<u64>,
        older_than: Option<chrono::NaiveDate>,
        newer_than: Option<chrono::NaiveDate>,
        verbose: bool,
    ) -> Result<()> {
        let mut next_marker = None;
        let mut limit = limit.unwrap_or(u64::MAX);
        loop {
            let mut list_request = self
                .client
                .list_objects()
                .bucket(&self.bucket)
                .set_delimiter(Some("/".to_string()))
                .prefix(&self.db_name);

            if let Some(marker) = next_marker {
                list_request = list_request.marker(marker)
            }

            if verbose {
                println!("Database {}:", self.db_name);
            }

            let response = list_request.send().await?;
            let prefixes = match response.common_prefixes() {
                Some(prefixes) => prefixes,
                None => {
                    println!("No generations found");
                    return Ok(());
                }
            };

            for prefix in prefixes {
                if let Some(prefix) = &prefix.prefix {
                    let prefix = &prefix[self.db_name.len() + 1..prefix.len() - 1];
                    let uuid = uuid::Uuid::try_parse(prefix)?;
                    let datetime = uuid_to_datetime(&uuid);
                    if datetime.date() < newer_than.unwrap_or(chrono::NaiveDate::MIN) {
                        continue;
                    }
                    if datetime.date() > older_than.unwrap_or(chrono::NaiveDate::MAX) {
                        continue;
                    }
                    println!("{uuid}");
                    if verbose {
                        let counter = self.get_remote_change_counter(&uuid).await?;
                        let consistent_frame = self.get_last_consistent_frame(&uuid).await?;
                        let m = self.get_metadata(&uuid).await?;
                        println!("\tcreated at (UTC):     {datetime}");
                        println!("\tchange counter:       {counter:?}");
                        println!("\tconsistent WAL frame: {consistent_frame}");
                        if let Some((page_size, checksum)) = m {
                            println!("\tpage size:            {page_size}");
                            println!("\tWAL frame checksum:   {checksum:x}");
                        }
                        self.print_snapshot_summary(&uuid).await?;
                        println!()
                    }
                }
                limit -= 1;
                if limit == 0 {
                    return Ok(());
                }
            }

            next_marker = response.next_marker().map(|s| s.to_owned());
            if next_marker.is_none() {
                return Ok(());
            }
        }
    }

    pub(crate) async fn remove(&self, generation: uuid::Uuid, verbose: bool) -> Result<()> {
        let mut removed = 0;
        let mut next_marker = None;
        loop {
            let mut list_request = self
                .client
                .list_objects()
                .bucket(&self.bucket)
                .prefix(format!("{}-{}/", &self.db_name, generation));

            if let Some(marker) = next_marker {
                list_request = list_request.marker(marker)
            }

            let response = list_request.send().await?;
            let objs = match response.contents() {
                Some(prefixes) => prefixes,
                None => {
                    if verbose {
                        println!("No objects found")
                    }
                    return Ok(());
                }
            };

            for obj in objs {
                if let Some(key) = obj.key() {
                    if verbose {
                        println!("Removing {key}")
                    }
                    self.client
                        .delete_object()
                        .bucket(&self.bucket)
                        .key(key)
                        .send()
                        .await?;
                    removed += 1;
                }
            }

            next_marker = response.next_marker().map(|s| s.to_owned());
            if next_marker.is_none() {
                if verbose {
                    println!("Removed {removed} snapshot generations");
                }
                return Ok(());
            }
        }
    }

    pub(crate) async fn remove_many(
        &self,
        older_than: chrono::NaiveDate,
        verbose: bool,
    ) -> Result<()> {
        let mut next_marker = None;
        let mut removed_count = 0;
        loop {
            let mut list_request = self
                .client
                .list_objects()
                .bucket(&self.bucket)
                .set_delimiter(Some("/".to_string()))
                .prefix(&self.db_name);

            if let Some(marker) = next_marker {
                list_request = list_request.marker(marker)
            }

            let response = list_request.send().await?;
            let prefixes = match response.common_prefixes() {
                Some(prefixes) => prefixes,
                None => {
                    if verbose {
                        println!("No generations found")
                    }
                    return Ok(());
                }
            };

            for prefix in prefixes {
                if let Some(prefix) = &prefix.prefix {
                    let prefix = &prefix[self.db_name.len() + 1..prefix.len() - 1];
                    let uuid = uuid::Uuid::try_parse(prefix)?;
                    let datetime = uuid_to_datetime(&uuid);
                    if datetime.date() >= older_than {
                        continue;
                    }
                    if verbose {
                        println!("Removing {uuid}");
                    }
                    self.remove(uuid, verbose).await?;
                    removed_count += 1;
                }
            }

            next_marker = response.next_marker().map(|s| s.to_owned());
            if next_marker.is_none() {
                break;
            }
        }
        if verbose {
            println!("Removed {removed_count} generations");
        }
        Ok(())
    }

    pub(crate) async fn list_generation(&self, generation: uuid::Uuid) -> Result<()> {
        self.client
            .list_objects()
            .bucket(&self.bucket)
            .prefix(format!("{}-{}/", &self.db_name, generation))
            .max_keys(1)
            .send()
            .await?
            .contents()
            .ok_or_else(|| {
                anyhow::anyhow!("Generation {} not found for {}", generation, &self.db_name)
            })?;

        let counter = self.get_remote_change_counter(&generation).await?;
        let consistent_frame = self.get_last_consistent_frame(&generation).await?;
        let meta = self.get_metadata(&generation).await?;
        println!("Generation {} for {}", generation, self.db_name);
        println!("\tcreated at:           {}", uuid_to_datetime(&generation));
        println!("\tchange counter:       {counter:?}");
        println!("\tconsistent WAL frame: {consistent_frame}");
        if let Some((page_size, checksum)) = meta {
            println!("\tpage size:            {page_size}");
            println!("\tWAL frame checksum:   {checksum:x}");
        }
        self.print_snapshot_summary(&generation).await?;
        Ok(())
    }
}
