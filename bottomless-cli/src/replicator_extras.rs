use anyhow::Result;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::types::ObjectAttributes;
use aws_sdk_s3::Client;
use aws_smithy_types::date_time::Format;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use tokio::io::BufReader;

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
    let timestamp = bottomless::replicator::Replicator::generation_to_timestamp(uuid);
    let (seconds, _) = timestamp
        .as_ref()
        .map(uuid::Timestamp::to_unix)
        .unwrap_or_default();
    chrono::DateTime::from_timestamp_millis((seconds * 1000) as i64)
        .unwrap()
        .naive_utc()
}

pub(crate) async fn detect_db(client: &Client, bucket: &str, namespace: &str) -> Option<String> {
    let namespace = namespace.to_owned() + ":";
    let response = client
        .list_objects()
        .bucket(bucket)
        .set_delimiter(Some("/".to_string()))
        .prefix(namespace.clone())
        .send()
        .await
        .ok()?;

    let prefix = response.common_prefixes().first()?.prefix()?;
    // 38 is the length of the uuid part
    if let Some('-') = prefix.chars().nth(prefix.len().saturating_sub(38)) {
        let ns_db = &prefix[..prefix.len().saturating_sub(38)];
        Some(ns_db.strip_prefix(&namespace).unwrap_or(ns_db).to_owned())
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
                println!("\t\tobject size:   {}", attrs.object_size().unwrap());
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
            let prefixes = response.common_prefixes();

            if prefixes.is_empty() {
                println!("No generations found");
                return Ok(());
            }

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
                    println!("{} (created: {})", uuid, datetime.and_utc().to_rfc3339());
                    if verbose {
                        let counter = self.get_remote_change_counter(&uuid).await?;
                        let consistent_frame = self.get_last_consistent_frame(&uuid).await?;
                        let m = self.get_metadata(&uuid).await?;
                        let parent = self.get_dependency(&uuid).await?;
                        println!("\tcreated at (UTC):     {datetime}");
                        println!("\tchange counter:       {counter:?}");
                        println!("\tconsistent WAL frame: {consistent_frame}");
                        if let Some((page_size, crc)) = m {
                            println!("\tpage size:            {}", page_size);
                            println!("\tWAL frame checksum:   {:X}-{:X}", crc.0, crc.1);
                        }
                        if let Some(prev_gen) = parent {
                            println!("\tprevious generation:  {}", prev_gen);
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

    pub(crate) async fn remove_many(&self, older_than: NaiveDate, verbose: bool) -> Result<()> {
        let older_than = NaiveDateTime::new(older_than, NaiveTime::MIN);
        let delete_all = self.inner.delete_all(Some(older_than)).await?;
        if verbose {
            println!("Tombstoned {} at {}", self.inner.db_path, older_than);
        }
        let removed_generations = delete_all.commit().await?;
        if verbose {
            println!(
                "Removed {} generations of {} up to {}",
                removed_generations, self.inner.db_path, older_than
            );
        }
        Ok(())
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
            let objs = response.contents();

            if objs.is_empty() {
                if verbose {
                    println!("No objects found")
                }
                return Ok(());
            }

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

    pub(crate) async fn list_generation(&self, generation: uuid::Uuid) -> Result<()> {
        let res = self
            .client
            .list_objects()
            .bucket(&self.bucket)
            .prefix(format!("{}-{}/", &self.db_name, generation))
            .max_keys(1)
            .send()
            .await?;

        if res.contents().is_empty() {
            anyhow::bail!("Generation {} not found for {}", generation, &self.db_name)
        }

        let counter = self.get_remote_change_counter(&generation).await?;
        let consistent_frame = self.get_last_consistent_frame(&generation).await?;
        let meta = self.get_metadata(&generation).await?;
        let dep = self.get_dependency(&generation).await?;
        println!("Generation {} for {}", generation, self.db_name);
        println!("\tcreated at:           {}", uuid_to_datetime(&generation));
        println!("\tchange counter:       {counter:?}");
        println!("\tconsistent WAL frame: {consistent_frame}");
        if let Some((page_size, crc)) = meta {
            println!("\tpage size:            {}", page_size);
            println!("\tWAL frame checksum:   {:X}-{:X}", crc.0, crc.1);
        }
        if let Some(prev_gen) = dep {
            println!("\tprevious generation:  {}", prev_gen);
        }
        self.print_snapshot_summary(&generation).await?;
        Ok(())
    }

    pub async fn restore_from_local_snapshot(
        from_dir: impl AsRef<std::path::Path>,
        db: &mut tokio::fs::File,
    ) -> Result<bool> {
        let from_dir = from_dir.as_ref();
        use bottomless::replicator::CompressionKind;
        use tokio::io::AsyncWriteExt;

        let algos_to_try = &[
            CompressionKind::Gzip,
            CompressionKind::Zstd,
            CompressionKind::None,
        ];

        for algo in algos_to_try {
            let main_db_path = match algo {
                CompressionKind::None => from_dir.join("db.db"),
                CompressionKind::Gzip => from_dir.join("db.gz"),
                CompressionKind::Zstd => from_dir.join("db.zstd"),
            };
            if let Ok(mut db_file) = tokio::fs::File::open(&main_db_path).await {
                let db_size = match algo {
                    CompressionKind::None => tokio::io::copy(&mut db_file, db).await?,
                    CompressionKind::Gzip => {
                        let mut decompress_reader =
                            async_compression::tokio::bufread::GzipDecoder::new(
                                tokio::io::BufReader::new(db_file),
                            );
                        tokio::io::copy(&mut decompress_reader, db).await?
                    }
                    CompressionKind::Zstd => {
                        let mut decompress_reader =
                            async_compression::tokio::bufread::ZstdDecoder::new(
                                tokio::io::BufReader::new(db_file),
                            );
                        tokio::io::copy(&mut decompress_reader, db).await?
                    }
                };
                db.flush().await?;

                tracing::info!("Restored the main database file ({} bytes)", db_size);
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn apply_wal_from_local_generation(
        from_dir: impl AsRef<std::path::Path>,
        db: &mut tokio::fs::File,
        page_size: u32,
        checksum: (u32, u32),
    ) -> Result<u32> {
        use bottomless::transaction_cache::TransactionPageCache;
        use tokio::io::AsyncWriteExt;

        const SWAP_AFTER: u32 = 65536;
        const TMP_RESTORE_DIR: &str = ".bottomless.restore.tmp";

        let from_dir = from_dir.as_ref();
        let mut page_buf = {
            let mut v = Vec::with_capacity(page_size as usize);
            v.spare_capacity_mut();
            unsafe { v.set_len(page_size as usize) };
            v
        };

        let objs = {
            let mut objs = Vec::new();
            let mut dir = tokio::fs::read_dir(from_dir).await.unwrap();
            while let Some(entry) = dir.next_entry().await.unwrap() {
                let path = entry.path();
                if let Some(file_name) = path.file_name() {
                    if let Some(file_name) = file_name.to_str() {
                        if file_name.ends_with(".gz")
                            || file_name.ends_with(".zstd")
                            || file_name.ends_with(".raw")
                        {
                            objs.push(path);
                        }
                    }
                }
            }
            objs.sort();
            objs.into_iter()
        };

        let mut last_received_frame_no = 0;
        let mut pending_pages =
            TransactionPageCache::new(SWAP_AFTER, page_size, TMP_RESTORE_DIR.into());

        let mut checksum: Option<(u32, u32)> = Some(checksum);
        for obj in objs {
            let key = obj.file_name().unwrap().to_str().unwrap();
            tracing::debug!("Loading {}", key);

            let (first_frame_no, _last_frame_no, _timestamp, compression_kind) =
                match bottomless::replicator::Replicator::parse_frame_range(&format!("/{key}")) {
                    Some(result) => result,
                    None => {
                        if key != "db.gz" && key != "db.zstd" && key != "db.db" {
                            tracing::warn!("Failed to parse frame/page from key {}", key);
                        }
                        continue;
                    }
                };
            if first_frame_no != last_received_frame_no + 1 {
                tracing::warn!("Missing series of consecutive frames. Last applied frame: {}, next found: {}. Stopping the restoration process",
                        last_received_frame_no, first_frame_no);
                break;
            }
            // read frame from the file - from_dir and `obj` dir entry compose the path to it
            let frame = tokio::fs::File::open(&obj).await?;
            let frame_buf_reader = BufReader::new(frame);

            let mut frameno = first_frame_no;
            let mut reader = bottomless::read::BatchReader::new(
                frameno,
                frame_buf_reader,
                page_size as usize,
                compression_kind,
            );

            while let Some(frame) = reader.next_frame_header().await? {
                let pgno = frame.pgno();
                reader.next_page(&mut page_buf).await?;
                if let Some(ck) = checksum {
                    checksum = match frame.verify(ck, &page_buf) {
                        Ok(checksum) => Some(checksum),
                        Err(e) => {
                            println!("ERROR: failed to verify checksum of page {pgno}: {e}, continuing anyway. Checksum will no longer be validated");
                            tracing::error!("Failed to verify checksum of page {pgno}: {e}, continuing anyway. Checksum will no longer be validated");
                            None
                        }
                    };
                }
                pending_pages.insert(pgno, &page_buf).await?;
                if frame.is_committed() {
                    let pending_pages = std::mem::replace(
                        &mut pending_pages,
                        TransactionPageCache::new(SWAP_AFTER, page_size, TMP_RESTORE_DIR.into()),
                    );
                    pending_pages.flush(db).await?;
                }
                frameno += 1;
                last_received_frame_no += 1;
            }
            db.flush().await?;
        }
        Ok(last_received_frame_no)
    }

    pub async fn get_local_metadata(
        from_dir: impl AsRef<std::path::Path>,
    ) -> Result<Option<(u32, (u32, u32))>> {
        use bytes::Buf;

        if let Ok(data) = tokio::fs::read(from_dir.as_ref().join(".meta")).await {
            let mut data = bytes::Bytes::from(data);
            let page_size = data.get_u32();
            let crc = (data.get_u32(), data.get_u32());
            Ok(Some((page_size, crc)))
        } else {
            Ok(None)
        }
    }
}
