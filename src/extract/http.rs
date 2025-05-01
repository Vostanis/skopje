use anyhow::Result;
use async_trait::async_trait;
use bytesize::ByteSize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tracing::{debug, error, trace};

/// Size of each chunk when downloading; currently set to 100MB.
const CHUNK_SIZE: u64 = 100 * 1024 * 1024; // 100 MegaBytes

/// Extension of HTTP data-fetching methods for clients.
#[async_trait]
pub trait HttpExtractExt {
    async fn fetch<T: DeserializeOwned>(&self, url: &str) -> Result<T>;
    async fn download_chunk(
        &self,
        url: &str,
        start: u64,
        end: u64,
        output_file: &mut File,
    ) -> Result<()> {
        Ok(())
    }
    async fn download_file(&self, url: &str, path: &str) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl HttpExtractExt for reqwest::Client {
    async fn fetch<T: DeserializeOwned>(&self, url: &str) -> Result<T> {
        let data = get(self, url).await?;
        Ok(data)
    }

    async fn download_chunk(
        &self,
        url: &str,
        start: u64,
        end: u64,
        output_file: &mut File,
    ) -> Result<()> {
        let url = url.to_string();
        let range = format!("bytes={}-{}", start, end - 1);

        // download a range of bytes
        let response = self
            .get(url)
            .header(reqwest::header::RANGE, range)
            .send()
            .await?;

        // check the response status is 206 Partial Content
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(anyhow::anyhow!(
                "Failed to download chunk: expected 206 Partial Content, got {}",
                response.status()
            ));
        }

        // seek the position of bytes and write to the file
        let body = response.bytes().await?;
        let _seek = output_file.seek(tokio::io::SeekFrom::Start(start)).await?;
        let _write = output_file.write_all(&body).await?;

        Ok(())
    }

    async fn download_file(&self, url: &str, path: &str) -> Result<()> {
        // Retrieve the HTTP response.
        trace!("fetching {url}");
        let response = self.get(url).send().await?;
        let file_size = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|len| len.to_str().ok())
            .and_then(|len| len.parse::<u64>().ok())
            .unwrap_or(0);

        // Guarantee the parent directory exists of the target path.
        let dir = std::path::Path::new(path)
            .parent()
            .ok_or_else(|| anyhow::anyhow!("Failed to find parent for download path"))?;
        tokio::fs::create_dir_all(dir).await?;

        // Initialise async variables ...
        let file = Arc::new(Mutex::new(File::create(path).await?));
        let num_chunks = (file_size + CHUNK_SIZE - 1) / CHUNK_SIZE;
        let mut tasks = Vec::with_capacity(num_chunks as usize);

        debug!("Downloading {url} in chunks");
        for i in 0..num_chunks {
            let start = i * CHUNK_SIZE;
            let end = std::cmp::min((i + 1) * CHUNK_SIZE, file_size);
            let url = url.to_string();
            let file = file.clone();
            let client = self.clone();
            tasks.push(tokio::spawn(async move {
                let mut file = file.lock().await;
                match client.download_chunk(&url, start, end, &mut file).await {
                    Ok(_) => trace!(
                        total_size=%ByteSize(file_size),
                        "Downloaded chunk: ({start}, {end})",
                        start=ByteSize(start),
                        end=ByteSize(end)
                    ),
                    Err(e) => eprintln!("Error downloading chunk {}-{}: {}", start, end, e),
                }
            }));
        }

        // join all async tasks together, in order to execute
        let mut outputs = Vec::with_capacity(tasks.len());
        for task in tasks {
            outputs.push(task.await.expect("Failed to unwrap Future task"));
        }

        Ok(())
    }
}

/// Send a HTTP GET request, using a referenced [`reqweest::Client`] and a URL.
///
/// This function mainly aims to standardize any error handling.
pub async fn get<T: serde::de::DeserializeOwned>(client: &reqwest::Client, url: &str) -> Result<T> {
    let data: T = client
        .get(url)
        .send()
        .await
        .map_err(|e| {
            error!(url = %url, "Failed to send GET request: {e}");
            e
        })?
        .json()
        .await
        .map_err(|e| {
            error!(url = %url, "Failed to deserialize JSON: {e}");
            e
        })?;

    Ok(data)
}
