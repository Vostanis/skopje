use anyhow::Result;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use tracing::error;

/// Extension of HTTP data-fetching methods for clients.
#[async_trait]
pub trait HttpExtractExt<T: Send + Sync + DeserializeOwned> {
    async fn fetch(&self, url: &str) -> Result<T>;
}

#[async_trait]
impl<T: Send + Sync + DeserializeOwned> HttpExtractExt<T> for reqwest::Client {
    async fn fetch(&self, url: &str) -> Result<T> {
        let data = get(self, url).await?;
        Ok(data)
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
