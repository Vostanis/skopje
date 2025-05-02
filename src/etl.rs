use anyhow::Result;
use async_trait::async_trait;

/// Marker trait to define what has extract functionality.
pub trait Extractor {}
impl<T> Extractor for T {}

/// Marker trait to define what has load functionality.
pub trait Loader {}
impl<T> Loader for T {}

/// Extract the data from some source.
#[async_trait]
pub trait Extract: Sized {
    type Client: Extractor + Send;

    /// How is the data extracted?
    async fn extract(client: &Self::Client) -> Result<Self>;
}

/// Load the data to some data center.
#[async_trait]
pub trait Load {
    type Client: Loader + Send;

    /// How is the data loaded?
    async fn load(&self, client: &Self::Client) -> Result<()>;
}
