use anyhow::Result;
use async_trait::async_trait;
use futures::{StreamExt, stream};
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};
use tracing::{error, trace};

/// An extension for asynchronous versions of INSERT & COPY for loading data to Postgres.
///
/// The data needs to implement ['SqlMap'].
#[async_trait]
pub trait PgLoadExt {
    /// INSERT transaction.
    async fn insert<'a, I, T>(&self, stmt: &'a str, collection: I) -> Result<()>
    where
        I: Iterator<Item = T> + Send + Sync,
        T: SqlMap + Send + Sync;

    /// COPY transactions cannot fail and still continue committing the rest of the data; any duplicate
    /// data (or any other failing circumstances) must be dealt with prior to the use of the `copy()` function.
    async fn copy<'a, I, T>(&self, stmt: &'a str, collection: I) -> Result<()>
    where
        I: SqlTypes + Iterator<Item = T> + Send + Sync,
        T: SqlMap + Send + Sync;
}

/// Provide a SQL mapping for the item struct.
///
/// See [`postgres_types::types::ToSql`] for more detail.
pub trait SqlMap {
    fn sql_map(&self) -> Vec<&(dyn ToSql + Sync)>;
}

/// Provide the SQL types required.
///
/// See [`postgres_types::types::ToSql`] for more detail.
pub trait SqlTypes {
    fn sql_types(&self) -> &[Type];
}

#[async_trait]
impl PgLoadExt for &deadpool_postgres::Pool {
    async fn insert<'a, I, T>(&self, stmt: &'a str, collection: I) -> Result<()>
    where
        I: Iterator<Item = T> + Send + Sync,
        T: SqlMap + Send + Sync,
    {
        // Get a client from the Pool.
        let mut pg_client = self.get().await?;

        // Start a transaction with a prepared statement.
        let stmt = pg_client.prepare(stmt).await?;
        let tx = pg_client.transaction().await?;

        // Stream the symbols & insert them to the database.
        let mut stream = stream::iter(collection.into_iter());
        while let Some(item) = stream.next().await {
            let stmt = &stmt;
            let tx = &tx;
            async move {
                match tx.execute(stmt, &item.sql_map()).await {
                    Ok(_) => trace!("{stmt:?} executed successfully"),
                    Err(e) => error!("Failed to insert {stmt:#?}: {e}"),
                };
            }
            .await;
        }

        // Commit the transaction.
        tx.commit().await?;

        drop(pg_client); // guarantee the postgres client drops back to the pool

        Ok(())
    }

    async fn copy<'a, I, T>(&self, stmt: &'a str, collection: I) -> Result<()>
    where
        I: SqlTypes + Iterator<Item = T> + Send + Sync,
        T: SqlMap + Send + Sync,
    {
        // Get a client from the Pool.
        let mut pg_client = self.get().await?;
        let tx = pg_client.transaction().await?;
        let sink = tx.copy_in(stmt).await?;
        let writer = BinaryCopyInWriter::new(sink, collection.sql_types());
        futures::pin_mut!(writer); // writer must be pinned to use

        // Loop the collection & write to the Binary Writer.
        // Possible async stream could go here, but copies are so quick this may be faster.
        for item in collection {
            match writer.as_mut().write(&item.sql_map()).await {
                Ok(_) => trace!("{stmt:?} executed successfully"),
                Err(e) => error!("Failed to copy {stmt:#?}: {e})"),
            }
        }

        // Commit the transaction.
        writer.finish().await?;
        tx.commit().await?;

        drop(pg_client); // guarantee the postgres client drops back to the pool

        Ok(())
    }
}
