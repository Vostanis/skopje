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
        I: Iterator<Item = T> + Send + Sync,
        T: SqlTypes + SqlMap + Send + Sync;
}

/// Provide a SQL mapping for the item struct.
///
/// See [`postgres_types::types::ToSql`] for more detail.
pub trait SqlMap {
    fn sql_map(&self) -> Vec<&(dyn ToSql + Sync)>;
}

/// Auto implementation for all values that impl ToSql + Sync.
impl<T: ToSql + Sync> SqlMap for &T {
    fn sql_map(&self) -> Vec<&(dyn ToSql + Sync)> {
        vec![self]
    }
}

/// Provide the SQL types required.
///
/// See [`postgres_types::types::ToSql`] for more detail.
pub trait SqlTypes {
    fn sql_types() -> &'static [Type];
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
            tx.execute(stmt, &item.sql_map()).await?;
            // async move {
            //     match tx.execute(stmt, &item.sql_map()).await {
            //         Ok(_) => {}
            //         Err(e) => return Err(e),
            //     };

            //     Ok::<(), anyhow::Error>(());
            // }
            // .await;
        }
        trace!("{stmt:?} executed successfully");

        // Commit the transaction.
        tx.commit().await?;

        drop(pg_client); // guarantee the postgres client drops back to the pool

        Ok(())
    }

    async fn copy<'a, I, T>(&self, stmt: &'a str, collection: I) -> Result<()>
    where
        I: Iterator<Item = T> + Send + Sync,
        T: SqlTypes + SqlMap + Send + Sync,
    {
        // Get a client from the Pool.
        let mut pg_client = self.get().await?;
        let tx = pg_client.transaction().await?;
        let sink = tx.copy_in(stmt).await?;
        let writer = BinaryCopyInWriter::new(sink, T::sql_types());
        futures::pin_mut!(writer); // writer must be pinned to use

        // Loop the collection & write to the `BinaryCopyInWriter`.
        // Possible async stream could go here, but copies are so quick this may be faster.
        for item in collection {
            match writer.as_mut().write(&item.sql_map()).await {
                Ok(_) => {}
                Err(e) => error!("Failed to copy {stmt:#?}: {e})"),
            }
        }
        trace!("{stmt:?} executed successfully");

        // Commit the transaction.
        writer.finish().await?;
        tx.commit().await?;

        drop(pg_client); // guarantee the postgres client drops back to the pool

        Ok(())
    }
}
