use anyhow::Result;
use bimap::BiMap;
use deadpool_postgres::Pool;
use num::PrimInt;
use std::hash::Hash;
use std::ops::AddAssign;
use tokio_postgres::types::{FromSql, ToSql};

/// Data structure for tracking used Primary Keys, and providing the next available one.
///
/// The core structure is a Bijective Mapping, [BiMap];
///
/// - on the left, a Primary Key, `PK`, of some integer type (i8, i16, i32, etc.), that is also compatible with [Postgres Types];
/// - on the right, an `Obj`: some hashable value worth sorting (which also requires [Postgres Types] compatibility).
///
/// In tangent, there is the `next_key` variable; the constantly-recalculated, lowest value, of type `PK`.
///
/// [BiMap]: bimap::BiMap,
/// [Postgres Types]: tokio_postgres::types::FromSql,
///
/// ----------------------------------------------------------------------------------------------
///
/// ## Example
/// ```rust
/// use bimap::BiMap;
/// use skopje::KeyMap;
///
/// let mut bimap: BiMap<i8, String> = BiMap::new();
/// bimap.insert(0, "zero".to_string());
/// bimap.insert(1, "one".to_string());
/// bimap.insert(3, "three".to_string());
///
/// let mut keymap = KeyMap::from(bimap);
/// assert_eq!(keymap.see_next_key(), &2);
/// ```
#[derive(Debug)]
pub struct KeyMap<PK, Obj>
where
    PK: Eq + PartialEq + Hash + PrimInt + for<'a> FromSql<'a> + AddAssign,
    Obj: Eq + PartialEq + Hash + for<'a> FromSql<'a>,
{
    pub bimap: BiMap<PK, Obj>,
    pub next_key: PK,
}

impl<PK, Obj> KeyMap<PK, Obj>
where
    PK: Eq + PartialEq + Hash + PrimInt + for<'a> FromSql<'a> + ToSql + AddAssign + Sync,
    Obj: Eq + PartialEq + Hash + for<'a> FromSql<'a> + ToSql + Sync,
{
    /// Retrieve a KeyMap from a PostgreSQL query.
    pub async fn pg_fetch(pg_pool: &Pool, stmt: &str) -> Result<Self> {
        let pg_client = pg_pool.get().await?;
        
        //retrieve a BiMap from a pg query
        let bimap: BiMap<PK, Obj> = pg_client
            .query(stmt, &[])
            .await
            .expect("Failed to fetch key map")
            .into_iter()
            .map(|row| {
                let key: PK = row.get(0);
                let value: Obj = row.get(1);
                (key, value)
            })
            .collect();

        // calculate the first, lowest, available key
        let starting_key = Self::calc_lowest_key(&bimap);

        Ok(Self {
            bimap,
            next_key: starting_key,
        })
    }

    /// Insert a KeyMap into a PostgreSQL table, synchronously.
    pub async fn pg_insert(&self, pg_pool: &Pool, stmt: &str) -> Result<()> {
        let mut pg_client = pg_pool.get().await?;
        
        let query = pg_client
            .prepare(stmt)
            .await.map_err(|e| {
                tracing::error!("Failed to prepare {stmt:?}: {e}");
                e
            })?;

        let tx = pg_client
            .transaction()
            .await.map_err(|e| {
                tracing::error!("Failed to open a transaction: {e}");
                e
            })?;

        for (key, value) in self.bimap.iter() {
            tx.execute(&query, &[&key, &value])
                .await.map_err(|e| {
                    tracing::error!("Failed to execute {query:?}: {e}");
                    e
                })?;
        }

        tx.commit().await.map_err(|e| {
            tracing::error!("Failed to commit transaction: {e}");
            e
        })?;

        Ok(())
    }

    /// Turn a BiMap into a `KeyMap`.
    ///
    /// ```rust
    /// use bimap::BiMap;
    /// use skopje::KeyMap;
    ///
    /// let mut bimap: BiMap<i8, String> = BiMap::new();
    /// bimap.insert(0, "zero".to_string());
    /// bimap.insert(1, "one".to_string());
    /// bimap.insert(3, "three".to_string());
    ///
    /// let mut keymap = KeyMap::from(bimap);
    /// assert_eq!(keymap.see_next_key(), &2);
    /// ```
    pub fn from(bimap: BiMap<PK, Obj>) -> Self {
        let starting_key = Self::calc_lowest_key(&bimap);
        Self {
            bimap,
            next_key: starting_key,
        }
    }

    /// Finds the lowest available key, for some generic N, starting from 0.
    pub fn calc_lowest_key(map: &BiMap<PK, Obj>) -> PK {
        let mut next_key = PK::zero();
        while map.contains_left(&next_key) {
            next_key += PK::one();
        }
        next_key
    }

    /// Set the next available key.
    pub fn calc_next_key(&mut self) {
        while self.bimap.contains_left(&self.next_key) {
            self.next_key += PK::one();
        }
    }

    /// Return a clone of the current `next_key`.
    pub fn see_next_key(&self) -> &PK {
        &self.next_key
    }

    /// Search the BiMap for an existing value;
    ///     - if it does exist, return the associated key.
    ///     - if it doesn't exist, insert the Value with the `next_key`, returning that key.
    ///
    /// ```rust
    /// use bimap::BiMap;
    /// use skopje::KeyMap;
    ///
    /// let mut bimap: BiMap<i8, String> = BiMap::new();
    /// bimap.insert(0, "zero".to_string());
    /// bimap.insert(1, "one".to_string());
    /// bimap.insert(3, "three".to_string());
    ///
    /// // create a keymap from the bimap
    /// let mut keymap = KeyMap::from(bimap);
    /// assert_eq!(keymap.see_next_key(), &2);
    ///
    /// // insert a new value
    /// let pk = keymap.transact("two".to_string());
    /// assert_eq!(pk, 2);
    /// assert_eq!(keymap.see_next_key(), &4); // next key recalculates
    /// ```
    pub fn transact(&mut self, value: Obj) -> PK {
        // if the value already exists, return a clone of the associated key
        if let Some(key) = self.bimap.get_by_right(&value) {
            return key.clone();
        // if the value does not exist, insert it with the next available key, and
        // copy a clone of that key
        } else {
            let key = self.next_key;
            self.bimap.insert(key.clone(), value);
            self.calc_next_key();
            return key;
        }
    }
}
