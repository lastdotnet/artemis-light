use crate::types::Persistence;
use anyhow::Result;
use sqlx::{
    query, Database, Pool, Type
};

pub struct SqlStorage<DB: Database> {
    pool: Pool<DB>,
}

impl<DB: Database> SqlStorage<DB> {
    pub fn new(pool: Pool<DB>) -> Self {
        Self { pool }
    }
}

impl<E, DB> Persistence<E> for SqlStorage<DB>
where
    E: Send +  sqlx::Encode<'static, DB> + Type<DB>,
    DB: Database,
{
    async fn persist(&self, event: E, block_number: i32) -> Result<E> {
todo!()
    }

    async fn replay_from(&self, n: usize) -> Result<crate::types::CollectorStream<'_, E>> {
        todo!()
    }
}
