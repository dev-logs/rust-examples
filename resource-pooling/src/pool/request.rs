use std::sync::Arc;
use std::time::Duration;

use super::pool::PoolAllocator;
use super::response::PoolResponse;

pub struct PoolRequest<T>
where
    T: Send + Sync + 'static,
{
    pub retrieving_timeout: Option<Duration>,
    pub pool: Arc<PoolAllocator<T>>
}

pub struct PoolRequestBuilder<T>
where
    T: 'static + Send + Sync
{
    pub retrieving_timeout: Option<Duration>,
    pub pool_sync: Option<Arc<PoolAllocator<T>>>
}

impl<T> PoolRequestBuilder<T>
where
    T: Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            retrieving_timeout: None,
            pool_sync: None
        }
    }

    pub fn retrieving_timeout(mut self, timeout: Duration) -> Self {
        self.retrieving_timeout = Some(timeout);
        self
    }

    pub fn pool(mut self, pool: Arc<PoolAllocator<T>>) -> Self {
        self.pool_sync = Some(pool);
        self
    }

    pub fn build(mut self) -> PoolRequest<T> {
        PoolRequest {
            retrieving_timeout: self.retrieving_timeout,
            pool: self.pool_sync.take().expect("Pool must be set")
        }
    }
}

impl<T> Clone for PoolRequest<T>
where
    T: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            retrieving_timeout: self.retrieving_timeout.clone()
        }
    }
}

impl<T> PoolRequest<T>
where
    T: Send + Sync + 'static
{
    pub async fn retrieve(&self) -> Option<PoolResponse<T>> {
        match (self.retrieving_timeout, self.pool.retrieve().await) {
            (_, Ok(resource)) => Some(resource),
            (Some(timeout), Err(waiter)) => {
                match tokio::time::timeout(timeout, waiter).await {
                    Ok(Ok(value)) => Some(value),
                    _ => None
                }
            },
            _ => {
                None
            }
        }
    }
}
