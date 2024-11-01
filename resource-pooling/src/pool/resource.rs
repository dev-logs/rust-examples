#[async_trait::async_trait]
pub trait PoolResourceProvider<T>: Send + Sync where T: Send + Sync {
    async fn new(&self) -> T where Self: 'async_trait; 
}

