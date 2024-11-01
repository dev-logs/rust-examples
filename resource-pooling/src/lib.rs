pub mod pool;

#[cfg(test)]
pub mod test {
    use std::{sync::Arc, time::Duration};
    use tokio::sync::OnceCell;
    use crate::pool::{pool::{PoolAllocator, PoolBuilder}, request::PoolRequest, resource::PoolResourceProvider};

    pub struct DbConnection {}

    impl DbConnection {
        pub async fn new(_: String) -> Self {
            Self {}
        }

        pub async fn query(&self) {
            tokio::time::sleep(Duration::from_millis(10)).await; // 200ms
        }
    }

    pub struct DbPoolResourceProvider {
        connection_string: String
    }

    #[async_trait::async_trait]
    impl PoolResourceProvider<DbConnection> for DbPoolResourceProvider {
        async fn new(&self) -> DbConnection {
           DbConnection::new(self.connection_string.clone()).await
        }
    }
 
    pub static DB_POOL: OnceCell<Arc<PoolAllocator<DbConnection>>> = OnceCell::const_new();

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_pool() {
        DB_POOL.get_or_init(|| async move {
            let resource_provider = DbPoolResourceProvider {
                connection_string: String::from("mydb://root@root")
            };

            let pool = PoolBuilder::new(Box::new(resource_provider))
                .min_pool_size(100)
                .max_pool_size(1000)
                .resource_idle_timeout(Duration::from_millis(5000))
                .build().await;

            pool
        }).await;

        let request = PoolRequest {
            pool: DB_POOL.get().unwrap().clone(),
            retrieving_timeout: Some(Duration::from_millis(1000))
        };

        let number_of_tasks: usize = 10000;
        tokio_scoped::scope(|scope| {
            for _ in 0..number_of_tasks {
                let request = request.clone();
                scope.spawn(async move {
                    let db = request.retrieve().await;
                    if db.is_none() {
                        return;
                    }

                    let db = db.unwrap();
                    db.query().await;
                    // By calling drop early we can quickly return the db conneciton back to the
                    // pool
                    drop(db)
                });
            }
        });

        DB_POOL.get().unwrap().pool.wait_for_idle().await;
    }
}

