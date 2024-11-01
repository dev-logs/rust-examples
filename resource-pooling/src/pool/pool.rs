use tokio::spawn;
use tokio::task::{yield_now, JoinHandle};
use tokio::time::sleep;
use std::borrow::BorrowMut;
use std::cmp::min;
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, Mutex};

use super::resource::PoolResourceProvider;
use super::response::PoolResponse;

pub struct Pool<T> where T: Send + Sync + 'static {
    max_size: usize,
    min_size: usize,
    items: Mutex<VecDeque<PoolItem<T>>>,
    counter: Mutex<usize>,
}

impl<T> Pool<T> where T: Send + Sync + 'static {
    fn new(min_size: usize, max_size: usize) -> Self {
        if min_size > max_size {
            panic!("The min size can not be greater than max size");
        }

        Self {
           items: Mutex::new(VecDeque::with_capacity(min_size)),
           counter: Mutex::new(0),
           min_size,
           max_size
        } 
    }

    async fn add_new_item(&self, item: PoolItem<T>) {
        if self.increase_counter().await.is_ok() {
            self.items.lock().await.push_front(item);
        }
    }

    async fn borrow_item(&self) -> Option<PoolItem<T>> {
        self.items.lock().await.pop_front()
    }

    async fn return_borrowed_item(&self, mut item: PoolItem<T>) {
        let mut counter = self.counter.lock().await;

        if *counter <= self.max_size {
            item.borrow_mut().refresh();
            drop(counter);
            self.items.lock().await.push_front(item);
        }
        else {
            *counter -= 1;
            // Discard orphan resource
            // This only occured when there is an item
            // that not belong to this pool.
        }
    }

    async fn increase_counter(&self) -> Result<usize, usize> {
        let mut counter = self.counter.lock().await;

        if *counter < self.max_size {
            *counter += 1;
            return Ok(*counter)
        }

        Err(*counter)
    }

    async fn decrease_counter(&self) -> Result<usize, usize> {
        let mut counter = self.counter.lock().await;
        if *counter > self.min_size {
            *counter -= 1;
            return Ok(*counter);
        }

        Err(*counter)
    }

    async fn invalidate(&self, index: usize) -> Result<(), Duration> {
        let mut items = self.items.lock().await;
        let item = items.get(index);
        if item.is_none() {
            return Ok(())
        }

        let timeleft = item.as_ref().unwrap().timeleft();
        if !timeleft.is_zero() {
            return Err(timeleft)
        }

        if let Some(removed_item) = items.swap_remove_back(index) {
            drop(items);
            if let Err(_) = self.decrease_counter().await {
                self.items.lock().await.push_back(removed_item);
            }
        }

        Ok(())
    }

    async fn number_of_available_items(&self) -> usize {
        self.items.lock().await.len()
    }

    async fn is_exceed_min_size(&self) -> bool {
        *self.counter.lock().await > self.min_size
    }

    // Wait until no requests to the pool and the cleanup has finished it's job
    pub async fn wait_for_idle(&self) {
        loop {
            if self.min_size == *self.counter.lock().await && // The pool is scaled down to min size
               self.min_size == self.number_of_available_items().await // All items has returned 
            {
                break;
            }

            yield_now().await;
        }
    }
}

struct PoolCleaner<T> where T: Send + Sync + 'static {
    pool: Arc<Pool<T>>,
    background_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl<T> PoolCleaner<T> where T: Send + Sync {
    fn new(pool: Arc<Pool<T>>) -> Self {
        Self {
            pool,
            background_handle: Arc::new(Mutex::new(None))
        }
    }

    async fn request_cleanup_loop(&self) {
        if !self.pool.is_exceed_min_size().await {
            return
        }

        let mut background_handle = self.background_handle.lock().await;
        if background_handle.is_some() && !background_handle.as_ref().unwrap().is_finished() { 
            return
        }

        let pool = self.pool.clone();
        *background_handle = {
            Some(spawn(async move {
                let pool = pool;
                let mut curr_index = pool.min_size;
                let mut min_timeout: Option<Duration> = None;

                loop {
                    // yield first to make sure every lock can be quickly released
                    yield_now().await;

                    if !pool.is_exceed_min_size().await {
                        break;
                    }

                    let number_of_idle_items = pool.number_of_available_items().await;

                    if number_of_idle_items == 0 {
                        break;
                    }

                    if curr_index >= number_of_idle_items {
                        if let Some(timeout) = min_timeout {
                            min_timeout = None;
                            sleep(timeout).await;
                        }

                        curr_index = 0;
                        continue;
                    }

                    if let Err(timeleft) = pool.invalidate(curr_index).await {
                        min_timeout = Some(min_timeout
                            .map(|it| min(timeleft, it))
                            .unwrap_or(timeleft)
                        );
                    }

                    curr_index += 1;
                }
            }))
        };
    }
}

impl<T> Drop for PoolCleaner<T> where T: Send + Sync {
    fn drop(&mut self) {
        let background_handle = self.background_handle.clone();
        spawn(async move {
            let mut handle = background_handle.lock().await;
            if handle.is_some() {
                handle.as_mut().unwrap().abort();
            }
        });
    }
}

pub struct PoolAllocator<T>
where
    T: 'static + Send + Sync
{
    pub pool: Arc<Pool<T>>,
    resource_idle_timeout: Duration,
    resource_provider: Box<dyn PoolResourceProvider<T> + 'static>,
    waiters: Mutex<VecDeque<oneshot::Sender<PoolResponse<T>>>>,

    cleaner: PoolCleaner<T>
}

pub struct PoolBuilder<T> where T: Send + Sync + 'static
{
    min_pool_size: usize,
    max_pool_size: usize,
    resource_idle_timeout: Duration,
    pool_resource_provider: Option<Box<dyn PoolResourceProvider<T> + 'static>>,
}

impl<T> PoolBuilder<T> where T: Send + Sync + 'static
{
    pub fn new(resource_provider: Box<dyn PoolResourceProvider<T> + 'static>) -> Self {
        Self {
            min_pool_size: 10,
            max_pool_size: 20,
            resource_idle_timeout: Duration::new(10, 0),
            pool_resource_provider: Some(resource_provider),
        }
    }

    pub fn resource_idle_timeout(mut self, timeout: Duration) -> Self {
        self.resource_idle_timeout = timeout;

        self
    }

    pub fn min_pool_size(mut self, size: usize) -> Self {
        if size <= 0 {
            panic!("The min pool size must be positive");
        }

        self.min_pool_size = size;

        self
    }

    pub fn max_pool_size(mut self, size: usize) -> Self {
        if size <= 0 {
            panic!("The max pool size must be positive");
        }

        self.max_pool_size = size;
        self
    }

    pub async fn build(mut self) -> Arc<PoolAllocator<T>>
    {
        if self.max_pool_size < self.min_pool_size {
            panic!("The max pool size can not less than min pool size");
        }

        let pool = Arc::new(Pool::new(self.min_pool_size, self.max_pool_size));

        let cleaner = PoolCleaner::new(pool.clone());

        let pool_allocator = PoolAllocator {
            resource_provider: self.pool_resource_provider.take().expect("The resource_provider is required"),
            resource_idle_timeout: self.resource_idle_timeout,
            pool,
            cleaner,
            waiters: Mutex::new(VecDeque::new()),
        };

        pool_allocator.init().await;

        Arc::new(pool_allocator)
    }
}

pub struct PoolItem<T>
where
    T: Send + Sync + 'static
{
    resource: Box<T>,
    start_time: Instant,
    max_idling_timeout: Duration
}

impl<T> PoolItem<T> where T: Send + Sync + 'static
{
    pub fn new(resource: Box<T>, max_idling_timeout: Duration) -> Self {
        Self {
            resource,
            start_time: Instant::now(),
            max_idling_timeout
        }
    }

    pub fn refresh(&mut self) {
        self.start_time = Instant::now();
    }

    pub fn timeleft(&self) -> Duration {
       let elapsed = self.start_time.elapsed();
       if elapsed.gt(&self.max_idling_timeout) {
           return Duration::ZERO
       }

       self.max_idling_timeout - elapsed
    }
}

impl<T> Deref for PoolItem<T> where T: Send + Sync + 'static {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.resource
    }
}

impl<T> DerefMut for PoolItem<T> where T: Send + Sync + 'static {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.resource.borrow_mut()
    }
}

impl<T> PoolAllocator<T> where T: Send + Sync + 'static
{
    pub async fn init(&self)
    {
        tokio_scoped::scope(|scope| {
            for _i in 0..self.pool.min_size {
                let pool = self.pool.clone();
                let resource_provider = &self.resource_provider;
                let idle_timeout = self.resource_idle_timeout.clone();
                scope.spawn(async move {
                    let resource = resource_provider.new().await;
                    let pool_item: PoolItem<T> = PoolItem::new(Box::new(resource), idle_timeout);

                    pool.add_new_item(pool_item).await;
                });
            }
        });
    }

    pub async fn retrieve(self: &Arc<Self>) -> Result<PoolResponse<T>, oneshot::Receiver<PoolResponse<T>>> {
        let available_item = self.pool.borrow_item().await;
        if available_item.is_some() {
            return Ok(PoolResponse::new(available_item.unwrap(), self.clone())); 
        }

        // If there is not enough resources, we will create a new one until
        // it reached max_pool_size limit.
        if self.pool.increase_counter().await.is_ok() {
            let new_resource = self.resource_provider.new().await;
            let borrowed_item = PoolItem::new(
                Box::new(new_resource),
                self.resource_idle_timeout);

            return Ok(PoolResponse::new(borrowed_item, self.clone()))
        }

        // If the pool still not have enough available resources
        // we will init a waiter queue to wait for others resource return.
        let (sender, receiver) = oneshot::channel::<PoolResponse<T>>();
        self.waiters.lock().await.push_back(sender);
        Err(receiver)
    }

    /// Put back resource to the pool
    /// If there are waiters in the queue, we will serve waiters first
    /// Otherwise the resource will be returned to the pool.
    /// The cleanup also being triggered if the pool has exceed it's min_size.
    pub async fn put(self: &Arc<Self>, resource: PoolItem<T>) {
        let resource = 'serve_waiter_first: {
            let mut waiters = self.waiters.lock().await;
            let mut resource = Some(resource);
            while let Some(waiter) = waiters.pop_front() {
                resource = waiter
                    .send(PoolResponse::new(resource.unwrap(), self.clone()))
                    .err()
                    .map(|it| it.try_into().unwrap());

                if resource.is_none() {
                    break 'serve_waiter_first None
                }
            }

            resource
        };

        if resource.is_some() {
            self.pool.return_borrowed_item(resource.unwrap()).await;
            self.cleaner.request_cleanup_loop().await;
        }
    }
}
