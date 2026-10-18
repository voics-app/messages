//! Request coalescing, as described in Discord's "How Discord Stores Trillions
//! of Messages": the first request for a key spins up a *worker task* that
//! runs the query; identical requests that arrive while it is in flight
//! subscribe to that task instead of querying again, and every subscriber
//! receives the same result.
//!
//! Results are not cached: once the worker finishes, the key is released and
//! the next request starts a fresh query. The worker is a spawned task, so a
//! subscriber that gives up (client disconnect) does not cancel the query for
//! everyone else.

use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};

type InFlight<V> = Shared<BoxFuture<'static, V>>;

pub struct Coalescer<K, V>
where
    V: Clone,
{
    in_flight: Arc<DashMap<K, (u64, InFlight<V>)>>,
    generation: AtomicU64,
}

impl<K, V> Default for Coalescer<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Coalescer<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            in_flight: Arc::new(DashMap::new()),
            generation: AtomicU64::new(0),
        }
    }

    /// Runs `query` for `key`, or joins the identical query already in
    /// flight. `query` is only invoked by the request that starts the worker.
    pub async fn run<F, Fut>(&self, key: K, query: F) -> V
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = V> + Send + 'static,
    {
        let subscription = match self.in_flight.entry(key.clone()) {
            Entry::Occupied(entry) => entry.get().1.clone(),
            Entry::Vacant(entry) => {
                let generation = self.generation.fetch_add(1, Ordering::Relaxed);
                let worker = self.spawn_worker(key, generation, query());
                entry.insert((generation, worker.clone()));
                worker
            }
        };
        subscription.await
    }

    /// Number of distinct queries currently in flight.
    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }

    fn spawn_worker<Fut>(&self, key: K, generation: u64, query: Fut) -> InFlight<V>
    where
        Fut: Future<Output = V> + Send + 'static,
    {
        let registry = Arc::clone(&self.in_flight);
        let task = tokio::spawn(async move {
            let value = query.await;
            registry.remove_if(&key, |_, (current, _)| *current == generation);
            value
        });
        async move {
            match task.await {
                Ok(value) => value,
                Err(error) => std::panic::resume_unwind(error.into_panic()),
            }
        }
        .boxed()
        .shared()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    fn slow_counted(calls: &Arc<AtomicUsize>, value: u32) -> impl Future<Output = u32> + Send + 'static {
        let calls = Arc::clone(calls);
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            value
        }
    }

    #[tokio::test]
    async fn one_hundred_identical_concurrent_calls_run_one_query() {
        let coalescer = Arc::new(Coalescer::<&'static str, u32>::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let tasks: Vec<_> = (0..100)
            .map(|_| {
                let coalescer = Arc::clone(&coalescer);
                let calls = Arc::clone(&calls);
                tokio::spawn(async move { coalescer.run("k", || slow_counted(&calls, 7)).await })
            })
            .collect();
        for task in tasks {
            assert_eq!(task.await.unwrap(), 7);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(coalescer.in_flight(), 0);
    }

    #[tokio::test]
    async fn distinct_keys_run_separate_queries() {
        let coalescer = Coalescer::<u32, u32>::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let (a, b) = tokio::join!(
            coalescer.run(1, || slow_counted(&calls, 1)),
            coalescer.run(2, || slow_counted(&calls, 2)),
        );
        assert_eq!((a, b), (1, 2));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn results_are_not_cached_after_completion() {
        let coalescer = Coalescer::<u32, u32>::new();
        let calls = Arc::new(AtomicUsize::new(0));
        coalescer.run(1, || slow_counted(&calls, 1)).await;
        coalescer.run(1, || slow_counted(&calls, 1)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_cancelled_first_caller_does_not_cancel_the_query() {
        let coalescer = Arc::new(Coalescer::<u32, u32>::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let first = {
            let coalescer = Arc::clone(&coalescer);
            let calls = Arc::clone(&calls);
            tokio::spawn(async move { coalescer.run(1, || slow_counted(&calls, 9)).await })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        first.abort();
        let second = coalescer.run(1, || slow_counted(&calls, 0)).await;
        assert_eq!(second, 9);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
