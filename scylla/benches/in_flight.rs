//! Compares the in-flight request counter of a `Session` with a plain atomic
//! counter that all threads share.
//!
//! Each request does `try_enter()` when it starts, and drops the guard when it
//! ends. These two operations are on the hot path of every request. The
//! scenarios measure them in these variants:
//! - `none`: no counter. It shows the cost of the loop itself.
//! - `plain`: one atomic word that all threads change.
//! - `sharded`: the counter that the driver uses.
//!
//! The scenarios:
//! - `same_thread/work_{N}`: each thread enters, spins `N` iterations, and
//!   exits, in a loop. The parameter is the number of threads.
//! - `tokio`: tasks on a multi-thread runtime enter, yield, and exit, in a
//!   loop. A task can continue on another worker after the yield, so its guard
//!   can be dropped on another thread. The parameter is the number of workers.

#![allow(missing_docs)]

use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId, Criterion, Throughput};
use criterion::{criterion_group, criterion_main};
use tokio::runtime::Runtime;
use tokio::sync::Notify;

// The counter of the driver. It is compiled into this benchmark, so that it is
// inlined like the plain counter below. Thus, the module must not use
// `crate::` paths.
//
// Cargo compiles a benchmark without a harness with `cfg(test)`, but without
// the test functions. Thus, the imports of the unit tests are not used.
#[path = "../src/client/in_flight.rs"]
#[allow(dead_code, unused_imports)]
mod sharded;

const THREADS: [usize; 5] = [1, 2, 4, 8, 16];
const WORK: [u32; 2] = [0, 1000];
const TASKS_PER_WORKER: usize = 4;

/// The hot path of a counter of in-flight requests.
trait Counter: Send + Sync + 'static {
    type Guard<'a>: Send
    where
        Self: 'a;

    fn try_enter(&self) -> Option<Self::Guard<'_>>;
}

struct NoCounter;

impl Counter for NoCounter {
    type Guard<'a> = ();

    fn try_enter(&self) -> Option<()> {
        Some(())
    }
}

/// Same as in the sharded counter.
const CLOSED: usize = 1 << (usize::BITS - 1);

/// The alternative to the sharded counter: one atomic word for all threads.
///
/// It has the `CLOSED` bit and the wakeup of the sharded counter. Its
/// `close()` and `wait_idle()` are not on the hot path, so they are not here.
struct PlainCounter {
    state: AtomicUsize,
    idle: Notify,
}

struct PlainGuard<'a>(&'a PlainCounter);

impl PlainCounter {
    fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
            idle: Notify::new(),
        }
    }
}

impl Counter for PlainCounter {
    type Guard<'a> = PlainGuard<'a>;

    fn try_enter(&self) -> Option<PlainGuard<'_>> {
        // Not a CAS loop: `fetch_add` does not retry when other threads
        // change the value at the same time.
        let previous = self.state.fetch_add(1, Ordering::AcqRel);
        let guard = PlainGuard(self);
        if previous & CLOSED != 0 {
            // The drop undoes the increment.
            drop(guard);
            return None;
        }
        Some(guard)
    }
}

impl Drop for PlainGuard<'_> {
    fn drop(&mut self) {
        let previous = self.0.state.fetch_sub(1, Ordering::AcqRel);
        if previous == CLOSED | 1 {
            self.0.idle.notify_waiters();
        }
    }
}

impl Counter for sharded::InFlightRequests {
    type Guard<'a> = sharded::InFlightGuard<'a>;

    fn try_enter(&self) -> Option<sharded::InFlightGuard<'_>> {
        sharded::InFlightRequests::try_enter(self)
    }
}

fn spin(iterations: u32) {
    for i in 0..iterations {
        black_box(i);
    }
}

/// Runs `iters` requests on each of `threads` threads at the same time.
fn run_on_threads(counter: &impl Counter, threads: usize, work: u32, iters: u64) -> Duration {
    let barrier = Barrier::new(threads + 1);
    std::thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| {
                barrier.wait();
                for _ in 0..iters {
                    let guard = counter.try_enter().unwrap();
                    spin(work);
                    drop(black_box(guard));
                }
                barrier.wait();
            });
        }
        barrier.wait();
        let start = Instant::now();
        barrier.wait();
        start.elapsed()
    })
}

/// Runs `iters` requests in each of `tasks` tasks at the same time.
fn run_in_tasks<C: Counter>(
    counter: &Arc<C>,
    runtime: &Runtime,
    tasks: usize,
    iters: u64,
) -> Duration {
    runtime.block_on(async {
        let start = Instant::now();
        let handles: Vec<_> = (0..tasks)
            .map(|_| {
                let counter = Arc::clone(counter);
                tokio::spawn(async move {
                    for _ in 0..iters {
                        let guard = counter.try_enter().unwrap();
                        tokio::task::yield_now().await;
                        drop(black_box(guard));
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.await.unwrap();
        }
        start.elapsed()
    })
}

fn bench_threads(
    group: &mut BenchmarkGroup<'_, WallTime>,
    name: &str,
    counter: &impl Counter,
    threads: usize,
    work: u32,
) {
    group.bench_function(BenchmarkId::new(name, threads), |b| {
        b.iter_custom(|iters| run_on_threads(counter, threads, work, iters))
    });
}

fn bench_tasks<C: Counter>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    name: &str,
    counter: C,
    runtime: &Runtime,
    workers: usize,
) {
    let counter = Arc::new(counter);
    let tasks = workers * TASKS_PER_WORKER;
    group.bench_function(BenchmarkId::new(name, workers), |b| {
        b.iter_custom(|iters| run_in_tasks(&counter, runtime, tasks, iters))
    });
}

fn same_thread(c: &mut Criterion) {
    for work in WORK {
        let mut group = c.benchmark_group(format!("same_thread/work_{work}"));
        for threads in THREADS {
            group.throughput(Throughput::Elements(threads as u64));
            bench_threads(&mut group, "none", &NoCounter, threads, work);
            bench_threads(&mut group, "plain", &PlainCounter::new(), threads, work);
            let sharded = sharded::InFlightRequests::new();
            bench_threads(&mut group, "sharded", &sharded, threads, work);
        }
        group.finish();
    }
}

fn tokio_tasks(c: &mut Criterion) {
    let mut group = c.benchmark_group("tokio");
    for workers in THREADS {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .build()
            .unwrap();
        group.throughput(Throughput::Elements((workers * TASKS_PER_WORKER) as u64));
        bench_tasks(&mut group, "none", NoCounter, &runtime, workers);
        bench_tasks(&mut group, "plain", PlainCounter::new(), &runtime, workers);
        let sharded = sharded::InFlightRequests::new();
        bench_tasks(&mut group, "sharded", sharded, &runtime, workers);
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(2));
    targets = same_thread, tokio_tasks
}
criterion_main!(benches);
