//! Shared limits for the suites that start a **foreign engine** container.
//!
//! # The failure this exists to stop
//!
//! `interop_duckdb` and `interop_pyiceberg` start a container per test, and both
//! containers do real work before they print anything: DuckDB runs `INSTALL
//! iceberg`, and the Python one runs `pip install pyiceberg[pyarrow]`. Each is
//! seconds of network and CPU, and the wait strategy is a log line, so the clock
//! that matters is *time to first output* rather than time to start.
//!
//! Cargo runs the suite with one thread per core, so a dozen of those land at
//! once. Every one of them then competes for the same network and the same CPU,
//! every one takes longer than it would alone, and the ones that lose the race
//! trip `WaitContainer(StartupTimeout)`. The tell is that the failing *set*
//! changes from run to run, and that running either suite with
//! `--test-threads=1` passes every time.
//!
//! That is the worst kind of red suite: it is not reporting anything about the
//! code, and a suite that is red for reasons nobody can act on is one people
//! stop reading.
//!
//! # The fix is a bound, not a longer timeout
//!
//! A longer timeout alone only moves the threshold — the load still scales with
//! the core count of whatever machine runs it, so a bigger CI runner makes the
//! problem *worse*. [`engine_slot`] bounds how many foreign-engine containers
//! exist at once, which makes the per-container start time roughly independent
//! of the host. The generous [`STARTUP_TIMEOUT`] is then headroom for a slow
//! network rather than the mechanism.
//!
//! The bound is deliberately not 1. These suites are the slowest in the binary,
//! and serialising them entirely would add about a minute to every run for a
//! safety margin that [`MAX_CONCURRENT`] already provides.

use std::time::Duration;

use tokio::sync::{Semaphore, SemaphorePermit};

/// How many foreign-engine containers may be starting or running at once.
///
/// Four keeps the suites parallel enough to be quick while staying far below the
/// point where they starve each other. Measured rather than guessed: at this
/// bound both suites pass repeatedly on a machine where the unbounded version
/// failed a different handful of tests on every run.
pub const MAX_CONCURRENT: usize = 4;

/// How long a foreign-engine container may take to print its first output.
///
/// Far above the library default, because the wait is not for a process to
/// start: it covers an `INSTALL`/`pip install` over the network. This is
/// headroom for a slow mirror, not the concurrency control — that is
/// [`engine_slot`].
pub const STARTUP_TIMEOUT: Duration = Duration::from_secs(300);

/// The gate itself.
static ENGINES: Semaphore = Semaphore::const_new(MAX_CONCURRENT);

/// Reserve one of the [`MAX_CONCURRENT`] foreign-engine slots.
///
/// Hold the returned permit for as long as the container is alive — dropping it
/// early would let the next test start a container while this one is still
/// competing for the network, which is the situation being avoided.
///
/// ```ignore
/// let _slot = engine_slot().await;
/// let container = GenericImage::new(..).start().await?;
/// // ... permit released when `_slot` drops, after `container` is done
/// ```
pub async fn engine_slot() -> SemaphorePermit<'static> {
    ENGINES
        .acquire()
        .await
        .expect("the engine gate is never closed")
}
