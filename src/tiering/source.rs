//! The CDC seam: a trait, deliberately with no v1 implementation.
//!
//! Archival is the ingestion path (§8.1), and that is not a placeholder. The
//! comparison is closer than it looks — logical decoding reads the WAL and touches
//! no heap pages, which is a real advantage on a busy primary — but it does not
//! solve the expensive half. Replicating rows out of PostgreSQL does not *remove*
//! them, and the hot tier has to stay bounded either way. Both designs need the
//! purge, and the purge is where the cost actually is; once it is a partition
//! drop, what remains of CDC's advantage does not pay for a replication slot that
//! can fill the primary's disk when a consumer stalls.
//!
//! # So why does this file exist
//!
//! Because the *shape* of the alternative is what makes it cheap to adopt later,
//! and expensive to retrofit. If lake latency ever has to drop below an hour, the
//! question should be "which strategy is configured" rather than "how do we
//! restructure ingestion". The trait costs a definition; discovering its absence
//! costs a rewrite.
//!
//! It is feature-gated (`cdc`), unbuilt, and unscheduled. [`rustcdc`] implements
//! this shape against PostgreSQL logical replication when the requirement
//! arrives.
//!
//! [`rustcdc`]: https://github.com/hupe1980/rustcdc
//!
//! # What a source owes the store
//!
//! The two obligations are the ones §8.4 already places on any transport, plus
//! one that is specific to streaming:
//!
//! 1. **Batches carry the storage schema.** A source decodes rows; it does not
//!    get to invent columns. Anything it cannot map belongs upstream.
//! 2. **Positions are opaque and monotonic.** The store never interprets one; it
//!    stores it and hands it back. An LSN, a Kafka offset and a file cursor are
//!    all positions, and none of them is comparable to another.
//! 3. **A checkpoint means durable in the lake, not consumed.** Checkpointing
//!    before the Iceberg commit is the same class of bug as dropping a partition
//!    before it — and it is why `checkpoint` is a separate call rather than an
//!    acknowledgement folded into the stream.

use async_trait::async_trait;
use futures::Stream;

use crate::arrow::array::RecordBatch;
use crate::error::Result;

/// An opaque, source-defined stream position.
///
/// Not interpreted, not compared across sources, and not parsed. A PostgreSQL
/// LSN, a Kafka offset and a byte cursor are all positions; giving the store any
/// structure to reason about would make it responsible for semantics only the
/// source knows.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Position(String);

impl Position {
    /// Wrap a source's own encoding of where it is.
    pub fn new(token: impl Into<String>) -> Self {
        Self(token.into())
    }

    /// The token, for the source that issued it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Position {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A batch of changes, with the position that follows them.
#[derive(Debug, Clone)]
pub struct ChangeBatch {
    /// Rows in the storage schema (§7.1).
    ///
    /// Inserts only. Metering is append-only (§4.2): a correction is a new row at
    /// a higher version, so there is no update or delete to represent, and a
    /// source that produced one would be describing a different domain.
    pub rows: RecordBatch,
    /// The position immediately after this batch.
    ///
    /// Checkpointing it means every row in the batch is durable in the lake. It
    /// is deliberately *after* rather than *at*, so a resumed subscription does
    /// not redeliver the batch it already committed.
    pub next: Position,
}

/// A stream of changes out of the hot tier.
///
/// The seam archival would be swapped for, not an addition to it: both produce
/// batches in the storage schema, and both must respect the tiering invariant.
#[async_trait]
pub trait ChangeSource: Send + Sync {
    /// The stream this source produces.
    type Stream: Stream<Item = Result<ChangeBatch>> + Send;

    /// Subscribe from `from`, or from the source's own beginning.
    ///
    /// `None` means "everything you have", which for a logical replication slot
    /// is a bootstrap snapshot followed by the stream. That is the expensive path
    /// and it should happen once.
    async fn subscribe(&self, from: Option<Position>) -> Result<Self::Stream>;

    /// Record that everything up to `up_to` is durable **in the cold tier**.
    ///
    /// Not "received", not "written to a buffer". A source that may discard
    /// history behind a checkpoint — a replication slot advancing, a retention
    /// window moving — will do so on the strength of this call, so calling it
    /// before the Iceberg commit is the same mistake as dropping a partition
    /// before it (§8.2).
    async fn checkpoint(&self, up_to: Position) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_position_is_opaque_and_round_trips() {
        let p = Position::new("0/16B3748");
        assert_eq!(p.as_str(), "0/16B3748");
        assert_eq!(p.to_string(), "0/16B3748");
        assert_eq!(Position::new("0/16B3748"), p);
    }

    #[test]
    fn positions_from_one_source_order_lexically() {
        // Ordering exists so a source can compare its *own* tokens. It says
        // nothing across sources, which is why nothing here parses one.
        let mut v = [
            Position::new("offset-000003"),
            Position::new("offset-000001"),
            Position::new("offset-000002"),
        ];
        v.sort();
        assert_eq!(v[0].as_str(), "offset-000001");
        assert_eq!(v[2].as_str(), "offset-000003");
    }
}
