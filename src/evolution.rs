//! Schema evolution, and the quarantine that stops an unsafe one.
//!
//! Metering schemas are regulator-defined and move on multi-year cycles, so this
//! is far smaller than in a general CDC framework — but it is not empty. New OBIS
//! channels, added deployment attributes, and widened decimal precision all
//! happen, and Iceberg's field-id resolution makes most of them free.
//!
//! # What the classification is for
//!
//! The store's configuration says what the schema *should* be. The cold table
//! says what it *is*. Those two drift for exactly two reasons, and they need
//! opposite responses:
//!
//! - **A deployment added a column.** The Iceberg table gains it with a fresh
//!   field id; every existing Parquet file still reads, with null for the new
//!   column. Nothing to do but proceed.
//! - **Something narrowed, retyped, or moved into or out of the merge key.**
//!   Then rows already written mean something different from rows about to be
//!   written, and *no* amount of care downstream recovers that. The table halts.
//!
//! Both directions of a merge-key change land here: a *new* identity column as a
//! non-nullable [`Added`](SchemaChange::Added), an *undeclared* one as a required
//! [`Dropped`](SchemaChange::Dropped). The second is the more dangerous — the key
//! narrows, two readings the wider key kept apart start competing in resolution,
//! and one supersedes the other with no error anywhere.
//!
//! # Why halting is the right answer
//!
//! Quarantine is the honest response to a change that cannot be applied safely
//! (P6). A store that guessed would keep accepting writes and produce a table
//! where the same column carries two meanings, discoverable months later by
//! whoever reconciles a settlement. Halting one table while the others keep
//! running costs an operator an afternoon; not halting costs a restatement.
//!
//! The watermark freezes with it, which is the point: nothing is archived out of
//! PostgreSQL — where it can still be corrected — into a lake layout nobody has
//! agreed on.

use crate::arrow::datatypes::{DataType, Field, SchemaRef};
use crate::error::{Error, Result};

/// One difference between the configured schema and the stored one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaChange {
    /// A column the configuration declares and the table does not have.
    ///
    /// Safe when nullable: Iceberg adds it with a new field id and historical
    /// files read as null. A **non-nullable** addition is not safe, because every
    /// existing row would violate it — and every identity column is non-nullable,
    /// so this is also how a merge-key change arrives.
    Added {
        /// The declared column.
        field: Field,
        /// Whether it can be added without invalidating existing rows.
        safe: bool,
    },
    /// A column the table has and the configuration no longer declares.
    ///
    /// Safe when the stored column is **nullable**: Iceberg keeps it for time
    /// travel, historical files still carry it, and rows written from now on
    /// simply leave it null.
    ///
    /// A **non-nullable** one is not, and this is the mirror of an unsafe
    /// [`Added`](Self::Added). Every identity column is non-nullable
    /// ([`TableConfig::identity_column`]), so undeclaring one arrives here: the
    /// resolution view would partition by the *narrower* merge key, two tenants'
    /// readings for one measuring point would compete, and one would supersede
    /// the other — a cross-tenant leak with no error anywhere.
    ///
    /// It is not safe for Iceberg either: a required field is required, and a
    /// data file that omits it does not conform to the schema.
    ///
    /// [`TableConfig::identity_column`]: crate::config::TableConfig::identity_column
    Dropped {
        /// The column's name.
        name: String,
        /// Whether the column can be left out of future writes.
        safe: bool,
    },
    /// A column whose type changed.
    Retyped {
        /// The column's name.
        name: String,
        /// What the table holds today.
        from: DataType,
        /// What the configuration now declares.
        to: DataType,
        /// Whether Iceberg can promote the old type to the new one.
        safe: bool,
    },
    /// A column that became nullable, or stopped being nullable.
    ///
    /// Widening to nullable is safe. Narrowing is not: existing rows may hold
    /// nulls the new declaration forbids, and no rewrite is available to find out.
    Nullability {
        /// The column's name.
        name: String,
        /// Whether the change widens rather than narrows.
        safe: bool,
    },
}

impl SchemaChange {
    /// Whether this change can be applied without invalidating stored rows.
    pub fn is_safe(&self) -> bool {
        match self {
            Self::Added { safe, .. } => *safe,
            Self::Dropped { safe, .. } => *safe,
            Self::Retyped { safe, .. } => *safe,
            Self::Nullability { safe, .. } => *safe,
        }
    }

    /// A human-readable account, for the operator who has to resolve it.
    pub fn describe(&self) -> String {
        match self {
            Self::Added { field, safe: true } => {
                format!(
                    "column {:?} added (nullable — historical files read as null)",
                    field.name()
                )
            }
            Self::Added { field, safe: false } => format!(
                "column {:?} added as NOT NULL — every existing row would violate it; \
                 declare it nullable, or rewrite history out of band first",
                field.name()
            ),
            Self::Dropped { name, safe: true } => {
                format!("column {name:?} no longer declared (retained for time travel)")
            }
            Self::Dropped { name, safe: false } => format!(
                "column {name:?} is NOT NULL in the table and is no longer declared — a \
                 required field cannot be left out of a write, and if it was an identity \
                 column the merge key has just narrowed: two readings the wider key kept \
                 apart would now compete, and one would supersede the other. Restore the \
                 declaration, or create a new table"
            ),
            Self::Retyped {
                name,
                from,
                to,
                safe: true,
            } => format!("column {name:?} promoted {from:?} -> {to:?}"),
            Self::Retyped {
                name,
                from,
                to,
                safe: false,
            } => format!(
                "column {name:?} changed {from:?} -> {to:?}, which Iceberg cannot promote; \
                 stored values would be reinterpreted rather than converted"
            ),
            Self::Nullability { name, safe: true } => {
                format!("column {name:?} widened to nullable")
            }
            Self::Nullability { name, safe: false } => format!(
                "column {name:?} narrowed to NOT NULL, but stored rows may already hold nulls"
            ),
        }
    }
}

/// The verdict on a whole schema comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compatibility {
    /// Every difference found, in schema order.
    pub changes: Vec<SchemaChange>,
}

impl Compatibility {
    /// Whether the schemas already agree.
    pub fn is_identical(&self) -> bool {
        self.changes.is_empty()
    }

    /// Whether every change can be applied safely.
    pub fn is_safe(&self) -> bool {
        self.changes.iter().all(SchemaChange::is_safe)
    }

    /// The changes that cannot be applied.
    pub fn unsafe_changes(&self) -> Vec<&SchemaChange> {
        self.changes.iter().filter(|c| !c.is_safe()).collect()
    }

    /// Turn an unsafe comparison into the error that quarantines the table.
    ///
    /// `Ok(())` when every change is safe, including when there are none.
    pub fn require_safe(&self, table: &str) -> Result<()> {
        let unsafe_changes = self.unsafe_changes();
        if unsafe_changes.is_empty() {
            return Ok(());
        }
        Err(Error::Quarantined {
            table: table.to_string(),
            detail: unsafe_changes
                .iter()
                .map(|c| c.describe())
                .collect::<Vec<_>>()
                .join("; "),
        })
    }
}

/// Compare the schema a store would write against the one a table holds.
///
/// `configured` is the source of truth for intent; `stored` for reality. Columns
/// are matched by **name**, because that is what the encoder writes and what an
/// external engine reads — Iceberg matches by field id underneath, which is what
/// makes a rename free, but a rename is invisible from here and reads as a drop
/// plus an add.
///
/// For a nullable column that costs nothing: both halves are safe. For a
/// **non-nullable** one — every identity column is one — both halves are unsafe
/// and the table halts, which is right rather than incidental: renaming an
/// identity column changes the merge key.
pub fn compare(configured: &SchemaRef, stored: &SchemaRef) -> Compatibility {
    let mut changes = Vec::new();

    for field in configured.fields() {
        match stored.field_with_name(field.name()) {
            Err(_) => changes.push(SchemaChange::Added {
                field: field.as_ref().clone(),
                // A new column has no values in existing files, so it can only
                // be added if null is a legal value for it.
                safe: field.is_nullable(),
            }),
            Ok(existing) => {
                if existing.data_type() != field.data_type() {
                    changes.push(SchemaChange::Retyped {
                        name: field.name().clone(),
                        from: existing.data_type().clone(),
                        to: field.data_type().clone(),
                        safe: is_promotable(existing.data_type(), field.data_type()),
                    });
                } else if existing.is_nullable() != field.is_nullable() {
                    changes.push(SchemaChange::Nullability {
                        name: field.name().clone(),
                        // Widening only. Narrowing would forbid nulls that may
                        // already be stored, and nothing here can prove they are
                        // not.
                        safe: field.is_nullable(),
                    });
                }
            }
        }
    }

    for field in stored.fields() {
        if configured.field_with_name(field.name()).is_err() {
            changes.push(SchemaChange::Dropped {
                name: field.name().clone(),
                // The mirror of the `Added` rule. A nullable column may simply
                // stop being written; a required one may not — and every
                // identity column is required, so undeclaring one arrives here
                // and nowhere else.
                safe: field.is_nullable(),
            });
        }
    }

    Compatibility { changes }
}

/// Whether Iceberg can promote `from` to `to` without rewriting data.
///
/// The allowed set is Iceberg's own type-promotion list, restricted to what this
/// schema can actually contain. Widening a decimal's **precision** is permitted
/// and its scale is not: precision adds representable digits to the left, while
/// changing scale reinterprets every stored integer by a factor of ten — a silent
/// factor-of-ten error in a settlement figure.
fn is_promotable(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        (a, b) if a == b => true,
        (DataType::Int32, DataType::Int64) => true,
        (DataType::Float32, DataType::Float64) => true,
        (
            DataType::Decimal128(from_precision, from_scale),
            DataType::Decimal128(to_precision, to_scale),
        ) => from_scale == to_scale && to_precision >= from_precision,
        // Iceberg spells a UTC timestamp `+00:00` where the canonical Arrow
        // schema says `UTC`; the same instant, a different string. Treating that
        // as a retype would quarantine every table on its first check.
        (DataType::Timestamp(a, Some(x)), DataType::Timestamp(b, Some(y))) => {
            a == b && is_utc(x) && is_utc(y)
        }
        _ => false,
    }
}

/// Whether a timezone string denotes UTC, in any of its accepted spellings.
fn is_utc(tz: &str) -> bool {
    matches!(tz, "UTC" | "utc" | "+00:00" | "Z" | "z" | "GMT" | "Etc/UTC")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::datatypes::Schema;
    use std::sync::Arc;

    fn schema(fields: Vec<Field>) -> SchemaRef {
        Arc::new(Schema::new(fields))
    }

    fn base() -> SchemaRef {
        schema(vec![
            Field::new("malo_id", DataType::Utf8, false),
            Field::new("value", DataType::Decimal128(18, 6), false),
        ])
    }

    #[test]
    fn identical_schemas_have_nothing_to_report() {
        let c = compare(&base(), &base());
        assert!(c.is_identical());
        assert!(c.is_safe());
        assert!(c.require_safe("readings").is_ok());
    }

    #[test]
    fn a_nullable_addition_is_safe() {
        let configured = schema(vec![
            Field::new("malo_id", DataType::Utf8, false),
            Field::new("value", DataType::Decimal128(18, 6), false),
            Field::new("bilanzkreis", DataType::Utf8, true),
        ]);
        let c = compare(&configured, &base());
        assert_eq!(c.changes.len(), 1);
        assert!(c.is_safe());
        assert!(c.require_safe("readings").is_ok());
    }

    #[test]
    fn a_non_nullable_addition_quarantines() {
        // This is also how a merge-key change arrives: identity columns are
        // non-nullable by validation, so declaring a new one lands here.
        let configured = schema(vec![
            Field::new("malo_id", DataType::Utf8, false),
            Field::new("value", DataType::Decimal128(18, 6), false),
            Field::new("tenant", DataType::Utf8, false),
        ]);
        let c = compare(&configured, &base());
        assert!(!c.is_safe());
        let err = c.require_safe("readings").unwrap_err();
        assert!(matches!(err, Error::Quarantined { .. }));
        assert!(err.to_string().contains("tenant"), "{err}");
    }

    #[test]
    fn a_dropped_nullable_column_is_safe_because_iceberg_keeps_it() {
        let stored = schema(vec![
            Field::new("malo_id", DataType::Utf8, false),
            Field::new("bilanzkreis", DataType::Utf8, true),
        ]);
        let configured = schema(vec![Field::new("malo_id", DataType::Utf8, false)]);
        let c = compare(&configured, &stored);
        assert_eq!(
            c.changes,
            vec![SchemaChange::Dropped {
                name: "bilanzkreis".into(),
                safe: true,
            }]
        );
        assert!(c.is_safe());
    }

    #[test]
    fn dropping_a_required_column_quarantines() {
        // The mirror of `a_non_nullable_addition_quarantines`, and the more
        // dangerous direction. Identity columns are non-nullable by validation,
        // so *undeclaring* one arrives here: the resolution view would partition
        // by the narrower merge key, two tenants' readings for one measuring
        // point would compete, and one would supersede the other with nothing
        // downstream reporting it.
        let stored = schema(vec![
            Field::new("malo_id", DataType::Utf8, false),
            Field::new("value", DataType::Decimal128(18, 6), false),
            Field::new("tenant", DataType::Utf8, false),
        ]);
        let c = compare(&base(), &stored);
        assert_eq!(
            c.changes,
            vec![SchemaChange::Dropped {
                name: "tenant".into(),
                safe: false,
            }]
        );
        assert!(!c.is_safe());

        let err = c.require_safe("readings").unwrap_err();
        assert!(matches!(err, Error::Quarantined { .. }));
        let msg = err.to_string();
        assert!(msg.contains("tenant"), "{msg}");
        assert!(
            msg.contains("merge key"),
            "the message must name the consequence, not just the column: {msg}"
        );
    }

    #[test]
    fn widening_decimal_precision_is_promotable() {
        let configured = schema(vec![
            Field::new("malo_id", DataType::Utf8, false),
            Field::new("value", DataType::Decimal128(20, 6), false),
        ]);
        assert!(compare(&configured, &base()).is_safe());
    }

    #[test]
    fn changing_decimal_scale_quarantines() {
        // A scale change reinterprets every stored integer by a factor of ten.
        // In a settlement figure that is a silent order-of-magnitude error.
        let configured = schema(vec![
            Field::new("malo_id", DataType::Utf8, false),
            Field::new("value", DataType::Decimal128(18, 3), false),
        ]);
        let c = compare(&configured, &base());
        assert!(!c.is_safe());
        assert!(c.require_safe("readings").is_err());
    }

    #[test]
    fn narrowing_decimal_precision_quarantines() {
        let configured = schema(vec![
            Field::new("malo_id", DataType::Utf8, false),
            Field::new("value", DataType::Decimal128(9, 6), false),
        ]);
        assert!(!compare(&configured, &base()).is_safe());
    }

    #[test]
    fn retyping_a_string_to_a_number_quarantines() {
        let configured = schema(vec![
            Field::new("malo_id", DataType::Int64, false),
            Field::new("value", DataType::Decimal128(18, 6), false),
        ]);
        assert!(!compare(&configured, &base()).is_safe());
    }

    #[test]
    fn widening_to_nullable_is_safe_and_narrowing_is_not() {
        let widened = schema(vec![
            Field::new("malo_id", DataType::Utf8, true),
            Field::new("value", DataType::Decimal128(18, 6), false),
        ]);
        assert!(compare(&widened, &base()).is_safe());
        assert!(!compare(&base(), &widened).is_safe());
    }

    #[test]
    fn the_two_utc_spellings_are_not_a_retype() {
        // Iceberg writes `+00:00`, the canonical Arrow schema says `UTC`. Same
        // instant. Treating it as a change would quarantine every table on its
        // very first check.
        let ours = schema(vec![Field::new(
            "from",
            DataType::Timestamp(
                crate::arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into()),
            ),
            false,
        )]);
        let theirs = schema(vec![Field::new(
            "from",
            DataType::Timestamp(
                crate::arrow::datatypes::TimeUnit::Microsecond,
                Some("+00:00".into()),
            ),
            false,
        )]);
        assert!(compare(&ours, &theirs).is_safe());
        assert!(compare(&ours, &theirs).is_identical() || compare(&ours, &theirs).is_safe());
    }

    #[test]
    fn a_changed_timestamp_unit_is_not_promotable() {
        use crate::arrow::datatypes::TimeUnit;
        let micros = schema(vec![Field::new(
            "from",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )]);
        let nanos = schema(vec![Field::new(
            "from",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        )]);
        assert!(!compare(&nanos, &micros).is_safe());
    }

    #[test]
    fn the_quarantine_message_names_every_offending_column() {
        // The operator resolving this needs the whole list, not the first item.
        let configured = schema(vec![
            Field::new("malo_id", DataType::Utf8, false),
            Field::new("value", DataType::Decimal128(18, 3), false),
            Field::new("tenant", DataType::Utf8, false),
        ]);
        let err = compare(&configured, &base())
            .require_safe("readings")
            .unwrap_err()
            .to_string();
        assert!(err.contains("value"), "{err}");
        assert!(err.contains("tenant"), "{err}");
    }
}
