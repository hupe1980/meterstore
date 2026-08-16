//! PostgreSQL hot tier.
//!
//! The table is declaratively partitioned by `from` so that purging an archived
//! window is `DROP TABLE` — an `O(1)` catalog operation — rather than a row-wise
//! `DELETE`. At 15-minute metering volume the difference is not a micro-
//! optimisation: deleting a day of readings for 100 k measuring points would
//! create ~9.6 M dead tuples, all of which autovacuum must then clean up while
//! competing with the operational workload the tiering exists to protect.

use async_trait::async_trait;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use sqlx::{PgPool, Row};
use time::{Duration, OffsetDateTime};
use tracing::{debug, info};

use crate::arrow::array::{
    Array, ArrayRef, Decimal128Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use crate::encode::schema::{
    self, VALUE_PRECISION, VALUE_SCALE, VERSION_PRECISION, VERSION_SCALE, col,
};
use crate::error::{Error, Result};
use crate::planner::TimeRange;
use crate::tiering::store::{BatchStream, HotStore, PartitionId, ScanSpec};
use crate::watermark::TieringWatermark;

/// A PostgreSQL-backed hot tier.
#[derive(Debug, Clone)]
pub struct PostgresHot {
    pool: PgPool,
    scan_chunk_rows: usize,
    integrity_constraints: bool,
}

impl PostgresHot {
    /// Rows fetched per round trip when streaming a range.
    ///
    /// Bounds the memory a scan holds regardless of how much the range covers.
    const DEFAULT_SCAN_CHUNK_ROWS: usize = 50_000;

    /// Wrap an existing connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            scan_chunk_rows: Self::DEFAULT_SCAN_CHUNK_ROWS,
            integrity_constraints: true,
        }
    }

    /// Whether each partition refuses writes that would silently corrupt a sum.
    ///
    /// **On by default.** Two GiST exclusion constraints per partition, both
    /// guarding failures that produce a wrong number rather than an error:
    ///
    /// 1. **Overlapping intervals within one version.** The primary key is the
    ///    merge key plus `version`, so two rows for one channel at one version
    ///    must differ in `from` — and two ranges that differ in `from` can still
    ///    overlap. An hourly delivery followed by a quarter-hourly one leaves
    ///    both stored and every aggregate over them inflated. Scoped to a single
    ///    version, so a correction — a higher version covering the same span —
    ///    stays legal.
    ///
    /// 2. **A second network operator for one interval.** A version is
    ///    comparable only within its `(operator, month)` scope (§4.2). The month
    ///    half is guarded at encode time; the operator half is whatever the
    ///    caller passed. Two operators for one `(malo_id, obis_code, from)`
    ///    therefore give two incomparable scopes, resolution picks a winner in
    ///    each, and **both** survive into the resolved view. The realistic cause
    ///    is not a grid-operator change — those deliver for different intervals —
    ///    but a caller passing a forwarding party's MP-ID, or a tenant id, where
    ///    the network operator belongs.
    ///
    /// # When the second one is not what you want
    ///
    /// A store that deliberately keeps **two parties' assertions about the same
    /// reading** — reconciling what a grid operator and a supplier each reported,
    /// say — is a legitimate shape, and constraint 2 refuses it. The right answer
    /// is usually to say so in the schema: give the reporting party an
    /// `identity_column`, and the two assertions become two readings that no
    /// aggregate can conflate. That is what identity columns are for (§7.3), and
    /// it makes the intent explicit rather than resting on a constraint being off.
    ///
    /// Disabling is the escape hatch when that is genuinely not the model.
    ///
    /// Turning this off trades both guarantees for insert throughput — two GiST
    /// indexes per partition on the busiest table in the schema — and should be
    /// done with a measurement in hand rather than on principle. Completeness
    /// (§9.6) then reports an overlap as `surplus` after the fact; a duplicated
    /// scope has no after-the-fact detection at all.
    ///
    /// Requires `btree_gist` (PostgreSQL contrib), created on demand.
    pub fn integrity_constraints(mut self, enabled: bool) -> Self {
        self.integrity_constraints = enabled;
        self
    }

    /// Set how many rows a streaming scan fetches per round trip.
    pub fn scan_chunk_rows(mut self, rows: usize) -> Self {
        self.scan_chunk_rows = rows.max(1);
        self
    }

    /// The underlying pool, for callers that also write through it.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Create the partitioned parent table if it does not exist.
    ///
    /// Note there is no primary key: a unique constraint on a partitioned table
    /// must include the partition key, and `(malo_id, obis_code, "from",
    /// version)` already does. Corrections are new rows with a higher `version`,
    /// so uniqueness must not collapse them.
    pub async fn create_table(&self, table: &str) -> Result<()> {
        self.create_table_with_key(
            table,
            &crate::encode::schema::MERGE_KEY
                .iter()
                .map(|s| (*s).to_string())
                .collect::<Vec<_>>(),
            &[],
        )
        .await
    }

    /// Create the table with an extended identity and extra columns.
    ///
    /// `merge_key` must match what resolution partitions by — it is the primary
    /// key together with `version`, and the two must agree or a row can be
    /// stored that resolution then treats as a competitor to a different
    /// reading.
    pub async fn create_table_with_key(
        &self,
        table: &str,
        merge_key: &[String],
        extra: &[crate::arrow::datatypes::Field],
    ) -> Result<()> {
        let mut extra_ddl = String::new();
        for f in extra {
            // A coded column (declared via `config::coded_column`) carries its
            // allowed-value set in field metadata; render it as a CHECK so the
            // deployment's vocabulary is enforced at the DB layer like sparte/
            // unit/quality, not only by the application that writes the column.
            let check = f
                .metadata()
                .get(crate::config::CHECK_VALUES_KEY)
                .map(|vals| {
                    let codes: Vec<&str> = vals.split(',').collect();
                    format!(
                        " CONSTRAINT {constraint:?} CHECK ({col:?} IN ({list}))",
                        constraint = format!("{}_known", f.name()),
                        col = f.name(),
                        list = sql_code_list(&codes),
                    )
                })
                .unwrap_or_default();
            extra_ddl.push_str(&format!(
                "{:?} {} {}{},\n                ",
                f.name(),
                pg_type(f.data_type())?,
                if f.is_nullable() { "" } else { "NOT NULL" },
                check,
            ));
        }

        let ddl = format!(
            r#"
            CREATE TABLE IF NOT EXISTS "{table}" (
                malo_id       TEXT             NOT NULL,
                melo_id       TEXT,
                -- Canonical OBIS only. This column is part of the merge key,
                -- so two spellings of one channel would let a correction fail
                -- to supersede the value it corrects. The canonical form omits
                -- the storage group when it is unused (255), and keeps it when
                -- it carries information, so `*255` is the one suffix that must
                -- never appear. Failing the write beats resolving wrongly later.
                obis_code     TEXT             NOT NULL
                    CONSTRAINT obis_code_canonical CHECK (
                        obis_code ~ '^[0-9]+-[0-9]+:[0-9]+\.[0-9]+\.[0-9]+(\*[0-9]+)?$'
                        AND obis_code !~ '\*255$'
                    ),
                -- Commodity and unit are checked against `metering`'s own code
                -- lists, rendered below rather than spelled out here: a second
                -- copy of the domain's vocabulary in DDL is a copy that drifts.
                sparte        TEXT             NOT NULL
                    CONSTRAINT sparte_known CHECK (sparte IN ({sparte_codes})),
                "from"        TIMESTAMPTZ      NOT NULL,
                "to"          TIMESTAMPTZ      NOT NULL
                    CONSTRAINT interval_forward CHECK ("to" > "from"),
                value         NUMERIC({VALUE_PRECISION},{VALUE_SCALE}) NOT NULL,
                -- Water is m³ and gas may be either side of the Brennwert
                -- conversion, so the number's dimension is stored, never implied
                -- by the column name.
                unit          TEXT             NOT NULL
                    CONSTRAINT unit_known CHECK (unit IN ({unit_codes})),
                -- Quality is checked against `metering`'s own code list, rendered
                -- below like sparte/unit: the stored value is the resolved reading's
                -- quality, and a drifting literal must fail the write, not read back
                -- as an unknown flag on the authoritative store.
                quality       TEXT             NOT NULL
                    CONSTRAINT quality_known CHECK (quality IN ({quality_codes})),
                resolution    TEXT,
                source_kind   TEXT             NOT NULL,
                source_detail TEXT,
                provenance    TEXT,
                version       NUMERIC({VERSION_PRECISION},{VERSION_SCALE}) NOT NULL,
                -- Canonical `<operator>:<YYYY-MM>` only. `VersionScope` refuses
                -- an operator containing the separator, and the one-operator
                -- exclusion below reads the operator back with `split_part`,
                -- which would take the wrong half if a second separator ever
                -- appeared. Every other coded column carries a CHECK; this one
                -- carries the load-bearing one and had none.
                version_scope TEXT             NOT NULL
                    CONSTRAINT version_scope_canonical CHECK (
                        version_scope ~ '^[^:]+:[0-9]{{4}}-(0[1-9]|1[0-2])$'
                    ),
                recorded_at   TIMESTAMPTZ      NOT NULL,
                -- The day this reading is balanced on: the Berlin calendar day,
                -- or the 06:00-06:00 Gastag for gas. Derived by the encoder from
                -- `from` and `sparte` and stored because no portable SQL
                -- expresses the rule — see `encode::schema`. Not checked here:
                -- PostgreSQL would need the Gastag rule to check it, which is
                -- the very thing being avoided.
                balancing_day DATE             NOT NULL,
                {extra_ddl}
                PRIMARY KEY ({pk})
            ) PARTITION BY RANGE ("from")
            "#,
            extra_ddl = extra_ddl,
            sparte_codes = sql_code_list(metering::Sparte::CODES),
            unit_codes = sql_code_list(metering::interval::MeasurementUnit::CODES),
            quality_codes = sql_code_list(metering::QualityFlag::CODES),
            pk = merge_key
                .iter()
                .map(|c| format!("{c:?}"))
                .chain(std::iter::once("version".to_string()))
                .collect::<Vec<_>>()
                .join(", "),
        );
        sqlx::query(&ddl).execute(&self.pool).await.map_err(pg)?;

        // The dominant read is one meter over a time range.
        let idx = format!(
            r#"CREATE INDEX IF NOT EXISTS "{table}_malo_from_idx" ON "{table}" (malo_id, "from")"#
        );
        sqlx::query(&idx).execute(&self.pool).await.map_err(pg)?;

        info!(table, "hot table ready");
        Ok(())
    }
}

/// Refuse intervals that overlap another at the same version, and a reading that
/// carries two network operators.
///
/// Attached per partition, not to the parent: PostgreSQL rejects `EXCLUDE` on a
/// partitioned table unless the constraint compares the partition key with `=`,
/// and the partition key is `from` — which appears here inside a range, not as an
/// equality. An interval crossing a partition boundary is already impossible,
/// because a row lives in the partition of its `from` and every archival window
/// is one partition (§7.2).
///
/// The equality columns are read from the parent's **primary key**, minus `from`,
/// so a deployment that extends the merge key with identity columns gets an
/// exclusion over the same notion of "the same reading" that resolution uses.
/// Deriving them rather than passing them in keeps the two from disagreeing — the
/// failure mode would be an exclusion that spans tenants, rejecting one
/// operator's reading because another reported the same meter.
///
/// Takes a connection rather than the pool so it runs inside the transaction that
/// created the partition — outside it, the `ALTER TABLE` would target a relation
/// no other session can see yet.
async fn add_integrity_constraints(
    conn: &mut sqlx::PgConnection,
    table: &str,
    partition: &str,
) -> Result<()> {
    // Ships in contrib; supplies the GiST equality operators for text and
    // numeric, without which the constraint cannot combine `=` with `&&`.
    sqlx::query("CREATE EXTENSION IF NOT EXISTS btree_gist")
        .execute(&mut *conn)
        .await
        .map_err(|e| {
            Error::config(format!(
                "overlap exclusion needs the btree_gist extension, and creating it \
                     failed ({e}). Install it as a superuser, or disable the check with \
                     PostgresHot::integrity_constraints(false) and accept that an \
                     overlapping delivery is then detected by completeness rather than \
                     refused"
            ))
        })?;

    let key = primary_key_columns(&mut *conn, table).await?;
    let equality: Vec<String> = key
        .iter()
        .filter(|c| c.as_str() != col::FROM)
        .map(|c| format!("{c:?} WITH ="))
        .collect();
    if equality.is_empty() {
        return Err(Error::config(format!(
            "{table} has no primary key columns besides {:?}, so an overlap \
                 exclusion would compare every row against every other",
            col::FROM
        )));
    }

    let ddl = format!(
        r#"ALTER TABLE "{partition}" ADD CONSTRAINT "{partition}_no_overlap"
               EXCLUDE USING gist ({}, tstzrange("from", "to", '[)') WITH &&)"#,
        equality.join(", "),
    );
    sqlx::query(&ddl).execute(&mut *conn).await.map_err(pg)?;

    // One network operator per reading.
    //
    // A version is comparable only within its `(operator, month)` scope, and
    // resolution partitions by that scope — so two operators for one reading
    // produce two winners, both of which survive into the resolved view and
    // double every sum over them. The month half of the scope is guarded at
    // encode time; this is the operator half, which can only be checked
    // against what is already stored.
    //
    // Equality on the **whole** merge key, because the conflict is two rows
    // for the same reading; `<>` on the operator, because the conflict is
    // that they disagree about it. `version` is deliberately absent: a
    // correction is a different version and must stay legal, but it must
    // still come from the same operator.
    //
    // The operator is the part of `version_scope` before the separator,
    // which is unambiguous because `VersionScope` refuses an operator
    // containing one.
    let key = primary_key_columns(&mut *conn, table).await?;
    let merge_key: Vec<String> = key
        .iter()
        .filter(|c| c.as_str() != col::VERSION)
        .map(|c| format!("{c:?} WITH ="))
        .collect();

    let ddl = format!(
        r#"ALTER TABLE "{partition}" ADD CONSTRAINT "{partition}_one_operator"
               EXCLUDE USING gist ({}, split_part(version_scope, ':', 1) WITH <>)"#,
        merge_key.join(", "),
    );
    sqlx::query(&ddl).execute(&mut *conn).await.map_err(pg)?;

    Ok(())
}

/// The parent table's primary key columns, in key order.
async fn primary_key_columns(conn: &mut sqlx::PgConnection, table: &str) -> Result<Vec<String>> {
    let rows: Vec<String> = sqlx::query_scalar(
        r#"SELECT a.attname::text
               FROM   pg_index i
               JOIN   pg_attribute a
                 ON   a.attrelid = i.indrelid
                AND   a.attnum = ANY(i.indkey)
               WHERE  i.indrelid = $1::regclass
                AND   i.indisprimary
               ORDER  BY array_position(i.indkey, a.attnum)"#,
    )
    .bind(format!("\"{table}\""))
    .fetch_all(&mut *conn)
    .await
    .map_err(pg)?;

    if rows.is_empty() {
        return Err(Error::config(format!(
            "{table} has no primary key; the overlap exclusion derives its \
             equality columns from it"
        )));
    }
    Ok(rows)
}

impl PostgresHot {
    /// Insert one batch and report what each row did to the current value.
    ///
    /// The prior state and the insert run in **one transaction**. Reading the
    /// prior state separately would race the write — and be wrong precisely when
    /// two corrections arrive together, which is when an audit trail matters.
    ///
    /// What the rows *do* on their way in — deduplication, and the refusal to
    /// restate a value under an existing version — is [`insert_rows`]'s, which
    /// this shares with the plain append path.
    ///
    /// [`insert_rows`]: Self::insert_rows
    async fn insert_reporting(
        &self,
        table: &str,
        merge_key: &[String],
        batch: &RecordBatch,
    ) -> Result<Vec<crate::session::Displacement>> {
        use crate::session::{Displacement, Effect, StoredValue};

        let n = batch.num_rows();
        if n == 0 {
            return Ok(Vec::new());
        }

        // Identity columns are part of what names a reading: with a `tenant`
        // column, `(malo_id, obis_code, from)` alone names two different
        // readings, and a report keyed on it would attribute one tenant's
        // correction to another's value.
        let identity: Vec<String> = merge_key
            .iter()
            .filter(|c| !crate::encode::schema::MERGE_KEY.contains(&c.as_str()))
            .cloned()
            .collect();

        let value_of = |row: usize| -> Result<StoredValue> {
            let r = RowView::new(batch, row)?;
            Ok(StoredValue {
                value: r.value,
                unit: r
                    .unit
                    .parse()
                    .map_err(|e| Error::decode(col::UNIT, format!("{:?}: {e}", r.unit)))?,
                quality: r
                    .quality
                    .parse()
                    .map_err(|e| Error::decode(col::QUALITY, format!("{:?}: {e}", r.quality)))?,
                version: crate::version::ScopedVersion::new(
                    crate::version::VersionScope::parse(r.version_scope)?,
                    decimal_to_version(r.version)?,
                ),
                recorded_at: r.recorded_at,
            })
        };

        let mut rows = Vec::with_capacity(n);
        for row in 0..n {
            let r = RowView::new(batch, row)?;
            let mut ident = Vec::with_capacity(identity.len());
            for name in &identity {
                ident.push((name.clone(), text_column(batch, name, row)?.to_string()));
            }
            rows.push((
                (r.malo.to_string(), r.obis.to_string(), r.from, ident),
                value_of(row)?,
                r.to,
            ));
        }

        let mut tx = self.pool.begin().await.map_err(pg)?;

        // **Every** stored version for the affected readings, not just the
        // winner. The winner alone cannot say whether this exact version is
        // already present, which is what separates a replay from a backfill.
        let malo: Vec<String> = rows.iter().map(|(k, ..)| k.0.clone()).collect();
        let obis: Vec<String> = rows.iter().map(|(k, ..)| k.1.clone()).collect();
        let from: Vec<OffsetDateTime> = rows.iter().map(|(k, ..)| k.2).collect();

        let ident_select = identity
            .iter()
            .map(|c| format!(", s.{c:?}"))
            .collect::<String>();
        let sql = format!(
            r#"SELECT s.malo_id, s.obis_code, s."from", s.value, s.unit, s.quality,
                      s.version, s.version_scope, s.recorded_at{ident_select}
               FROM "{table}" s
               JOIN unnest($1::text[], $2::text[], $3::timestamptz[])
                      AS k(malo_id, obis_code, "from")
                 ON s.malo_id = k.malo_id
                AND s.obis_code = k.obis_code
                AND s."from" = k."from""#
        );
        let stored = sqlx::query(&sql)
            .bind(&malo)
            .bind(&obis)
            .bind(&from)
            .fetch_all(&mut *tx)
            .await
            .map_err(pg)?;

        type Key = (String, String, OffsetDateTime, Vec<(String, String)>);
        let mut current: std::collections::HashMap<Key, StoredValue> = Default::default();
        let mut seen: std::collections::HashSet<(Key, u128)> = Default::default();

        for row in &stored {
            let mut ident = Vec::with_capacity(identity.len());
            for (i, name) in identity.iter().enumerate() {
                ident.push((name.clone(), row.try_get::<String, _>(9 + i).map_err(pg)?));
            }
            let key: Key = (
                row.try_get(0).map_err(pg)?,
                row.try_get(1).map_err(pg)?,
                row.try_get(2).map_err(pg)?,
                ident,
            );

            let unit: String = row.try_get(4).map_err(pg)?;
            let quality: String = row.try_get(5).map_err(pg)?;
            let scope: String = row.try_get(7).map_err(pg)?;
            let held = StoredValue {
                value: row.try_get(3).map_err(pg)?,
                unit: unit
                    .parse()
                    .map_err(|e| Error::decode(col::UNIT, format!("{unit:?}: {e}")))?,
                quality: quality
                    .parse()
                    .map_err(|e| Error::decode(col::QUALITY, format!("{quality:?}: {e}")))?,
                version: crate::version::ScopedVersion::new(
                    crate::version::VersionScope::parse(&scope)?,
                    decimal_to_version(row.try_get(6).map_err(pg)?)?,
                ),
                recorded_at: row.try_get(8).map_err(pg)?,
            };

            seen.insert((key.clone(), held.version.version().get()));
            match current.get(&key) {
                // Ordered through `ScopedVersion`, which refuses a comparison
                // across scopes rather than guessing — and the one-operator
                // constraint means that refusal cannot fire for one reading.
                Some(best) if best.version.try_cmp(&held.version)? != std::cmp::Ordering::Less => {}
                _ => {
                    current.insert(key, held);
                }
            }
        }

        self.insert_rows(&mut tx, table, merge_key, batch).await?;
        tx.commit().await.map_err(pg)?;

        // Folded in ascending version order, so a batch carrying two versions of
        // one reading reports the second as superseding the first rather than
        // both as superseding whatever was there before.
        let mut order: Vec<usize> = (0..rows.len()).collect();
        order.sort_by_key(|&i| rows[i].1.version.version().get());

        let mut out = vec![None; rows.len()];
        for i in order {
            let (key, written, to) = &rows[i];
            let prior = current.get(key).cloned();
            let replayed = seen.contains(&(key.clone(), written.version.version().get()));

            let effect = match &prior {
                None => Effect::Inserted,
                Some(_) if replayed => Effect::Duplicate,
                Some(p) => match p.version.try_cmp(&written.version)? {
                    std::cmp::Ordering::Less => Effect::Superseded,
                    _ => Effect::Shadowed,
                },
            };
            if effect.changed_current_value() {
                current.insert(key.clone(), written.clone());
            }
            out[i] = Some(Displacement {
                malo_id: key.0.clone(),
                obis_code: key.1.clone(),
                from: key.2,
                to: *to,
                identity: key.3.clone(),
                effect,
                superseded: prior,
                written: written.clone(),
            });
        }

        Ok(out.into_iter().flatten().collect())
    }

    async fn append_batch(
        &self,
        table: &str,
        merge_key: &[String],
        batch: &RecordBatch,
    ) -> Result<u64> {
        let mut conn = self.pool.acquire().await.map_err(pg)?;
        self.insert_rows(&mut conn, table, merge_key, batch).await
    }

    /// The one insert implementation, so the plain and reporting paths cannot
    /// diverge in what they accept or how they deduplicate.
    ///
    /// **Idempotent by design.** Every ingest transport worth using delivers at
    /// least once — Kafka redelivers after a failed commit, webhooks retry on a
    /// timeout — so a replayed batch is ordinary traffic. `ON CONFLICT DO
    /// NOTHING` makes replay a no-op, and unlike `DO UPDATE` it writes no row
    /// version, so redelivery creates no dead tuples for autovacuum.
    ///
    /// Rows are sent as arrays and expanded with `unnest`, one statement per
    /// batch rather than one per reading. At 96 values per meter per day that
    /// difference is the whole write path.
    ///
    /// A conflict means the same `(merge key, version)` already exists. That is
    /// only legitimate when the row is *identical*: a version identifies one
    /// assertion, so a different value under the same version is a producer
    /// error, and the skipped rows are checked for exactly that rather than
    /// being trusted as a replay.
    ///
    /// Takes a connection rather than the pool so the reporting path can run it
    /// inside the same transaction as the prior-state read — and so **every**
    /// statement it issues, the divergence check included, stays on that one
    /// connection.
    async fn insert_rows(
        &self,
        conn: &mut sqlx::PgConnection,
        table: &str,
        merge_key: &[String],
        batch: &RecordBatch,
    ) -> Result<u64> {
        // Extra columns are part of the row but not of the fixed column list.
        let core = crate::encode::schema::storage_schema(&[]);
        let extra: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .filter(|f| core.field_with_name(f.name()).is_err())
            .map(|f| f.name().clone())
            .collect();
        let conflict = merge_key
            .iter()
            .map(|c| format!("{c:?}"))
            .chain(std::iter::once("version".to_string()))
            .collect::<Vec<_>>()
            .join(", ");

        let n = batch.num_rows();
        let mut malo = Vec::with_capacity(n);
        let mut melo: Vec<Option<String>> = Vec::with_capacity(n);
        let mut obis = Vec::with_capacity(n);
        let mut sparte = Vec::with_capacity(n);
        let mut from = Vec::with_capacity(n);
        let mut to = Vec::with_capacity(n);
        let mut value = Vec::with_capacity(n);
        let mut unit = Vec::with_capacity(n);
        let mut quality = Vec::with_capacity(n);
        let mut resolution: Vec<Option<String>> = Vec::with_capacity(n);
        let mut source_kind = Vec::with_capacity(n);
        let mut source_detail: Vec<Option<String>> = Vec::with_capacity(n);
        let mut provenance: Vec<Option<String>> = Vec::with_capacity(n);
        let mut version = Vec::with_capacity(n);
        let mut version_scope = Vec::with_capacity(n);
        let mut recorded_at = Vec::with_capacity(n);
        let mut balancing_day = Vec::with_capacity(n);

        for row in 0..n {
            let r = RowView::new(batch, row)?;
            malo.push(r.malo.to_string());
            melo.push(r.melo.map(str::to_string));
            obis.push(r.obis.to_string());
            sparte.push(r.sparte.to_string());
            from.push(r.from);
            to.push(r.to);
            value.push(r.value);
            unit.push(r.unit.to_string());
            quality.push(r.quality.to_string());
            resolution.push(r.resolution.map(str::to_string));
            source_kind.push(r.source_kind.to_string());
            source_detail.push(r.source_detail.map(str::to_string));
            provenance.push(r.provenance.map(str::to_string));
            version.push(r.version);
            version_scope.push(r.version_scope.to_string());
            recorded_at.push(r.recorded_at);
            balancing_day.push(r.balancing_day);
        }

        // Extra column values, as text arrays. Config validation restricts
        // declared extra columns to text for exactly this reason: binding
        // arbitrary Arrow types as Postgres arrays needs a bind arm per type,
        // and every attribute a deployment has actually wanted — tenant,
        // Bilanzkreis, grid area — is a string.
        let mut extra_values: Vec<Vec<Option<String>>> = Vec::with_capacity(extra.len());
        for name in &extra {
            let column = batch
                .column_by_name(name)
                .ok_or_else(|| Error::encode(name, "declared column missing from batch"))?;
            let strings = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::encode(name, "extra columns must be text"))?;
            extra_values.push(
                (0..strings.len())
                    .map(|i| (!strings.is_null(i)).then(|| strings.value(i).to_string()))
                    .collect(),
            );
        }

        let extra_cols = extra.iter().map(|c| format!(", {c:?}")).collect::<String>();
        let extra_params = (0..extra.len())
            .map(|i| format!(", ${}::text[]", core_column_count() + 1 + i))
            .collect::<String>();

        let sql = format!(
            r#"INSERT INTO "{table}" ({core_cols}{extra_cols})
               SELECT * FROM unnest(
                   $1::text[], $2::text[], $3::text[], $4::text[],
                   $5::timestamptz[], $6::timestamptz[], $7::numeric[], $8::text[],
                   $9::text[], $10::text[], $11::text[], $12::text[], $13::text[],
                   $14::numeric[], $15::text[], $16::timestamptz[], $17::date[]{extra_params}
               )
               ON CONFLICT ({conflict}) DO NOTHING"#,
            core_cols = scan_columns(),
        );

        let mut query = sqlx::query(&sql)
            .bind(&malo)
            .bind(&melo)
            .bind(&obis)
            .bind(&sparte)
            .bind(&from)
            .bind(&to)
            .bind(&value)
            .bind(&unit)
            .bind(&quality)
            .bind(&resolution)
            .bind(&source_kind)
            .bind(&source_detail)
            .bind(&provenance)
            .bind(&version)
            .bind(&version_scope)
            .bind(&recorded_at)
            .bind(&balancing_day);

        for values in &extra_values {
            query = query.bind(values);
        }

        let inserted = query.execute(&mut *conn).await.map_err(pg)?.rows_affected();

        let skipped = n as u64 - inserted;
        let metrics = crate::observe::metrics();
        let attrs = crate::observe::table(table);
        metrics.rows_written.add(inserted, &attrs);
        metrics.rows_deduplicated.add(skipped, &attrs);

        if skipped > 0 {
            // Replay is fine; a changed value under the same version is not.
            // Compare on the identity the table actually uses. Joining on a
            // narrower key would report divergence between rows that are simply
            // different readings — and a *wider* one would not compile, which is
            // why the identity columns have to reach the `unnest` alias too. A
            // deployment declaring `identity_column("tenant")` otherwise turns
            // every redelivery into a SQL error about a column that does not
            // exist.
            let identity: Vec<&String> = merge_key
                .iter()
                .filter(|c| !crate::encode::schema::MERGE_KEY.contains(&c.as_str()))
                .collect();

            let join = merge_key
                .iter()
                .map(|c| format!(r#"stored.{c:?} = incoming.{c:?}"#))
                .chain(std::iter::once(
                    "stored.version = incoming.version".to_string(),
                ))
                .collect::<Vec<_>>()
                .join(" AND ");

            let identity_params = (0..identity.len())
                .map(|i| format!(", ${}::text[]", 6 + i))
                .collect::<String>();
            let identity_cols = identity
                .iter()
                .map(|c| format!(", {c:?}"))
                .collect::<String>();

            let sql = format!(
                r#"SELECT count(*) FROM unnest(
                       $1::text[], $2::text[], $3::timestamptz[],
                       $4::numeric[], $5::numeric[]{identity_params}
                   ) AS incoming(malo_id, obis_code, "from", version, value{identity_cols})
                   JOIN "{table}" stored ON {join}
                   WHERE stored.value IS DISTINCT FROM incoming.value"#
            );
            let mut query = sqlx::query_scalar::<_, i64>(&sql)
                .bind(&malo)
                .bind(&obis)
                .bind(&from)
                .bind(&version)
                .bind(&value);

            for name in &identity {
                let index = extra
                    .iter()
                    .position(|e| e == *name)
                    .ok_or_else(|| Error::encode(name.as_str(), "identity column missing"))?;
                query = query.bind(&extra_values[index]);
            }

            // On **this** connection, never `self.pool`.
            //
            // `insert_rows` is handed a connection that is already checked out —
            // and on the reporting path it is a connection inside an open
            // transaction. Reaching back to the pool for a second one from here
            // is a pool-exhaustion deadlock rather than a slow path: with a pool
            // of `n`, `n` concurrent writers each hold one connection and each
            // wait for an `n+1`th that can only free up when one of them
            // finishes. Nothing times out, and the symptom is a write path that
            // stops entirely under exactly the concurrency it was built for.
            //
            // It is also the only spelling that is *correct*. The check has to
            // see the same snapshot as the insert it is checking, and a separate
            // connection sees neither the transaction's own rows nor a
            // consistent view of a concurrent writer's.
            let diverged = query.fetch_one(&mut *conn).await.map_err(pg)?;

            if diverged > 0 {
                return Err(Error::InvariantViolated {
                    table: table.to_string(),
                    detail: format!(
                        "{diverged} row(s) restate a different value under an existing \
                         version — a version identifies one assertion, so a corrected \
                         value needs a higher version"
                    ),
                });
            }
            debug!(table, skipped, "replayed rows already present");
        }

        Ok(inserted)
    }

    /// Stream a relation in chunks, resuming by keyset.
    ///
    /// Keyset pagination rather than one long-lived cursor. A cursor would hold
    /// a connection — and a transaction snapshot — open for the whole scan,
    /// which at metering volume means minutes of blocked vacuum on the hot
    /// table. Paging by the sort key keeps each round trip short and memory
    /// bounded by `scan_chunk_rows` rather than by the range.
    ///
    /// The cursor tuple comes from [`ScanSpec::cursor_columns`], which is unique
    /// per row. That is not a detail: a cursor resumes at *strictly greater
    /// than* the last row of a chunk, so a non-unique cursor drops every
    /// remaining row that ties with it — silently, and only once a table is
    /// large enough for a chunk boundary to land inside a tie.
    fn chunked_scan(&self, relation: &str, range: TimeRange, spec: &ScanSpec) -> BatchStream {
        let pool = self.pool.clone();
        let relation = relation.to_string();
        let chunk = spec.chunk_rows().unwrap_or(self.scan_chunk_rows);
        let extra: Vec<String> = spec.extra().to_vec();
        let extra_select = extra.iter().map(|c| format!(", {c:?}")).collect::<String>();

        let cursor_columns = spec.cursor_columns();
        let order_by = cursor_columns
            .iter()
            .map(|c| format!("{c:?}"))
            .collect::<Vec<_>>()
            .join(", ");
        // Positions in the projected row that each cursor column is read from,
        // so resuming does not depend on the cursor columns also being selected
        // in cursor order.
        let cursor_positions: Vec<usize> = cursor_columns
            .iter()
            .map(|c| projection_index(c, &extra))
            .collect::<Result<Vec<_>>>()
            .unwrap_or_default();

        Box::pin(async_stream::try_stream! {
            // Guarded here rather than at the call sites: an empty position list
            // means a cursor column was not projected, and paging on a cursor we
            // cannot read would loop forever on the first chunk.
            if cursor_positions.len() != cursor_columns.len() {
                Err(Error::config(format!(
                    "cursor columns {cursor_columns:?} are not all projected by the scan"
                )))?;
            }

            let mut cursor: Option<Vec<CursorValue>> = None;

            loop {
                let mut sql =
                    format!(
                        r#"SELECT {}{extra_select} FROM "{relation}" WHERE true"#,
                        scan_columns()
                    );
                let mut n = 0;
                if range.start().is_some() {
                    n += 1;
                    sql.push_str(&format!(r#" AND "from" >= ${n}"#));
                }
                if range.end().is_some() {
                    n += 1;
                    sql.push_str(&format!(r#" AND "from" < ${n}"#));
                }
                if cursor.is_some() {
                    let placeholders = (0..cursor_columns.len())
                        .map(|i| format!("${}", n + 1 + i))
                        .collect::<Vec<_>>()
                        .join(", ");
                    sql.push_str(&format!(r#" AND ({order_by}) > ({placeholders})"#));
                }
                sql.push_str(&format!(r#" ORDER BY {order_by} LIMIT {chunk}"#));

                let mut query = sqlx::query(&sql);
                if let Some(start) = range.start() {
                    query = query.bind(start);
                }
                if let Some(end) = range.end() {
                    query = query.bind(end);
                }
                if let Some(values) = &cursor {
                    for value in values {
                        query = match value {
                            CursorValue::Text(v) => query.bind(v.clone()),
                            CursorValue::Timestamp(v) => query.bind(*v),
                            CursorValue::Numeric(v) => query.bind(*v),
                        };
                    }
                }

                let rows = query.fetch_all(&pool).await.map_err(pg)?;
                if rows.is_empty() {
                    break;
                }

                // Remember where to resume before the rows are consumed.
                let last = rows.last().expect("non-empty");
                cursor = Some(
                    cursor_positions
                        .iter()
                        .map(|i| CursorValue::read(last, *i))
                        .collect::<Result<Vec<_>>>()?,
                );
                let exhausted = rows.len() < chunk;

                for batch in rows_to_batches(rows, &extra)? {
                    yield batch;
                }
                if exhausted {
                    break;
                }
            }
        })
    }

    /// Create one partition if it is missing, returning whether it was created.
    ///
    /// # Why this is serialised
    ///
    /// Every writer ensures the partitions for the range it is about to write,
    /// so the **first batch of a new day has every ingest worker creating the
    /// same partition at the same moment** — and that is the ordinary topology
    /// (§5.2), not an unusual one. Check-then-create loses that race outright:
    /// `CREATE TABLE IF NOT EXISTS … PARTITION OF` does not suppress the
    /// collision, so the losers get `relation "readings_2026_08_20_0000" already
    /// exists` and the batch fails. Even where it did suppress it, the losers
    /// would go on to `ADD CONSTRAINT` a constraint that now exists.
    ///
    /// A transaction-scoped advisory lock keyed to the partition makes the
    /// creation atomic against other processes, and the **re-check inside it** is
    /// what makes the loser a no-op rather than a duplicate. Transaction-scoped
    /// rather than session-scoped so a creator that dies cannot wedge the write
    /// frontier for everyone else.
    ///
    /// The fast path is unchanged: an existing partition costs one catalogue
    /// lookup and never reaches the lock, which is every call after the first.
    async fn create_partition(
        &self,
        table: &str,
        id: &PartitionId,
        step: Duration,
    ) -> Result<bool> {
        let name = id.relation_name()?;
        if self.relation_exists(&name).await? {
            return Ok(false);
        }

        let mut tx = self.pool.begin().await.map_err(pg)?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(lock_key("partition", &name))
            .execute(&mut *tx)
            .await
            .map_err(pg)?;

        // Whoever held the lock before us may have created it. Without this the
        // lock would only reorder the collision, not remove it.
        if relation_exists_in(&mut tx, &name).await? {
            return Ok(false);
        }

        let end = id.start() + step;
        let ddl = format!(
            r#"CREATE TABLE "{name}" PARTITION OF "{table}"
               FOR VALUES FROM ('{}') TO ('{}')"#,
            pg_timestamp(id.start())?,
            pg_timestamp(end)?,
        );
        sqlx::query(&ddl).execute(&mut *tx).await.map_err(pg)?;

        if self.integrity_constraints {
            // In the same transaction, or the constraint would be added to a
            // partition another process cannot see yet.
            add_integrity_constraints(&mut tx, table, &name).await?;
        }
        tx.commit().await.map_err(pg)?;

        debug!(table, partition = %name, "created hot partition");
        Ok(true)
    }

    /// Whether a relation exists.
    async fn relation_exists(&self, name: &str) -> Result<bool> {
        let row = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_class c
               JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE c.relname = $1 AND n.nspname = current_schema())",
        )
        .bind(name)
        .fetch_one(&self.pool)
        .await
        .map_err(pg)?;
        Ok(row)
    }
}

/// An advisory-lock key in MeterStore's own namespace.
///
/// PostgreSQL advisory locks are keyed by a 64-bit integer in a namespace shared
/// by everything connected to the database, so the key has to be derived from
/// something specific: a bare hash of a name would collide with any other
/// application that also hashes a string. `purpose` separates MeterStore's own
/// uses from each other — an archiver's lease must not block a partition
/// creation that happens to hash the same.
///
/// FNV-1a, written out rather than taken from `DefaultHasher`: the standard
/// hasher's output is explicitly not stable across releases, and a lock key that
/// changed with the toolchain would let two processes on different builds both
/// believe they hold the same lock.
fn lock_key(purpose: &str, name: &str) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in b"meterstore."
        .iter()
        .chain(purpose.as_bytes())
        .chain(b":")
        .chain(name.as_bytes())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash as i64
}

/// Whether a relation exists, on a caller-supplied connection.
///
/// Separate from [`PostgresHot::relation_exists`] because the check that matters
/// for partition creation has to run **inside** the locking transaction.
async fn relation_exists_in(conn: &mut sqlx::PgConnection, name: &str) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_class c
           JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE c.relname = $1 AND n.nspname = current_schema())",
    )
    .bind(name)
    .fetch_one(&mut *conn)
    .await
    .map_err(pg)
}

/// A held PostgreSQL advisory lock over one table's archiver.
///
/// Owns its own connection for the whole lease, because an advisory lock is
/// *session*-scoped: taken on a pooled connection and then returned to the pool,
/// it would be released the moment another caller checked the connection out, or
/// held indefinitely by whoever got it next.
struct PgArchiveLease {
    connection: sqlx::pool::PoolConnection<sqlx::Postgres>,
    key: i64,
    table: String,
}

impl std::fmt::Debug for PgArchiveLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgArchiveLease")
            .field("table", &self.table)
            .field("key", &self.key)
            .finish()
    }
}

#[async_trait]
impl crate::tiering::store::ArchiveLease for PgArchiveLease {
    async fn release(mut self: Box<Self>) -> Result<()> {
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(self.key)
            .execute(&mut *self.connection)
            .await
            .map_err(pg)?;
        debug!(table = %self.table, "archive lease released");
        Ok(())
    }
}

/// A `version` column value as the domain type.
fn decimal_to_version(value: Decimal) -> Result<crate::version::Version> {
    // Via the decimal's own text form rather than `to_u128`: the column is
    // `NUMERIC(20,0)`, three orders of magnitude past `u64::MAX`, and a lossy
    // hop through a narrower integer is exactly the bug §20.2 already records
    // for this column once.
    let digits = value.trunc().to_string();
    crate::version::Version::new(
        digits
            .parse::<u128>()
            .map_err(|e| Error::decode(col::VERSION, format!("{digits}: {e}")))?,
    )
}

/// One text cell of a batch, by column name.
fn text_column<'a>(batch: &'a RecordBatch, name: &str, row: usize) -> Result<&'a str> {
    Ok(batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| Error::encode(name, "expected a string column"))?
        .value(row))
}

/// Render a domain code list as a SQL `IN` list.
///
/// The codes come from `metering` rather than being spelled out in the DDL, so a
/// Sparte added upstream widens the constraint on the next `CREATE TABLE` instead
/// of rejecting rows the domain considers valid. The codes are compile-time
/// constants of this crate's own dependency, so there is no injection surface —
/// but they are still quoted properly rather than pasted.
fn sql_code_list(codes: &[&str]) -> String {
    codes
        .iter()
        .map(|c| format!("'{}'", c.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The PostgreSQL type for a deployment-declared column.
///
/// Deliberately narrow: a type with no Iceberg equivalent must fail at table
/// creation, not once rows are already flowing.
fn pg_type(ty: &crate::arrow::datatypes::DataType) -> Result<&'static str> {
    use crate::arrow::datatypes::DataType;
    Ok(match ty {
        DataType::Utf8 | DataType::LargeUtf8 => "TEXT",
        DataType::Boolean => "BOOLEAN",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::Float64 => "DOUBLE PRECISION",
        DataType::Date32 => "DATE",
        DataType::Timestamp(_, _) => "TIMESTAMPTZ",
        other => {
            return Err(Error::config(format!(
                "no PostgreSQL mapping for extra column type {other:?}"
            )));
        }
    })
}

/// One component of a keyset cursor, in the type PostgreSQL binds it as.
///
/// The cursor spans three types — text for identifiers, `timestamptz` for the
/// interval start, `numeric` for the version — and a row comparison needs each
/// bound as itself. Binding everything as text would compare `10` below `9`.
#[derive(Debug, Clone)]
enum CursorValue {
    Text(String),
    Timestamp(OffsetDateTime),
    Numeric(Decimal),
}

impl CursorValue {
    /// Read the value at a projected position, choosing the type by column.
    ///
    /// Positions are fixed by [`scan_columns`] for the core columns; anything
    /// beyond them is a deployment column, which configuration restricts to text
    /// (§7.3).
    ///
    /// The type is taken from the storage schema rather than from a literal
    /// position. Hardcoded indices here were silently wrong the moment a column
    /// was inserted ahead of `from`, and the symptom was a decode error deep in
    /// `sqlx` naming a column number rather than a column.
    fn read(row: &sqlx::postgres::PgRow, index: usize) -> Result<Self> {
        use crate::arrow::datatypes::DataType;

        let core = schema::storage_schema(&[]);
        let kind = core.fields().get(index).map(|f| f.data_type().clone());

        Ok(match kind {
            Some(DataType::Timestamp(_, _)) => {
                Self::Timestamp(row.try_get::<OffsetDateTime, _>(index).map_err(pg)?)
            }
            Some(DataType::Decimal128(_, _)) => {
                Self::Numeric(row.try_get::<Decimal, _>(index).map_err(pg)?)
            }
            // Core text columns, and every deployment column: configuration
            // restricts those to `Utf8` (§7.3), so past the core they are text.
            _ => Self::Text(row.try_get::<String, _>(index).map_err(pg)?),
        })
    }
}

/// Where a column sits in the projection `scan_columns()` plus `extra` produces.
fn projection_index(column: &str, extra: &[String]) -> Result<usize> {
    if let Ok(index) = crate::encode::schema::storage_schema(&[]).index_of(column) {
        return Ok(index);
    }
    extra
        .iter()
        .position(|c| c == column)
        .map(|i| core_column_count() + i)
        .ok_or_else(|| Error::config(format!("column {column:?} is not projected by the scan")))
}

/// Core columns in the storage schema, and therefore in every projection.
///
/// Derived rather than written down. As a literal it was a second copy of the
/// schema's length, and adding a core column left it silently one short — which
/// surfaces as a `sqlx` decode error naming a column *number*, several layers
/// from the edit that caused it.
fn core_column_count() -> usize {
    schema::storage_schema(&[]).fields().len()
}

/// The column list every scan selects, in storage-schema order.
///
/// Generated from the schema for the same reason as [`core_column_count`]: a
/// hand-maintained list and a schema that disagree produce an `unnest` with the
/// wrong arity, and the message names neither the column nor the file.
///
/// Every name is quoted — `from` and `to` are reserved words, and quoting the
/// rest costs nothing.
fn scan_columns() -> String {
    schema::storage_schema(&[])
        .fields()
        .iter()
        .map(|f| format!("{:?}", f.name()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Convert Postgres rows into a single batch in the storage schema.
fn rows_to_batches(rows: Vec<sqlx::postgres::PgRow>, extra: &[String]) -> Result<Vec<RecordBatch>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let n = rows.len();
    let mut malo = Vec::with_capacity(n);
    let mut melo: Vec<Option<String>> = Vec::with_capacity(n);
    let mut obis = Vec::with_capacity(n);
    let mut sparte = Vec::with_capacity(n);
    let mut from = Vec::with_capacity(n);
    let mut to = Vec::with_capacity(n);
    let mut value = Vec::with_capacity(n);
    let mut unit = Vec::with_capacity(n);
    let mut quality = Vec::with_capacity(n);
    let mut resolution: Vec<Option<String>> = Vec::with_capacity(n);
    let mut source_kind = Vec::with_capacity(n);
    let mut source_detail: Vec<Option<String>> = Vec::with_capacity(n);
    let mut provenance: Vec<Option<String>> = Vec::with_capacity(n);
    let mut version = Vec::with_capacity(n);
    let mut version_scope = Vec::with_capacity(n);
    let mut recorded_at = Vec::with_capacity(n);
    let mut balancing_day = Vec::with_capacity(n);

    for row in &rows {
        malo.push(row.try_get::<String, _>(0).map_err(pg)?);
        melo.push(row.try_get::<Option<String>, _>(1).map_err(pg)?);
        obis.push(row.try_get::<String, _>(2).map_err(pg)?);
        sparte.push(row.try_get::<String, _>(3).map_err(pg)?);
        from.push(micros(row.try_get::<OffsetDateTime, _>(4).map_err(pg)?)?);
        to.push(micros(row.try_get::<OffsetDateTime, _>(5).map_err(pg)?)?);
        value.push(decimal_to_i128(
            row.try_get::<Decimal, _>(6).map_err(pg)?,
            VALUE_SCALE,
            col::VALUE,
        )?);
        unit.push(row.try_get::<String, _>(7).map_err(pg)?);
        quality.push(row.try_get::<String, _>(8).map_err(pg)?);
        resolution.push(row.try_get::<Option<String>, _>(9).map_err(pg)?);
        source_kind.push(row.try_get::<String, _>(10).map_err(pg)?);
        source_detail.push(row.try_get::<Option<String>, _>(11).map_err(pg)?);
        provenance.push(row.try_get::<Option<String>, _>(12).map_err(pg)?);
        version.push(decimal_to_i128(
            row.try_get::<Decimal, _>(13).map_err(pg)?,
            VERSION_SCALE,
            col::VERSION,
        )?);
        version_scope.push(row.try_get::<String, _>(14).map_err(pg)?);
        recorded_at.push(micros(row.try_get::<OffsetDateTime, _>(15).map_err(pg)?)?);
        balancing_day.push(days_since_epoch(
            row.try_get::<time::Date, _>(16).map_err(pg)?,
        ));
    }

    let tz: std::sync::Arc<str> = "UTC".into();
    let columns: Vec<ArrayRef> = vec![
        std::sync::Arc::new(StringArray::from(malo)),
        std::sync::Arc::new(StringArray::from(melo)),
        std::sync::Arc::new(StringArray::from(obis)),
        std::sync::Arc::new(StringArray::from(sparte)),
        std::sync::Arc::new(TimestampMicrosecondArray::from(from).with_timezone(tz.clone())),
        std::sync::Arc::new(TimestampMicrosecondArray::from(to).with_timezone(tz.clone())),
        std::sync::Arc::new(
            Decimal128Array::from(value).with_precision_and_scale(VALUE_PRECISION, VALUE_SCALE)?,
        ),
        std::sync::Arc::new(StringArray::from(unit)),
        std::sync::Arc::new(StringArray::from(quality)),
        std::sync::Arc::new(StringArray::from(resolution)),
        std::sync::Arc::new(StringArray::from(source_kind)),
        std::sync::Arc::new(StringArray::from(source_detail)),
        std::sync::Arc::new(StringArray::from(provenance)),
        std::sync::Arc::new(
            Decimal128Array::from(version)
                .with_precision_and_scale(VERSION_PRECISION, VERSION_SCALE)?,
        ),
        std::sync::Arc::new(StringArray::from(version_scope)),
        std::sync::Arc::new(TimestampMicrosecondArray::from(recorded_at).with_timezone(tz)),
        std::sync::Arc::new(crate::arrow::array::Date32Array::from(balancing_day)),
    ];

    let mut columns = columns;
    let mut fields = Vec::with_capacity(extra.len());
    for (i, name) in extra.iter().enumerate() {
        let values: Vec<Option<String>> = rows
            .iter()
            .map(|r| r.try_get::<Option<String>, _>(core_column_count() + i))
            .collect::<std::result::Result<_, _>>()
            .map_err(pg)?;
        columns.push(std::sync::Arc::new(StringArray::from(values)));
        fields.push(crate::arrow::datatypes::Field::new(
            name,
            crate::arrow::datatypes::DataType::Utf8,
            true,
        ));
    }

    Ok(vec![RecordBatch::try_new(
        schema::storage_schema(&fields),
        columns,
    )?])
}

/// One row of a storage-schema batch, in the types Postgres binds.
struct RowView<'a> {
    malo: &'a str,
    melo: Option<&'a str>,
    obis: &'a str,
    sparte: &'a str,
    from: OffsetDateTime,
    to: OffsetDateTime,
    value: Decimal,
    unit: &'a str,
    quality: &'a str,
    resolution: Option<&'a str>,
    source_kind: &'a str,
    source_detail: Option<&'a str>,
    provenance: Option<&'a str>,
    version: Decimal,
    version_scope: &'a str,
    recorded_at: OffsetDateTime,
    /// Derived by the encoder, never here — this only carries it across.
    balancing_day: time::Date,
}

impl<'a> RowView<'a> {
    fn new(batch: &'a RecordBatch, row: usize) -> Result<Self> {
        use crate::arrow::array::{Array, Decimal128Array, StringArray, TimestampMicrosecondArray};

        fn text<'b>(batch: &'b RecordBatch, name: &str, row: usize) -> Result<&'b str> {
            Ok(batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| Error::encode(name, "expected a string column"))?
                .value(row))
        }
        fn text_opt<'b>(batch: &'b RecordBatch, name: &str, row: usize) -> Option<&'b str> {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .filter(|a| !a.is_null(row))
                .map(|a| a.value(row))
        }
        fn ts(batch: &RecordBatch, name: &str, row: usize) -> Result<OffsetDateTime> {
            let micros = batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<TimestampMicrosecondArray>())
                .ok_or_else(|| Error::encode(name, "expected a timestamp column"))?
                .value(row);
            OffsetDateTime::from_unix_timestamp_nanos(i128::from(micros) * 1_000)
                .map_err(|e| Error::encode(name, e.to_string()))
        }
        fn date(batch: &RecordBatch, name: &str, row: usize) -> Result<time::Date> {
            let days = batch
                .column_by_name(name)
                .and_then(|c| {
                    c.as_any()
                        .downcast_ref::<crate::arrow::array::Date32Array>()
                })
                .ok_or_else(|| Error::encode(name, "expected a date column"))?
                .value(row);
            epoch_date()
                .checked_add(Duration::days(i64::from(days)))
                .ok_or_else(|| Error::encode(name, format!("{days} is out of Date range")))
        }
        fn dec(batch: &RecordBatch, name: &str, row: usize, scale: i8) -> Result<Decimal> {
            let raw = batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<Decimal128Array>())
                .ok_or_else(|| Error::encode(name, "expected a decimal column"))?
                .value(row);
            // `Decimal::new` takes an i64 and would reject the top of the
            // `version` range: MSCONS labels are ≥14 digits and the column holds
            // 20, which is three orders of magnitude past `i64::MAX`. The
            // 96-bit path covers the whole column.
            Decimal::try_from_i128_with_scale(raw, u32::try_from(scale).unwrap_or(0))
                .map_err(|e| Error::encode(name, format!("{raw}: {e}")))
        }

        Ok(Self {
            malo: text(batch, col::MALO_ID, row)?,
            melo: text_opt(batch, col::MELO_ID, row),
            obis: text(batch, col::OBIS_CODE, row)?,
            sparte: text(batch, col::SPARTE, row)?,
            from: ts(batch, col::FROM, row)?,
            to: ts(batch, col::TO, row)?,
            value: dec(batch, col::VALUE, row, VALUE_SCALE)?,
            unit: text(batch, col::UNIT, row)?,
            quality: text(batch, col::QUALITY, row)?,
            resolution: text_opt(batch, col::RESOLUTION, row),
            source_kind: text(batch, col::SOURCE_KIND, row)?,
            source_detail: text_opt(batch, col::SOURCE_DETAIL, row),
            provenance: text_opt(batch, col::PROVENANCE, row),
            version: dec(batch, col::VERSION, row, VERSION_SCALE)?,
            version_scope: text(batch, col::VERSION_SCOPE, row)?,
            recorded_at: ts(batch, col::RECORDED_AT, row)?,
            balancing_day: date(batch, col::BALANCING_DAY, row)?,
        })
    }
}

/// Map a `sqlx` failure into our error type.
fn pg(e: sqlx::Error) -> Error {
    Error::Storage(e.to_string())
}

/// Which partitions a catalog lookup should return.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Attachment {
    /// Attached and detached alike. A detached partition still holds rows, so
    /// anything deciding whether a time range is empty has to see it.
    Any,
    /// Detached only — a standalone relation still carrying the naming
    /// convention but with no parent in `pg_inherits`, which is what an
    /// interrupted archival run leaves behind.
    Detached,
}

/// Partition identifiers for one table, ascending.
///
/// The `LIKE` pattern escapes `_`, which is a wildcard in SQL and appears in
/// every relation name this crate creates. Relations that merely share a prefix
/// are skipped rather than failing the lookup, because the schema belongs to the
/// deployment and may hold anything.
async fn partitions_of(
    pool: &PgPool,
    table: &str,
    attachment: Attachment,
) -> Result<Vec<PartitionId>> {
    let sql = format!(
        r#"SELECT c.relname
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              AND n.nspname = current_schema()
              AND c.relname LIKE $1
              {}
            ORDER BY c.relname"#,
        match attachment {
            Attachment::Any => "",
            Attachment::Detached =>
                "AND NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid)",
        },
    );

    let rows = sqlx::query_scalar::<_, String>(&sql)
        .bind(format!("{}\\_%", table.replace('_', "\\_")))
        .fetch_all(pool)
        .await
        .map_err(pg)?;

    let mut found: Vec<PartitionId> = rows
        .iter()
        .filter_map(|r| PartitionId::from_relation_name(table, r).ok())
        .collect();
    // Lexical relation order is chronological for a fixed-width suffix, but the
    // ordering that matters is the one on the bound itself.
    found.sort();
    Ok(found)
}

#[async_trait]
impl HotStore for PostgresHot {
    async fn try_archive_lease(
        &self,
        table: &str,
    ) -> Result<Option<Box<dyn crate::tiering::store::ArchiveLease>>> {
        let key = lock_key("archive", table);
        let mut connection = self.pool.acquire().await.map_err(pg)?;

        // `try_` rather than the blocking form: a second scheduled archiver
        // should discover it has nothing to do, not queue behind a run that may
        // take minutes and then start its own against a watermark that moved.
        let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
            .bind(key)
            .fetch_one(&mut *connection)
            .await
            .map_err(pg)?;

        if !acquired {
            debug!(table, "archive lease held elsewhere");
            return Ok(None);
        }

        debug!(table, "archive lease acquired");
        Ok(Some(Box::new(PgArchiveLease {
            connection,
            key,
            table: table.to_string(),
        })))
    }

    async fn ensure_partitions(
        &self,
        table: &str,
        from: OffsetDateTime,
        until: OffsetDateTime,
        step: Duration,
    ) -> Result<Vec<PartitionId>> {
        if step <= Duration::ZERO {
            return Err(Error::config("partition step must be positive"));
        }

        let mut created = Vec::new();
        let mut start = crate::watermark::align_to_step(from, step);

        while start < until {
            let id = PartitionId::new(table, start);
            if self.create_partition(table, &id, step).await? {
                created.push(id);
            }
            start += step;
        }

        Ok(created)
    }

    async fn drop_table(&self, table: &str) -> Result<()> {
        // `CASCADE` because the partitions are dependent relations; without it
        // PostgreSQL refuses while any remains attached, and a detached orphan
        // from an interrupted run would survive as a table nobody owns.
        sqlx::query(&format!(r#"DROP TABLE IF EXISTS "{table}" CASCADE"#))
            .execute(&self.pool)
            .await
            .map_err(pg)?;

        // A detached partition is no longer a dependent relation, so `CASCADE`
        // above does not reach it. Left behind it would hold rows from a table
        // that no longer exists — invisible to every query and to the orphan
        // check, which needs the parent to find them.
        // Queried *after* the parent drop: `CASCADE` has already taken the
        // attached ones, so whatever still matches the naming convention with no
        // parent is exactly the detached leftovers.
        for orphan in self.orphaned_partitions(table).await? {
            let name = orphan.relation_name()?;
            sqlx::query(&format!(r#"DROP TABLE IF EXISTS "{name}""#))
                .execute(&self.pool)
                .await
                .map_err(pg)?;
        }

        info!(table, "hot table dropped");
        Ok(())
    }

    async fn partition_exists(&self, partition: &PartitionId) -> Result<bool> {
        self.relation_exists(&partition.relation_name()?).await
    }

    async fn detach_partition(&self, partition: &PartitionId) -> Result<()> {
        let name = partition.relation_name()?;
        let sql = format!(
            r#"ALTER TABLE "{}" DETACH PARTITION "{name}""#,
            partition.table()
        );
        sqlx::query(&sql).execute(&self.pool).await.map_err(pg)?;
        debug!(partition = %name, "detached");
        Ok(())
    }

    async fn create_tables(
        &self,
        table: &str,
        merge_key: &[String],
        extra: &[crate::arrow::datatypes::Field],
    ) -> Result<()> {
        self.create_table_with_key(table, merge_key, extra).await
    }

    async fn append_reporting(
        &self,
        table: &str,
        merge_key: &[String],
        batches: &[RecordBatch],
    ) -> Result<Vec<crate::session::Displacement>> {
        let mut out = Vec::new();
        for batch in batches {
            out.extend(self.insert_reporting(table, merge_key, batch).await?);
        }
        Ok(out)
    }

    async fn append(
        &self,
        table: &str,
        merge_key: &[String],
        batches: &[RecordBatch],
    ) -> Result<u64> {
        let mut written = 0u64;
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            written += self.append_batch(table, merge_key, batch).await?;
        }
        Ok(written)
    }

    async fn scan_range(
        &self,
        table: &str,
        range: TimeRange,
        spec: &ScanSpec,
    ) -> Result<BatchStream> {
        // An empty range must not become a full scan.
        if range.is_empty() {
            return Ok(Box::pin(futures::stream::empty()));
        }
        Ok(self.chunked_scan(table, range, spec))
    }

    async fn scan_detached(&self, partition: &PartitionId, spec: &ScanSpec) -> Result<BatchStream> {
        // A detached partition holds exactly one window by construction, so the
        // range is unbounded *within* that relation and the chunking is the same
        // machinery the query path uses.
        Ok(self.chunked_scan(&partition.relation_name()?, TimeRange::unbounded(), spec))
    }

    async fn distinct_malo_ids(&self, partition: &PartitionId) -> Result<Option<u64>> {
        let name = partition.relation_name()?;
        // Answerable from the `(malo_id, "from")` index as an index-only scan,
        // so it touches no heap pages and evicts nothing the operational
        // workload is using — which is the same reason §8.1 prefers a closed
        // partition for the archival read itself.
        let count = sqlx::query_scalar::<_, i64>(&format!(
            r#"SELECT count(DISTINCT malo_id) FROM "{name}""#
        ))
        .fetch_one(&self.pool)
        .await
        .map_err(pg)?;
        Ok(Some(count.max(0) as u64))
    }

    async fn drop_partition(&self, partition: &PartitionId) -> Result<()> {
        let name = partition.relation_name()?;
        sqlx::query(&format!(r#"DROP TABLE IF EXISTS "{name}""#))
            .execute(&self.pool)
            .await
            .map_err(pg)?;
        debug!(partition = %name, "dropped");
        Ok(())
    }

    async fn orphaned_partitions(&self, table: &str) -> Result<Vec<PartitionId>> {
        partitions_of(&self.pool, table, Attachment::Detached).await
    }

    async fn partition_starts(&self, table: &str) -> Result<Option<Vec<OffsetDateTime>>> {
        Ok(Some(
            partitions_of(&self.pool, table, Attachment::Any)
                .await?
                .into_iter()
                .map(|p| p.start())
                .collect(),
        ))
    }

    async fn invariant_violations(&self, table: &str, watermark: TieringWatermark) -> Result<u64> {
        let sql = format!(r#"SELECT count(*) FROM "{table}" WHERE "from" < $1"#);
        let count = sqlx::query_scalar::<_, i64>(&sql)
            .bind(watermark.get())
            .fetch_one(&self.pool)
            .await
            .map_err(pg)?;
        Ok(count as u64)
    }
}

/// The Unix epoch as a date, the origin `Date32` counts from.
fn epoch_date() -> time::Date {
    time::Date::from_ordinal_date(1970, 1).expect("epoch is a valid date")
}

/// Days between the Unix epoch and a date, the `Date32` encoding.
fn days_since_epoch(date: time::Date) -> i32 {
    (date - epoch_date()).whole_days() as i32
}

/// Microseconds since the Unix epoch.
fn micros(ts: OffsetDateTime) -> Result<i64> {
    i64::try_from(ts.unix_timestamp_nanos() / 1_000)
        .map_err(|_| Error::decode("timestamp", format!("{ts} out of microsecond range")))
}

/// Render a timestamp for inclusion in DDL.
///
/// Partition bounds cannot be parameterised, so this is the one place a value
/// reaches SQL as text. It is a `Timestamp` we produced ourselves — never user
/// input — and the format is fixed.
fn pg_timestamp(ts: OffsetDateTime) -> Result<String> {
    ts.format(&time::format_description::well_known::Rfc3339)
        .map_err(|e| Error::encode("partition bound", e.to_string()))
}

/// Convert a Postgres `NUMERIC` into the fixed-point column representation.
fn decimal_to_i128(value: Decimal, scale: i8, column: &str) -> Result<i128> {
    let target = u32::try_from(scale).map_err(|_| Error::decode(column, "negative scale"))?;
    let mut v = value.normalize();
    if v.scale() > target {
        return Err(Error::decode(
            column,
            format!("{value} has more than {scale} decimal places"),
        ));
    }
    v.rescale(target);
    v.mantissa()
        .to_i128()
        .ok_or_else(|| Error::decode(column, format!("{value} does not fit i128")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn partitions_are_created_on_the_shared_alignment() {
        // The bound a partition is created at and the bound a window starts at
        // have to come from one function, or a window lands between partitions
        // and archives as empty (§7.2). This asserts the hot tier uses the
        // shared one rather than a private copy.
        assert_eq!(
            crate::watermark::align_to_step(datetime!(2026-07-20 13:47:03 UTC), Duration::DAY),
            datetime!(2026-07-20 00:00 UTC)
        );
    }

    #[test]
    fn decimal_conversion_rejects_excess_precision() {
        assert!(decimal_to_i128("0.1".parse().unwrap(), 6, "x").is_ok());
        assert!(decimal_to_i128("0.0000001".parse().unwrap(), 6, "x").is_err());
    }

    #[test]
    fn decimal_conversion_scales_correctly() {
        assert_eq!(
            decimal_to_i128("1.5".parse().unwrap(), 6, "x").unwrap(),
            1_500_000
        );
        assert_eq!(
            decimal_to_i128("20260727000001".parse().unwrap(), 0, "v").unwrap(),
            20_260_727_000_001
        );
    }

    #[test]
    fn a_version_at_the_top_of_the_column_range_round_trips() {
        // `version` is Decimal128(20,0), and the top of that range is three
        // orders of magnitude past i64::MAX. Reading it back through an i64
        // would fail on data the encoder was happy to write.
        let max = 10i128.pow(20) - 1;
        let as_decimal = Decimal::try_from_i128_with_scale(max, 0).unwrap();
        assert_eq!(decimal_to_i128(as_decimal, 0, col::VERSION).unwrap(), max);
    }
}
