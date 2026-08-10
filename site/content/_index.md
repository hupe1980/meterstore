+++
title = "MeterStore"
description = "Hot/cold tiered storage for metering time series. PostgreSQL for the recent interval window, Apache Iceberg for the history, one SQL surface across both — with MSCONS correction versioning and reproducible settlement reruns."
template = "index.html"
sort_by = "weight"
+++

## The wall this exists for

An intelligent measuring system produces one value per measuring point, per OBIS
code, per interval. Fifteen minutes is the German settlement grain, and iMSys
already delivers finer:

| Scale | Rows / day | Rows / year |
|---|---|---|
| 10 k measuring points | ~1 M | ~350 M |
| 100 k (mid-size utility) | ~9.6 M | ~3.5 B |
| 1 M (metering operator) | ~96 M | ~35 B |

Retention is regulatory — years to decades for the settlement record. PostgreSQL
handles the first row of that table comfortably, the second with care, and the
third not at all without becoming a full-time job.

But the operational workload genuinely needs Postgres: recent data is written
continuously, corrected, and read transactionally by billing and market
communication. Meanwhile settlement, forecasting and grid analysis scan years
across hundreds of thousands of meters — an object-storage-and-columnar-format
problem, and why analytical queries in legacy EDM systems take minutes.

> The data has a natural split most systems refuse to exploit: recent intervals
> are hot and still being corrected; historical intervals are cold and settled.
> **The boundary between them is a timestamp.**

## Four decisions that carry the weight

**The watermark lives inside the Iceberg snapshot.** Archival writes the tier
boundary into the snapshot summary, in the same commit as the data it describes.
Iceberg commits are a compare-and-swap, so the rows and the boundary become
durable together or not at all. There is no external checkpoint store to fall out
of sync, and recovery is just reading the watermark back.

**Purge is `DROP TABLE`, never `DELETE`.** The hot table is time-partitioned, so
archiving a window detaches and drops exactly one partition — an O(1) catalogue
operation. Deleting a day of readings for 100 k meters row by row would leave
~9.6 M dead tuples for autovacuum to clean up, competing with the very workload
the tiering exists to protect.

**Corrections are versions, not overwrites.** The MSCONS application handbook
specifies that a correction is made by *versioning* the value. Nothing is updated
in place, so the store needs only Iceberg's `append` — no tombstones, no equality
deletes, no deletion vectors — and a past settlement stays reproducible because
the value it used is still there.

**Nothing on the archival path holds a window.** A day at 100 k measuring points
is ~9.6 M rows. The detached partition is paged by keyset, the Parquet writer
consumes the stream, and rows are counted as they pass rather than collected to
count. Peak memory is the chunk size, not the window.

## One statement, both tiers

```rust
let result = store.query(r#"
    SELECT meter_local_day("from") AS day, SUM(value) AS kwh
    FROM readings
    WHERE malo_id = '12345678901'
      AND "from" >= '2025-01-01' AND "from" < '2026-01-01'
    GROUP BY 1 ORDER BY 1
"#).await?;

// Every result carries the boundary it was computed against.
result.watermark();         // where cold ended and hot began
result.tiers_scanned();     // [Cold, Hot]
result.touched_hot_tier();  // whether the answer is only valid for now
```

The range is cut at the watermark, each half is read from the tier that holds it,
and the halves are concatenated. Not merged, not deduplicated — they are disjoint
by construction, which is the entire payoff of tiering on a timestamp.

`meter_local_day` is not a convenience. `Europe/Berlin` observes daylight saving,
so the UTC day boundary sits at 01:00 or 02:00 local: grouping on UTC days
produces wrong daily sums *every day of the year*, not only at the two
transitions. The calendar arithmetic is
[`metering`](https://crates.io/crates/metering)'s, so there is exactly one
implementation of it.

## What you get that a general lakehouse does not

**An open format for regulated data.** Standard Iceberg v2 on object storage,
readable by Spark, Trino, DuckDB, Snowflake and PyIceberg with MeterStore nowhere
in the data path. For data under a decade-long retention obligation, avoiding
format lock-in is the whole procurement argument — and it is
[tested against a real DuckDB](@/docs/interop.md), not asserted.

**Time travel as compliance.** MaBiS settlement must be reproducible. Pin an
Iceberg snapshot plus a version ceiling to reconstruct what was known at a point
in time, or pin the row-level transaction-time axis and reproduce across *both*
tiers. Incumbent EDM systems build this by hand with versioned shadow tables.

**A correct domain model, stored correctly.** Readings are `metering`'s types:
DST-correct calendars, exact decimals, quality flags, all four Sparten. The
contribution here is storing them at 35-billion-row scale without losing a
decimal, a quality flag or an interval boundary.

**No extension, no server configuration.** A library, not a PostgreSQL extension.
It needs `SELECT` plus ownership of its own tables — no `shared_preload_libraries`,
no restart, no `CREATE EXTENSION` for the core path — so it deploys on RDS, Cloud
SQL and Azure Postgres unchanged.
